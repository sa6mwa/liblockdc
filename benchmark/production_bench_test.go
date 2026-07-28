package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
)

// Mirrors pkt.systems/lockd v0.9.0 DefaultLogstoreSegmentSize without importing
// the full server package into this focused benchmark module.
const lockdDiskDefaultLogstoreSegmentSize = int64(64 << 20)

func productionRows() int64 {
	return envInt64("LOCKDC_BENCH_PRODUCTION_ROWS", 128)
}

func productionUpdatesPerKey() int64 {
	return envInt64("LOCKDC_BENCH_PRODUCTION_UPDATES", 3)
}

func productionPayloadBytes() int64 {
	return envInt64("LOCKDC_BENCH_PRODUCTION_PAYLOAD_BYTES", 256*1024)
}

func productionBenchName(rows, updates, payload int64) string {
	return "Rows" + strconv.FormatInt(rows, 10) +
		"/Updates" + strconv.FormatInt(updates, 10) +
		"/Payload" + strconv.FormatInt(payload, 10)
}

func productionDocument(row, generation, payloadBytes int64) []byte {
	target := payloadBytes
	if target <= 0 {
		target = 1024
	}
	prefix := fmt.Sprintf(
		`{"bucket":"%s","group":"%s","region":"%s","value":%d,"generation":%d,"tags":["%s","%s"],"created_at":"%s","details":{"message":"%s production benchmark document %d"},"flag":%s,"pad":"`,
		productionBucket(row),
		productionGroup(row),
		productionRegion(row),
		row,
		generation,
		productionTag0(row),
		productionTag1(row),
		productionCreatedAt(row),
		productionMessage(row),
		row,
		productionFlag(row),
	)
	padLen := int(target) - len(prefix) - 2
	if padLen < 32 {
		padLen = 32
	}
	out := make([]byte, 0, len(prefix)+padLen+2)
	out = append(out, prefix...)
	out = append(out, bytes.Repeat([]byte{byte('a' + row%26)}, padLen)...)
	out = append(out, '"', '}')
	return out
}

func productionBucket(row int64) string {
	if row%64 == 0 {
		return "needle"
	}
	return "haystack"
}

func productionGroup(row int64) string {
	if row%2 == 0 {
		return "even"
	}
	return "odd"
}

func productionRegion(row int64) string {
	switch row % 3 {
	case 0:
		return "us"
	case 1:
		return "eu"
	default:
		return "apac"
	}
}

func productionTag0(row int64) string {
	if row%2 == 0 {
		return "planning"
	}
	return "runtime"
}

func productionTag1(row int64) string {
	if row%4 == 0 {
		return "finance"
	}
	return "ops"
}

func productionCreatedAt(row int64) string {
	switch row % 5 {
	case 0:
		return "2026-01-01T00:00:00Z"
	case 1:
		return "not-a-date"
	default:
		return "2024-01-01T00:00:00Z"
	}
}

func productionMessage(row int64) string {
	if row%8 == 0 {
		return "timeout"
	}
	return "ordinary"
}

func productionFlag(row int64) string {
	if row%7 == 0 {
		return "true"
	}
	return "false"
}

type productionMetrics struct {
	rows          int64
	writes        int64
	reads         int64
	attachments   int64
	queueMessages int64
	staleFailures int64
	segments      int64
	segmentBytes  int64
	bytes         int64
}

func runLockdDiskProduction(b *testing.B, rows, updatesPerKey, payloadBytes int64) productionMetrics {
	b.Helper()

	h := startLockdDiskHarness(b)
	metrics := productionMetrics{}
	for row := int64(0); row < rows; row++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		session, err := h.client.Acquire(ctx, api.AcquireRequest{
			Namespace:  lockdDiskBenchNamespace,
			Key:        fmt.Sprintf("doc/%08d", row),
			Owner:      "liblockdc-production-benchmark",
			TTLSeconds: 120,
			BlockSecs:  lockdclient.BlockNoWait,
		})
		cancel()
		if err != nil {
			b.Fatalf("lockd disk production acquire row %d: %v\n%s", row, err, h.logs.String())
		}
		var staleETag string
		for generation := int64(0); generation < updatesPerKey; generation++ {
			body := productionDocument(row, generation, payloadBytes)
			ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
			res, err := session.UpdateBytes(ctx, body)
			cancel()
			if err != nil {
				_ = session.Release(context.Background())
				b.Fatalf("lockd disk production update row %d gen %d: %v\n%s", row, generation, err, h.logs.String())
			}
			metrics.writes++
			metrics.bytes += int64(len(body))
			if row == 0 && generation == 0 {
				staleETag = res.NewStateETag
			}
		}
		if row == 0 && staleETag != "" {
			body := productionDocument(row, updatesPerKey+1, payloadBytes)
			ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
			_, err = session.UpdateWithOptions(ctx, bytes.NewReader(body), lockdclient.UpdateOptions{IfETag: staleETag})
			cancel()
			if err == nil {
				_ = session.Release(context.Background())
				b.Fatalf("lockd disk production stale update unexpectedly succeeded")
			}
			metrics.staleFailures++
		}
		if row%16 == 0 {
			payload := bytes.Repeat([]byte{byte('A' + row%26)}, 4096)
			ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
			_, err = session.Attach(ctx, lockdclient.AttachRequest{
				Name:        fmt.Sprintf("blob-%08d.bin", row),
				Body:        bytes.NewReader(payload),
				ContentType: "application/octet-stream",
			})
			cancel()
			if err != nil {
				_ = session.Release(context.Background())
				b.Fatalf("lockd disk production attach row %d: %v\n%s", row, err, h.logs.String())
			}
			ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
			attachment, err := session.RetrieveAttachment(ctx, lockdclient.AttachmentSelector{Name: fmt.Sprintf("blob-%08d.bin", row)})
			if err == nil {
				_, err = io.Copy(io.Discard, attachment)
				closeErr := attachment.Close()
				if err == nil {
					err = closeErr
				}
			}
			cancel()
			if err != nil {
				_ = session.Release(context.Background())
				b.Fatalf("lockd disk production attachment read row %d: %v\n%s", row, err, h.logs.String())
			}
			metrics.attachments++
			metrics.reads++
		}
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		err = session.Release(ctx)
		cancel()
		if err != nil {
			b.Fatalf("lockd disk production release row %d: %v\n%s", row, err, h.logs.String())
		}
	}

	queueMessages := rows / 8
	if queueMessages <= 0 {
		queueMessages = 1
	}
	for i := int64(0); i < queueMessages; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := h.client.EnqueueBytes(ctx, "production", []byte(fmt.Sprintf(`{"message":%d,"kind":"production"}`, i)), lockdclient.EnqueueOptions{
			Namespace:   lockdDiskBenchNamespace,
			Visibility:  30 * time.Second,
			TTL:         time.Hour,
			MaxAttempts: 3,
			ContentType: "application/json",
		})
		cancel()
		if err != nil {
			b.Fatalf("lockd disk production enqueue %d: %v\n%s", i, err, h.logs.String())
		}
		metrics.queueMessages++
	}
	ackedMessages := int64(0)
	for ackedMessages < queueMessages {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		messages, err := h.client.DequeueBatch(ctx, "production", lockdclient.DequeueOptions{
			Namespace:    lockdDiskBenchNamespace,
			Owner:        "lockd-production-benchmark",
			Visibility:   30 * time.Second,
			BlockSeconds: lockdclient.BlockNoWait,
			PageSize:     16,
		})
		cancel()
		if err != nil {
			b.Fatalf("lockd disk production dequeue: %v\n%s", err, h.logs.String())
		}
		if len(messages) == 0 {
			b.Fatalf("lockd disk production dequeue returned no messages after %d/%d acked\n%s", ackedMessages, queueMessages, h.logs.String())
		}
		for _, msg := range messages {
			ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
			err = msg.Ack(ctx)
			cancel()
			if err != nil {
				b.Fatalf("lockd disk production ack: %v\n%s", err, h.logs.String())
			}
			ackedMessages++
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	_, err := h.client.FlushIndex(ctx, lockdDiskBenchNamespace, lockdclient.WithFlushModeWait())
	cancel()
	if err != nil {
		b.Fatalf("lockd disk production flush index: %v\n%s", err, h.logs.String())
	}
	matched := runLockdDiskQuery(b, h, rows, "index", "RangeHalf", false)
	metrics.rows = int64(matched)
	for row := int64(0); row < rows; {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		resp, err := h.client.Get(ctx, fmt.Sprintf("doc/%08d", row), lockdclient.WithGetNamespace(lockdDiskBenchNamespace))
		if err == nil {
			_, err = io.Copy(io.Discard, resp.Reader())
			closeErr := resp.Close()
			if err == nil {
				err = closeErr
			}
		}
		cancel()
		if err != nil {
			b.Fatalf("lockd disk production get row %d: %v\n%s", row, err, h.logs.String())
		}
		metrics.reads++
		step := rows / 8
		if step <= 0 {
			step = 1
		}
		row += step
	}
	metrics.segments = countLockdDiskLogstoreSegments(b, h)
	metrics.segmentBytes = lockdDiskDefaultLogstoreSegmentSize
	if metrics.segments <= 1 {
		b.Fatalf(
			"lockd disk production benchmark produced %d logstore segment(s), want >1 with default %d-byte segment target",
			metrics.segments,
			metrics.segmentBytes,
		)
	}
	return metrics
}

func BenchmarkProductionPouch(b *testing.B) {
	rows := productionRows()
	updates := productionUpdatesPerKey()
	payload := productionPayloadBytes()
	b.Run(productionBenchName(rows, updates, payload), func(b *testing.B) {
		runPouchProductionC(b, rows, updates, payload)
	})
}

func BenchmarkProductionLockdDisk(b *testing.B) {
	rows := productionRows()
	updates := productionUpdatesPerKey()
	payload := productionPayloadBytes()
	b.Run(productionBenchName(rows, updates, payload), func(b *testing.B) {
		var metrics productionMetrics
		for i := 0; i < b.N; i++ {
			metrics = runLockdDiskProduction(b, rows, updates, payload)
		}
		b.ReportMetric(float64(metrics.rows), "rows/op")
		b.ReportMetric(float64(metrics.writes), "writes/op")
		b.ReportMetric(float64(metrics.reads), "reads/op")
		b.ReportMetric(float64(metrics.attachments), "attachments/op")
		b.ReportMetric(float64(metrics.queueMessages), "queue-msgs/op")
		b.ReportMetric(float64(metrics.staleFailures), "stale-failures/op")
		b.ReportMetric(float64(metrics.segments), "segments/op")
		b.ReportMetric(float64(metrics.segmentBytes), "segment-target-bytes/op")
		b.ReportMetric(float64(metrics.bytes), "bytes/op")
	})
}
