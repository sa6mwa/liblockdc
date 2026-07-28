package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
)

// Mirrors pkt.systems/lockd v0.9.0 DefaultLogstoreSegmentSize without importing
// the full server package into this focused benchmark module.
const lockdDiskDefaultLogstoreSegmentSize = int64(64 << 20)

type productionScenario struct {
	name          string
	rows          int64
	updatesPerKey int64
	payloadBytes  int64
}

func productionRows() int64 {
	return envInt64("LOCKDC_BENCH_PRODUCTION_ROWS", 128)
}

func productionUpdatesPerKey() int64 {
	return envInt64("LOCKDC_BENCH_PRODUCTION_UPDATES", 3)
}

func productionPayloadBytes() int64 {
	return envInt64("LOCKDC_BENCH_PRODUCTION_PAYLOAD_BYTES", 256*1024)
}

func productionScenarios() []productionScenario {
	if os.Getenv("LOCKDC_BENCH_PRODUCTION_ROWS") != "" ||
		os.Getenv("LOCKDC_BENCH_PRODUCTION_UPDATES") != "" ||
		os.Getenv("LOCKDC_BENCH_PRODUCTION_PAYLOAD_BYTES") != "" {
		return []productionScenario{{
			name:          "Env",
			rows:          productionRows(),
			updatesPerKey: productionUpdatesPerKey(),
			payloadBytes:  productionPayloadBytes(),
		}}
	}
	return []productionScenario{
		{name: "WideLarge", rows: 128, updatesPerKey: 3, payloadBytes: 256 * 1024},
		{name: "DeepNested", rows: 384, updatesPerKey: 2, payloadBytes: 192 * 1024},
		{name: "HotChurn", rows: 112, updatesPerKey: 6, payloadBytes: 128 * 1024},
	}
}

func productionBenchName(s productionScenario) string {
	return s.name + "/Rows" + strconv.FormatInt(s.rows, 10) +
		"/Updates" + strconv.FormatInt(s.updatesPerKey, 10) +
		"/Payload" + strconv.FormatInt(s.payloadBytes, 10)
}

func productionPayloadForGeneration(generation, updatesPerKey, payloadBytes int64) int64 {
	const currentPayloadBytes = int64(2 * 1024)

	if updatesPerKey <= 1 || generation < updatesPerKey-1 {
		return payloadBytes
	}
	if payloadBytes < currentPayloadBytes {
		return payloadBytes
	}
	return currentPayloadBytes
}

func productionDocument(row, generation, payloadBytes int64) []byte {
	target := payloadBytes
	if target <= 0 {
		target = 1024
	}
	prefix := fmt.Sprintf(
		`{"bucket":"%s","group":"%s","region":"%s","value":%d,"generation":%d,"tags":["%s","%s"],"created_at":"%s","tenant":{"id":"tenant-%03d","tier":"%s","region":"%s"},"workflow":{"stage":"%s","attempt":%d,"owner":{"team":"%s","user":"user-%05d"}},"metrics":{"amount_usd":%d,"latency_ms":%d,"retries":%d},"risk":{"score":%d,"summary":"%s risk signal for production timeout workflow %d"},"narrative":{"summary":"%s","description":"%s","operator_notes":"%s"},"details":{"message":"%s production benchmark document %d","attributes":{"priority":"%s","source":"%s","schema_version":3}},"line_items":[{"sku":"sku-%04d","qty":%d,"price":%d},{"sku":"sku-%04d","qty":%d,"price":%d}],"flag":%s,"storage_pressure":null`,
		productionBucket(row),
		productionGroup(row),
		productionRegion(row),
		row,
		generation,
		productionTag0(row),
		productionTag1(row),
		productionCreatedAt(row),
		row%47,
		productionTenantTier(row),
		productionRegion(row),
		productionWorkflowStage(row),
		generation+1,
		productionTeam(row),
		row%10000,
		productionAmount(row),
		25+(row%250),
		generation%5,
		productionRiskScore(row),
		productionMessage(row),
		row,
		productionNarrativeSummary(row),
		productionNarrativeDescription(row),
		productionOperatorNotes(row),
		productionMessage(row),
		row,
		productionPriority(row),
		productionSource(row),
		row%4096,
		1+(row%9),
		100+(row%500),
		(row+17)%4096,
		1+(row%4),
		50+(row%300),
		productionFlag(row),
	)
	padLen := int(target) - len(prefix) - 1
	if padLen < 32 {
		padLen = 32
	}
	out := make([]byte, 0, len(prefix)+padLen+1)
	out = append(out, prefix...)
	out = append(out, bytes.Repeat([]byte{' '}, padLen)...)
	out = append(out, '}')
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

func productionTenantTier(row int64) string {
	if row%11 == 0 {
		return "enterprise"
	}
	if row%3 == 0 {
		return "business"
	}
	return "standard"
}

func productionWorkflowStage(row int64) string {
	switch row % 6 {
	case 0:
		return "ingest"
	case 1:
		return "review"
	case 2:
		return "approve"
	case 3:
		return "escalated"
	case 4:
		return "settled"
	default:
		return "archive"
	}
}

func productionTeam(row int64) string {
	if row%5 == 0 {
		return "risk"
	}
	if row%2 == 0 {
		return "platform"
	}
	return "ops"
}

func productionAmount(row int64) int64 {
	return 1000 + ((row * 7919) % 120000)
}

func productionRiskScore(row int64) int64 {
	return (row * 37) % 100
}

func productionPriority(row int64) string {
	if row%13 == 0 {
		return "critical"
	}
	if row%4 == 0 {
		return "high"
	}
	return "normal"
}

func productionSource(row int64) string {
	if row%3 == 0 {
		return "api"
	}
	if row%3 == 1 {
		return "batch"
	}
	return "worker"
}

func productionNarrativeSummary(row int64) string {
	if row%8 == 0 {
		return "timeout remediation required for customer escalation with repeated queue delivery delays, partial worker retries, owner handoff notes, and operational impact across billing, provisioning, audit trail, and downstream reconciliation services"
	}
	return "standard production summary with customer context, processing history, operator observations, reconciliation status, retry notes, audit trail references, and downstream service health annotations"
}

func productionNarrativeDescription(row int64) string {
	return "long production description capturing the full audit trail, workflow transitions, validation notes, customer-visible symptoms, previous remediation attempts, backoffice comments, service ownership history, deployment context, business priority, compliance review markers, and expected follow-up actions for operators and automated reconciliation jobs"
}

func productionOperatorNotes(row int64) string {
	if row%13 == 0 {
		return "operator notes include critical escalation context, manual override history, cross-team review comments, incident timeline, retry budget exhaustion notes, and final remediation checklist for the current production workflow"
	}
	return "operator notes include routine triage comments, observed state transitions, queue consumer handoff details, attachment review status, replay expectations, and post-processing verification notes"
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

	acquireNS           int64
	updateNS            int64
	releaseNS           int64
	staleNS             int64
	attachmentNS        int64
	queueNS             int64
	flushNS             int64
	reopenNS            int64
	getPublicNS         int64
	getLeaseNS          int64
	indexQueryKeysNS    int64
	indexQueryDocsNS    int64
	scanQueryKeysNS     int64
	scanQueryDocsNS     int64
	fullTextIndexKeysNS int64
	fullTextScanDocsNS  int64
}

func addMetricDuration(dst *int64, start time.Time) {
	*dst += time.Since(start).Nanoseconds()
}

func runLockdDiskProduction(b *testing.B, rows, updatesPerKey, payloadBytes int64) productionMetrics {
	b.Helper()

	h := startLockdDiskHarness(b)
	metrics := productionMetrics{}
	for row := int64(0); row < rows; row++ {
		phaseStart := time.Now()
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
		addMetricDuration(&metrics.acquireNS, phaseStart)
		var staleETag string
		for generation := int64(0); generation < updatesPerKey; generation++ {
			body := productionDocument(row, generation, productionPayloadForGeneration(generation, updatesPerKey, payloadBytes))
			phaseStart = time.Now()
			ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
			res, err := session.UpdateBytes(ctx, body)
			cancel()
			if err != nil {
				_ = session.Release(context.Background())
				b.Fatalf("lockd disk production update row %d gen %d: %v\n%s", row, generation, err, h.logs.String())
			}
			addMetricDuration(&metrics.updateNS, phaseStart)
			metrics.writes++
			metrics.bytes += int64(len(body))
			if row == 0 && generation == 0 {
				staleETag = res.NewStateETag
			}
		}
		if row == 0 && staleETag != "" {
			body := productionDocument(row, updatesPerKey+1, productionPayloadForGeneration(updatesPerKey+1, updatesPerKey, payloadBytes))
			phaseStart = time.Now()
			ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
			_, err = session.UpdateWithOptions(ctx, bytes.NewReader(body), lockdclient.UpdateOptions{IfETag: staleETag})
			cancel()
			addMetricDuration(&metrics.staleNS, phaseStart)
			if err == nil {
				_ = session.Release(context.Background())
				b.Fatalf("lockd disk production stale update unexpectedly succeeded")
			}
			metrics.staleFailures++
		}
		if row%16 == 0 {
			payload := bytes.Repeat([]byte{byte('A' + row%26)}, 4096)
			phaseStart = time.Now()
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
			addMetricDuration(&metrics.attachmentNS, phaseStart)
			metrics.attachments++
			metrics.reads++
		}
		if row%32 == 0 {
			phaseStart = time.Now()
			ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
			snapshot, err := session.Get(ctx)
			if err == nil && snapshot != nil {
				_, err = snapshot.Bytes()
				closeErr := snapshot.Close()
				if err == nil {
					err = closeErr
				}
			}
			cancel()
			if err != nil {
				_ = session.Release(context.Background())
				b.Fatalf("lockd disk production lease get row %d: %v\n%s", row, err, h.logs.String())
			}
			addMetricDuration(&metrics.getLeaseNS, phaseStart)
			metrics.reads++
		}
		phaseStart = time.Now()
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		err = session.Release(ctx)
		cancel()
		if err != nil {
			b.Fatalf("lockd disk production release row %d: %v\n%s", row, err, h.logs.String())
		}
		addMetricDuration(&metrics.releaseNS, phaseStart)
	}

	queueMessages := rows / 8
	if queueMessages <= 0 {
		queueMessages = 1
	}
	phaseStart := time.Now()
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
	addMetricDuration(&metrics.queueNS, phaseStart)
	phaseStart = time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	_, err := h.client.FlushIndex(ctx, lockdDiskBenchNamespace, lockdclient.WithFlushModeWait())
	cancel()
	if err != nil {
		b.Fatalf("lockd disk production flush index: %v\n%s", err, h.logs.String())
	}
	addMetricDuration(&metrics.flushNS, phaseStart)
	phaseStart = time.Now()
	h.restart(b)
	addMetricDuration(&metrics.reopenNS, phaseStart)
	phaseStart = time.Now()
	ctx, cancel = context.WithTimeout(context.Background(), 60*time.Second)
	_, err = h.client.FlushIndex(ctx, lockdDiskBenchNamespace, lockdclient.WithFlushModeWait())
	cancel()
	if err != nil {
		b.Fatalf("lockd disk production post-restart flush index: %v\n%s", err, h.logs.String())
	}
	addMetricDuration(&metrics.flushNS, phaseStart)

	phaseStart = time.Now()
	matched := runLockdDiskQuery(b, h, rows, "index", "RangeHalf", false)
	addMetricDuration(&metrics.indexQueryKeysNS, phaseStart)
	if matched <= 0 {
		b.Fatalf("lockd disk production RangeHalf index query matched %d rows, want >0", matched)
	}
	metrics.rows = int64(matched)
	phaseStart = time.Now()
	matched = runLockdDiskQuery(b, h, rows, "index", "NarrativeSummary", true)
	addMetricDuration(&metrics.indexQueryDocsNS, phaseStart)
	if matched <= 0 {
		b.Fatalf("lockd disk production NarrativeSummary index query matched %d rows, want >0", matched)
	}
	phaseStart = time.Now()
	matched = runLockdDiskQuery(b, h, rows, "scan", "WorkflowEscalated", false)
	addMetricDuration(&metrics.scanQueryKeysNS, phaseStart)
	if matched <= 0 {
		b.Fatalf("lockd disk production WorkflowEscalated scan query matched %d rows, want >0", matched)
	}
	phaseStart = time.Now()
	matched = runLockdDiskQuery(b, h, rows, "scan", "NarrativeDescription", true)
	addMetricDuration(&metrics.scanQueryDocsNS, phaseStart)
	if matched <= 0 {
		b.Fatalf("lockd disk production NarrativeDescription scan query matched %d rows, want >0", matched)
	}
	phaseStart = time.Now()
	matched = runLockdDiskQuery(b, h, rows, "index", "FullTextAny", false)
	addMetricDuration(&metrics.fullTextIndexKeysNS, phaseStart)
	if matched <= 0 {
		b.Fatalf("lockd disk production FullTextAny index query matched %d rows, want >0", matched)
	}
	phaseStart = time.Now()
	matched = runLockdDiskQuery(b, h, rows, "scan", "FullTextAny", true)
	addMetricDuration(&metrics.fullTextScanDocsNS, phaseStart)
	if matched <= 0 {
		b.Fatalf("lockd disk production FullTextAny scan query matched %d rows, want >0", matched)
	}

	for row := int64(0); row < rows; {
		phaseStart = time.Now()
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
		addMetricDuration(&metrics.getPublicNS, phaseStart)
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

func BenchmarkProductionPouchPT(b *testing.B) {
	for _, scenario := range productionScenarios() {
		scenario := scenario
		b.Run(productionBenchName(scenario), func(b *testing.B) {
			runPouchProductionC(b, scenario.rows, scenario.updatesPerKey, scenario.payloadBytes, false)
		})
	}
}

func BenchmarkProductionPouchCrypto(b *testing.B) {
	for _, scenario := range productionScenarios() {
		scenario := scenario
		b.Run(productionBenchName(scenario), func(b *testing.B) {
			runPouchProductionC(b, scenario.rows, scenario.updatesPerKey, scenario.payloadBytes, true)
		})
	}
}

func BenchmarkProductionLockdDiskNoCrypto(b *testing.B) {
	for _, scenario := range productionScenarios() {
		scenario := scenario
		b.Run(productionBenchName(scenario), func(b *testing.B) {
			var metrics productionMetrics
			for i := 0; i < b.N; i++ {
				metrics = runLockdDiskProduction(b, scenario.rows, scenario.updatesPerKey, scenario.payloadBytes)
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

			b.ReportMetric(float64(metrics.acquireNS), "acquire-ns/op")
			if metrics.rows > 0 {
				b.ReportMetric(float64(metrics.acquireNS)/float64(metrics.rows), "acquire-one-ns/op")
			}
			b.ReportMetric(float64(metrics.updateNS), "update-ns/op")
			if metrics.writes > 0 {
				b.ReportMetric(float64(metrics.updateNS)/float64(metrics.writes), "update-one-ns/op")
			}
			b.ReportMetric(float64(metrics.releaseNS), "release-ns/op")
			if metrics.rows > 0 {
				b.ReportMetric(float64(metrics.releaseNS)/float64(metrics.rows), "release-one-ns/op")
			}
			b.ReportMetric(float64(metrics.staleNS), "stale-ns/op")
			b.ReportMetric(float64(metrics.attachmentNS), "attachment-ns/op")
			b.ReportMetric(float64(metrics.queueNS), "queue-ns/op")
			if metrics.queueMessages > 0 {
				b.ReportMetric(float64(metrics.queueNS)/float64(metrics.queueMessages), "queue-one-ns/op")
			}
			b.ReportMetric(float64(metrics.flushNS), "flush-ns/op")
			b.ReportMetric(float64(metrics.reopenNS), "reopen-ns/op")
			b.ReportMetric(float64(metrics.getPublicNS), "get-public-ns/op")
			b.ReportMetric(float64(metrics.getLeaseNS), "get-lease-ns/op")
			b.ReportMetric(float64(metrics.indexQueryKeysNS), "index-query-keys-ns/op")
			b.ReportMetric(float64(metrics.indexQueryDocsNS), "index-query-docs-ns/op")
			b.ReportMetric(float64(metrics.scanQueryKeysNS), "scan-query-keys-ns/op")
			b.ReportMetric(float64(metrics.scanQueryDocsNS), "scan-query-docs-ns/op")
			b.ReportMetric(float64(metrics.fullTextIndexKeysNS), "full-text-index-keys-ns/op")
			b.ReportMetric(float64(metrics.fullTextScanDocsNS), "full-text-scan-docs-ns/op")
		})
	}
}
