package benchmark

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
)

type concurrencyScenario struct {
	name            string
	writers         int
	writesPerWriter int
	payloadBytes    int
	sameKey         bool
}

type concurrencyRunResult struct {
	writes      int64
	bytes       int64
	wall        time.Duration
	updateTotal time.Duration
	updateMax   time.Duration
}

const concurrencyPayloadMinBytes = len(`{"payload":""}`)

func concurrencyPayload(size int) []byte {
	prefix := []byte(`{"payload":"`)
	suffix := []byte(`"}`)
	payload := make([]byte, size)
	copy(payload, prefix)
	for index := len(prefix); index < size-len(suffix); index++ {
		payload[index] = 'x'
	}
	copy(payload[size-len(suffix):], suffix)
	return payload
}

func concurrencyScenarios(b *testing.B) []concurrencyScenario {
	b.Helper()

	writers := concurrencyDimension(b, "LOCKDC_BENCH_CONCURRENCY_WRITERS", 2, 64)
	writesPerWriter := concurrencyDimension(b, "LOCKDC_BENCH_CONCURRENCY_WRITES_PER_WRITER", 32, 4096)
	payloadBytes := concurrencyDimension(b, "LOCKDC_BENCH_CONCURRENCY_PAYLOAD_BYTES", 256, 1<<20)
	base := fmt.Sprintf("Writers%d/Writes%d/Payload%d", writers, writesPerWriter, payloadBytes)
	return []concurrencyScenario{
		{
			name:            "SameKey/" + base,
			writers:         writers,
			writesPerWriter: writesPerWriter,
			payloadBytes:    payloadBytes,
			sameKey:         true,
		},
		{
			name:            "IndependentKeys/" + base,
			writers:         writers,
			writesPerWriter: writesPerWriter,
			payloadBytes:    payloadBytes,
			sameKey:         false,
		},
	}
}

func concurrencyDimension(b *testing.B, name string, fallback, maximum int) int {
	b.Helper()
	value := envInt64(name, int64(fallback))
	if value > int64(maximum) {
		b.Fatalf("%s=%d exceeds bounded benchmark maximum %d", name, value, maximum)
	}
	if name == "LOCKDC_BENCH_CONCURRENCY_PAYLOAD_BYTES" && value < int64(concurrencyPayloadMinBytes) {
		b.Fatalf("%s=%d is smaller than the minimum JSON payload %d", name, value, concurrencyPayloadMinBytes)
	}
	return int(value)
}

func concurrencyKey(scenario concurrencyScenario, run, writer, operation int) string {
	if scenario.sameKey {
		return fmt.Sprintf("concurrency/shared/r%06d", run)
	}
	return fmt.Sprintf("concurrency/r%06d/w%03d/k%06d", run, writer, operation)
}

func BenchmarkConcurrencyPouch(b *testing.B) {
	for _, cryptoEnabled := range []bool{false, true} {
		cryptoEnabled := cryptoEnabled
		cryptoName := "NoCrypto"
		if cryptoEnabled {
			cryptoName = "Crypto"
		}
		b.Run(cryptoName, func(b *testing.B) {
			for _, scenario := range concurrencyScenarios(b) {
				scenario := scenario
				b.Run(scenario.name, func(b *testing.B) {
					runPouchConcurrencyC(b, int64(scenario.writers), int64(scenario.writesPerWriter), int64(scenario.payloadBytes), scenario.sameKey, cryptoEnabled)
				})
			}
		})
	}
}

func newLockdDiskConcurrencyClient(tb testing.TB, h *lockdDiskHarness) *lockdclient.Client {
	tb.Helper()

	client, err := lockdclient.New("http://"+h.addr,
		lockdclient.WithDisableMTLS(true),
		lockdclient.WithEndpointShuffle(false),
	)
	if err != nil {
		tb.Fatalf("create lockd disk concurrency client: %v", err)
	}
	tb.Cleanup(func() {
		if err := client.Close(); err != nil {
			tb.Errorf("close lockd disk concurrency client: %v", err)
		}
	})
	return client
}

func runLockdDiskConcurrencyIteration(clients []*lockdclient.Client, scenario concurrencyScenario, run int) (concurrencyRunResult, error) {
	if len(clients) != scenario.writers {
		return concurrencyRunResult{}, fmt.Errorf("got %d disk clients, need %d", len(clients), scenario.writers)
	}
	payload := concurrencyPayload(scenario.payloadBytes)
	startGate := make(chan struct{})
	results := make(chan concurrencyRunResult, scenario.writers)
	errors := make(chan error, scenario.writers)
	var workers sync.WaitGroup

	for writer := 0; writer < scenario.writers; writer++ {
		writer := writer
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-startGate
			var local concurrencyRunResult
			for operation := 0; operation < scenario.writesPerWriter; operation++ {
				key := concurrencyKey(scenario, run, writer, operation)
				owner := fmt.Sprintf("disk-concurrency-r%06d-w%03d", run, writer)
				opStart := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				session, err := clients[writer].Acquire(ctx, api.AcquireRequest{
					Namespace:  lockdDiskBenchNamespace,
					Key:        key,
					Owner:      owner,
					TTLSeconds: 60,
					BlockSecs:  30,
				})
				if err == nil {
					_, err = session.UpdateBytes(ctx, payload)
				}
				if err == nil {
					err = session.Release(ctx)
				} else if session != nil {
					_ = session.Close()
				}
				cancel()
				if err != nil {
					errors <- fmt.Errorf("writer %d operation %d key %s: %w", writer, operation, key, err)
					return
				}
				elapsed := time.Since(opStart)
				local.writes++
				local.bytes += int64(len(payload))
				local.updateTotal += elapsed
				if elapsed > local.updateMax {
					local.updateMax = elapsed
				}
			}
			results <- local
		}()
	}

	wallStart := time.Now()
	close(startGate)
	workers.Wait()
	wall := time.Since(wallStart)
	close(errors)
	for err := range errors {
		if err != nil {
			return concurrencyRunResult{}, err
		}
	}
	close(results)
	var total concurrencyRunResult
	total.wall = wall
	for result := range results {
		total.writes += result.writes
		total.bytes += result.bytes
		total.updateTotal += result.updateTotal
		if result.updateMax > total.updateMax {
			total.updateMax = result.updateMax
		}
	}
	expectedWrites := int64(scenario.writers * scenario.writesPerWriter)
	if total.writes != expectedWrites {
		return concurrencyRunResult{}, fmt.Errorf("completed %d writes, expected %d", total.writes, expectedWrites)
	}
	return total, nil
}

func verifyLockdDiskConcurrencyIteration(tb testing.TB, h *lockdDiskHarness, scenario concurrencyScenario, run int) {
	tb.Helper()

	keys := make([]string, 0, scenario.writers*scenario.writesPerWriter)
	if scenario.sameKey {
		keys = append(keys, concurrencyKey(scenario, run, 0, 0))
	} else {
		for writer := 0; writer < scenario.writers; writer++ {
			for operation := 0; operation < scenario.writesPerWriter; operation++ {
				keys = append(keys, concurrencyKey(scenario, run, writer, operation))
			}
		}
	}
	expectedVersion := int64(1)
	if scenario.sameKey {
		expectedVersion = int64(scenario.writers * scenario.writesPerWriter)
	}
	for _, key := range keys {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		response, err := h.client.Get(ctx, key, lockdclient.WithGetNamespace(lockdDiskBenchNamespace))
		if err != nil {
			cancel()
			tb.Fatalf("read back concurrent key %s: %v\n%s", key, err, h.logs.String())
		}
		version, versionErr := strconv.ParseInt(response.Version, 10, 64)
		closeErr := response.Close()
		cancel()
		if versionErr != nil {
			tb.Fatalf("parse concurrent key %s version %q: %v", key, response.Version, versionErr)
		}
		if closeErr != nil {
			tb.Fatalf("close concurrent key %s response: %v", key, closeErr)
		}
		if version != expectedVersion {
			tb.Fatalf("concurrent key %s version=%d, expected %d", key, version, expectedVersion)
		}
	}
}

func runLockdDiskConcurrency(b *testing.B, scenario concurrencyScenario, cryptoEnabled bool) {
	b.StopTimer()
	h := startLockdDiskHarnessWithCrypto(b, cryptoEnabled)
	clients := make([]*lockdclient.Client, scenario.writers)
	for index := range clients {
		clients[index] = newLockdDiskConcurrencyClient(b, h)
	}

	var total concurrencyRunResult
	b.ResetTimer()
	for run := 0; run < b.N; run++ {
		b.StartTimer()
		result, err := runLockdDiskConcurrencyIteration(clients, scenario, run)
		b.StopTimer()
		if err != nil {
			b.Fatalf("lockd disk concurrency benchmark failed: %v\n%s", err, h.logs.String())
		}
		verifyLockdDiskConcurrencyIteration(b, h, scenario, run)
		total.writes += result.writes
		total.bytes += result.bytes
		total.wall += result.wall
		total.updateTotal += result.updateTotal
		if result.updateMax > total.updateMax {
			total.updateMax = result.updateMax
		}
	}
	if b.N == 0 {
		return
	}
	b.ReportMetric(float64(total.wall.Nanoseconds())/float64(b.N), "write-wall-ns/op")
	b.ReportMetric(float64(scenario.writers), "writers/op")
	b.ReportMetric(float64(total.writes)/float64(b.N), "writes/op")
	b.ReportMetric(float64(total.bytes)/float64(b.N), "bytes/op")
	if total.writes > 0 {
		b.ReportMetric(float64(total.updateTotal.Nanoseconds())/float64(total.writes), "write-mean-ns/op")
	}
	b.ReportMetric(float64(total.updateMax.Nanoseconds()), "max-write-ns/op")
	if total.wall > 0 {
		b.ReportMetric(float64(total.writes)*1e9/float64(total.wall.Nanoseconds()), "writes/s")
	}
}

func BenchmarkConcurrencyLockdDisk(b *testing.B) {
	for _, cryptoEnabled := range []bool{false, true} {
		cryptoEnabled := cryptoEnabled
		cryptoName := "NoCrypto"
		if cryptoEnabled {
			cryptoName = "Crypto"
		}
		b.Run(cryptoName, func(b *testing.B) {
			for _, scenario := range concurrencyScenarios(b) {
				scenario := scenario
				b.Run(scenario.name, func(b *testing.B) {
					runLockdDiskConcurrency(b, scenario, cryptoEnabled)
				})
			}
		})
	}
}
