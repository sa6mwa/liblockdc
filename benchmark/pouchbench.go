package benchmark

/*
#include "pouchbench.h"
#include <stdlib.h>
*/
import "C"

import (
	"testing"
	"unsafe"
)

type pouchFixture struct {
	ptr  *C.lockdc_pouch_bench_fixture
	rows int64
}

func newPouchFixture(b *testing.B, rows int64) *pouchFixture {
	var result C.lockdc_pouch_bench_result
	var fixture *C.lockdc_pouch_bench_fixture
	rc := C.lockdc_pouch_bench_fixture_open(
		C.long(rows),
		&fixture,
		&result,
	)
	if rc != 0 {
		b.Fatalf("pouch C benchmark fixture failed: rc=%d rows=%d error=%q", int(rc), rows, C.GoString(&result.error[0]))
	}
	return &pouchFixture{ptr: fixture, rows: rows}
}

func (fixture *pouchFixture) close() {
	if fixture != nil && fixture.ptr != nil {
		C.lockdc_pouch_bench_fixture_close(fixture.ptr)
		fixture.ptr = nil
	}
}

func runPouchC(b *testing.B, rows int64, engine, scenario string, documents bool) {
	fixture := newPouchFixture(b, rows)
	defer fixture.close()
	runPouchFixtureC(b, fixture, engine, scenario, documents)
}

func runPouchFixtureC(b *testing.B, fixture *pouchFixture, engine, scenario string, documents bool) {
	cScenario := C.CString(scenario)
	cEngine := C.CString(engine)
	defer C.free(unsafe.Pointer(cScenario))
	defer C.free(unsafe.Pointer(cEngine))

	var totalCNS uint64
	var result C.lockdc_pouch_bench_result
	var warmResult C.lockdc_pouch_bench_result
	b.StopTimer()
	rc := C.lockdc_pouch_bench_fixture_query(
		fixture.ptr,
		cScenario,
		cEngine,
		C.int(boolToInt(documents)),
		&warmResult,
	)
	if rc != 0 {
		b.Fatalf("pouch C benchmark warmup failed: rc=%d scenario=%s rows=%d engine=%s documents=%t error=%q", int(rc), scenario, fixture.rows, engine, documents, C.GoString(&warmResult.error[0]))
	}
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		result = C.lockdc_pouch_bench_result{}
		rc = C.lockdc_pouch_bench_fixture_query(
			fixture.ptr,
			cScenario,
			cEngine,
			C.int(boolToInt(documents)),
			&result,
		)
		if rc != 0 {
			b.Fatalf("pouch C benchmark failed: rc=%d scenario=%s rows=%d engine=%s documents=%t error=%q", int(rc), scenario, fixture.rows, engine, documents, C.GoString(&result.error[0]))
		}
		totalCNS += uint64(result.c_ns)
	}
	if b.N > 0 {
		b.ReportMetric(float64(totalCNS)/float64(b.N), "c-ns/op")
	}
	if !documents {
		b.ReportMetric(float64(result.rows), "rows/op")
	}
}

func runPouchProductionC(b *testing.B, rows, updatesPerKey, payloadBytes int64, cryptoEnabled, compressionEnabled bool) {
	var totalCNS uint64
	var result C.lockdc_pouch_bench_result

	for i := 0; i < b.N; i++ {
		result = C.lockdc_pouch_bench_result{}
		rc := C.lockdc_pouch_bench_production_run(
			C.long(rows),
			C.long(updatesPerKey),
			C.long(payloadBytes),
			C.int(boolToInt(cryptoEnabled)),
			C.int(boolToInt(compressionEnabled)),
			&result,
		)
		if rc != 0 {
			b.Fatalf("pouch production benchmark failed: rc=%d rows=%d updates=%d payload=%d error=%q", int(rc), rows, updatesPerKey, payloadBytes, C.GoString(&result.error[0]))
		}
		totalCNS += uint64(result.c_ns)
	}
	if b.N > 0 {
		b.ReportMetric(float64(totalCNS)/float64(b.N), "c-ns/op")
	}
	b.ReportMetric(float64(result.rows), "rows/op")
	b.ReportMetric(float64(result.writes), "writes/op")
	b.ReportMetric(float64(result.reads), "reads/op")
	b.ReportMetric(float64(result.attachments), "attachments/op")
	b.ReportMetric(float64(result.queue_messages), "queue-msgs/op")
	b.ReportMetric(float64(result.stale_failures), "stale-failures/op")
	b.ReportMetric(float64(result.segments), "segments/op")
	b.ReportMetric(float64(result.bytes), "bytes/op")
	b.ReportMetric(float64(result.acquire_ns), "acquire-ns/op")
	if result.rows > 0 {
		b.ReportMetric(float64(result.acquire_ns)/float64(result.rows), "acquire-one-ns/op")
	}
	b.ReportMetric(float64(result.update_ns), "update-ns/op")
	if result.writes > 0 {
		b.ReportMetric(float64(result.update_ns)/float64(result.writes), "update-one-ns/op")
	}
	b.ReportMetric(float64(result.release_ns), "release-ns/op")
	if result.rows > 0 {
		b.ReportMetric(float64(result.release_ns)/float64(result.rows), "release-one-ns/op")
	}
	b.ReportMetric(float64(result.stale_ns), "stale-ns/op")
	b.ReportMetric(float64(result.attachment_ns), "attachment-ns/op")
	b.ReportMetric(float64(result.queue_ns), "queue-ns/op")
	if result.queue_messages > 0 {
		b.ReportMetric(float64(result.queue_ns)/float64(result.queue_messages), "queue-one-ns/op")
	}
	b.ReportMetric(float64(result.flush_ns), "flush-ns/op")
	b.ReportMetric(float64(result.flush_intermediate_ns), "flush-intermediate-ns/op")
	b.ReportMetric(float64(result.flush_final_ns), "flush-final-ns/op")
	b.ReportMetric(float64(result.flush_noop_ns), "flush-noop-ns/op")
	b.ReportMetric(float64(result.flush_reopen_ns), "flush-reopen-ns/op")
	b.ReportMetric(float64(result.reopen_ns), "reopen-ns/op")
	b.ReportMetric(float64(result.get_public_ns), "get-public-ns/op")
	b.ReportMetric(float64(result.get_lease_ns), "get-lease-ns/op")
	b.ReportMetric(float64(result.index_query_keys_ns), "index-query-keys-ns/op")
	b.ReportMetric(float64(result.index_query_docs_ns), "index-query-docs-ns/op")
	b.ReportMetric(float64(result.scan_query_keys_ns), "scan-query-keys-ns/op")
	b.ReportMetric(float64(result.scan_query_docs_ns), "scan-query-docs-ns/op")
	b.ReportMetric(float64(result.full_text_index_keys_ns), "full-text-index-keys-ns/op")
	b.ReportMetric(float64(result.full_text_scan_docs_ns), "full-text-scan-docs-ns/op")
}

func runPouchCompactionC(b *testing.B, rows, updatesPerKey, payloadBytes, segmentTargetBytes, minSegmentCount, minReclaimableBytes int64, scheduled, cryptoEnabled, compressionEnabled bool) {
	var totalCNS uint64
	var result C.lockdc_pouch_bench_result

	for i := 0; i < b.N; i++ {
		result = C.lockdc_pouch_bench_result{}
		rc := C.lockdc_pouch_bench_compaction_run(
			C.long(rows),
			C.long(updatesPerKey),
			C.long(payloadBytes),
			C.long(segmentTargetBytes),
			C.long(minSegmentCount),
			C.long(minReclaimableBytes),
			C.int(boolToInt(scheduled)),
			C.int(boolToInt(cryptoEnabled)),
			C.int(boolToInt(compressionEnabled)),
			&result,
		)
		if rc != 0 {
			b.Fatalf("pouch compaction benchmark failed: rc=%d rows=%d updates=%d payload=%d segment_target=%d min_segments=%d min_reclaimable=%d scheduled=%t crypto=%t error=%q", int(rc), rows, updatesPerKey, payloadBytes, segmentTargetBytes, minSegmentCount, minReclaimableBytes, scheduled, cryptoEnabled, C.GoString(&result.error[0]))
		}
		totalCNS += uint64(result.c_ns)
	}
	if b.N > 0 {
		b.ReportMetric(float64(totalCNS)/float64(b.N), "c-ns/op")
	}
	b.ReportMetric(float64(result.rows), "rows/op")
	b.ReportMetric(float64(result.writes), "writes/op")
	b.ReportMetric(float64(result.bytes), "bytes/op")
	b.ReportMetric(float64(result.segments), "segments/op")
	b.ReportMetric(float64(result.snapshots), "snapshots/op")
	b.ReportMetric(float64(result.compactions), "compactions/op")
	b.ReportMetric(float64(result.candidate_segments), "candidate-segments/op")
	b.ReportMetric(float64(result.candidate_bytes), "candidate-bytes/op")
	b.ReportMetric(float64(result.update_ns), "write-ns/op")
	if result.writes > 0 {
		b.ReportMetric(float64(result.update_ns)/float64(result.writes), "write-one-ns/op")
	}
	b.ReportMetric(float64(result.max_update_ns), "max-write-ns/op")
	b.ReportMetric(float64(result.compaction_ns), "compaction-ns/op")
}

func runPouchConcurrencyC(b *testing.B, writers, writesPerWriter, payloadBytes int64, sameKey, cryptoEnabled bool) {
	var totalCNS uint64
	var result C.lockdc_pouch_bench_result

	for i := 0; i < b.N; i++ {
		result = C.lockdc_pouch_bench_result{}
		rc := C.lockdc_pouch_bench_concurrency_run(
			C.long(writers),
			C.long(writesPerWriter),
			C.long(payloadBytes),
			C.int(boolToInt(sameKey)),
			C.int(boolToInt(cryptoEnabled)),
			&result,
		)
		if rc != 0 {
			b.Fatalf("pouch concurrency benchmark failed: rc=%d writers=%d writes_per_writer=%d payload=%d same_key=%t crypto=%t error=%q", int(rc), writers, writesPerWriter, payloadBytes, sameKey, cryptoEnabled, C.GoString(&result.error[0]))
		}
		totalCNS += uint64(result.c_ns)
	}
	if b.N == 0 {
		return
	}
	b.ReportMetric(float64(totalCNS)/float64(b.N), "write-wall-ns/op")
	b.ReportMetric(float64(result.rows), "writers/op")
	b.ReportMetric(float64(result.writes), "writes/op")
	b.ReportMetric(float64(result.bytes), "bytes/op")
	b.ReportMetric(float64(result.segments), "segments/op")
	if result.writes > 0 {
		b.ReportMetric(float64(result.update_ns)/float64(result.writes), "write-mean-ns/op")
	}
	b.ReportMetric(float64(result.max_update_ns), "max-write-ns/op")
	if result.c_ns > 0 {
		b.ReportMetric(float64(result.writes)*1e9/float64(result.c_ns), "writes/s")
	}
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
