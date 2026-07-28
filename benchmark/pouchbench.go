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

func runPouchProductionC(b *testing.B, rows, updatesPerKey, payloadBytes int64) {
	var totalCNS uint64
	var result C.lockdc_pouch_bench_result

	for i := 0; i < b.N; i++ {
		result = C.lockdc_pouch_bench_result{}
		rc := C.lockdc_pouch_bench_production_run(
			C.long(rows),
			C.long(updatesPerKey),
			C.long(payloadBytes),
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
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
