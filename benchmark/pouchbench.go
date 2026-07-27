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

func runPouchC(b *testing.B, rows int64, engine, scenario string, documents bool) {
	cScenario := C.CString(scenario)
	cEngine := C.CString(engine)
	defer C.free(unsafe.Pointer(cScenario))
	defer C.free(unsafe.Pointer(cEngine))

	var totalCNS uint64
	var result C.lockdc_pouch_bench_result
	for i := 0; i < b.N; i++ {
		result = C.lockdc_pouch_bench_result{}
		rc := C.lockdc_pouch_bench_run(
			cScenario,
			C.long(rows),
			cEngine,
			C.int(boolToInt(documents)),
			&result,
		)
		if rc != 0 {
			b.Fatalf("pouch C benchmark failed: rc=%d scenario=%s rows=%d engine=%s documents=%t", int(rc), scenario, rows, engine, documents)
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

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
