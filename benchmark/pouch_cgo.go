package benchmark

/*
#include <stdlib.h>
#include "pouchbench.h"
*/
import "C"

import "unsafe"

type pouchResult struct {
	operations uint64
	rows       uint64
	bytes      uint64
	elapsedNS  uint64
	indexSeq   uint64
	err        string
}

func runPouchIndexedLQLRows(root string, iterations int, seededRows int) (int, pouchResult) {
	cRoot := C.CString(root)
	defer C.free(unsafe.Pointer(cRoot))
	var res C.lockdc_pouch_bench_result
	rc := C.lockdc_pouch_bench_indexed_lql_rows(cRoot, C.uint64_t(iterations), C.uint64_t(seededRows), &res)
	return int(rc), convertPouchResult(res)
}

func runPouchIndexedLQLKeys(root string, iterations int, seededRows int) (int, pouchResult) {
	cRoot := C.CString(root)
	defer C.free(unsafe.Pointer(cRoot))
	var res C.lockdc_pouch_bench_result
	rc := C.lockdc_pouch_bench_indexed_lql_keys(cRoot, C.uint64_t(iterations), C.uint64_t(seededRows), &res)
	return int(rc), convertPouchResult(res)
}

func runPouchIndexedLQLScenarioRows(root string, scenario string, iterations int, seededRows int) (int, pouchResult) {
	cRoot := C.CString(root)
	defer C.free(unsafe.Pointer(cRoot))
	cScenario := C.CString(scenario)
	defer C.free(unsafe.Pointer(cScenario))
	var res C.lockdc_pouch_bench_result
	rc := C.lockdc_pouch_bench_indexed_lql_scenario_rows(cRoot, cScenario, C.uint64_t(iterations), C.uint64_t(seededRows), &res)
	return int(rc), convertPouchResult(res)
}

func runPouchIndexedLQLScenarioKeys(root string, scenario string, iterations int, seededRows int) (int, pouchResult) {
	cRoot := C.CString(root)
	defer C.free(unsafe.Pointer(cRoot))
	cScenario := C.CString(scenario)
	defer C.free(unsafe.Pointer(cScenario))
	var res C.lockdc_pouch_bench_result
	rc := C.lockdc_pouch_bench_indexed_lql_scenario_keys(cRoot, cScenario, C.uint64_t(iterations), C.uint64_t(seededRows), &res)
	return int(rc), convertPouchResult(res)
}

func runPouchStateWrite(root string, iterations int) (int, pouchResult) {
	cRoot := C.CString(root)
	defer C.free(unsafe.Pointer(cRoot))
	var res C.lockdc_pouch_bench_result
	rc := C.lockdc_pouch_bench_state_write(cRoot, C.uint64_t(iterations), &res)
	return int(rc), convertPouchResult(res)
}

func runPouchStateRead(root string, iterations int) (int, pouchResult) {
	cRoot := C.CString(root)
	defer C.free(unsafe.Pointer(cRoot))
	var res C.lockdc_pouch_bench_result
	rc := C.lockdc_pouch_bench_state_read(cRoot, C.uint64_t(iterations), &res)
	return int(rc), convertPouchResult(res)
}

func convertPouchResult(res C.lockdc_pouch_bench_result) pouchResult {
	return pouchResult{
		operations: uint64(res.operations),
		rows:       uint64(res.rows),
		bytes:      uint64(res.bytes),
		elapsedNS:  uint64(res.c_elapsed_ns),
		indexSeq:   uint64(res.index_seq),
		err:        C.GoString(&res.error[0]),
	}
}
