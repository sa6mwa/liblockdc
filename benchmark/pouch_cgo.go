package benchmark

/*
#include <stdlib.h>
#include "pouchbench.h"
*/
import "C"

import "unsafe"

type pouchResult struct {
	operations          uint64
	documents           uint64
	bytes               uint64
	pages               uint64
	firstPageElapsedNS  uint64
	firstPageCount      uint64
	nextPageElapsedNS   uint64
	nextPageCount       uint64
	queryCandidates     uint64
	queryCandidatePages uint64
	resultCacheEntries  uint64
	resultCacheHits     uint64
	resultCacheMisses   uint64
	resultCachePuts     uint64
	elapsedNS           uint64
	indexSeq            uint64
	err                 string
}

func runPouchIndexedLQLDocuments(root string, iterations int, seededRows int) (int, pouchResult) {
	cRoot := C.CString(root)
	defer C.free(unsafe.Pointer(cRoot))
	var res C.lockdc_pouch_bench_result
	rc := C.lockdc_pouch_bench_indexed_lql_documents(cRoot, C.uint64_t(iterations), C.uint64_t(seededRows), &res)
	return int(rc), convertPouchResult(res)
}

func runPouchIndexedLQLKeys(root string, iterations int, seededRows int) (int, pouchResult) {
	cRoot := C.CString(root)
	defer C.free(unsafe.Pointer(cRoot))
	var res C.lockdc_pouch_bench_result
	rc := C.lockdc_pouch_bench_indexed_lql_keys(cRoot, C.uint64_t(iterations), C.uint64_t(seededRows), &res)
	return int(rc), convertPouchResult(res)
}

func runPouchIndexedLQLScenarioDocuments(root string, scenario string, iterations int, seededRows int) (int, pouchResult) {
	cRoot := C.CString(root)
	defer C.free(unsafe.Pointer(cRoot))
	cScenario := C.CString(scenario)
	defer C.free(unsafe.Pointer(cScenario))
	var res C.lockdc_pouch_bench_result
	rc := C.lockdc_pouch_bench_indexed_lql_scenario_documents(cRoot, cScenario, C.uint64_t(iterations), C.uint64_t(seededRows), &res)
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

func runPouchLQLScenarioDocuments(root string, scenario string, engine string, iterations int, seededRows int) (int, pouchResult) {
	cRoot := C.CString(root)
	defer C.free(unsafe.Pointer(cRoot))
	cScenario := C.CString(scenario)
	defer C.free(unsafe.Pointer(cScenario))
	cEngine := C.CString(engine)
	defer C.free(unsafe.Pointer(cEngine))
	var res C.lockdc_pouch_bench_result
	rc := C.lockdc_pouch_bench_lql_scenario_documents(cRoot, cScenario, cEngine, C.uint64_t(iterations), C.uint64_t(seededRows), &res)
	return int(rc), convertPouchResult(res)
}

func runPouchLQLScenarioKeys(root string, scenario string, engine string, iterations int, seededRows int) (int, pouchResult) {
	cRoot := C.CString(root)
	defer C.free(unsafe.Pointer(cRoot))
	cScenario := C.CString(scenario)
	defer C.free(unsafe.Pointer(cScenario))
	cEngine := C.CString(engine)
	defer C.free(unsafe.Pointer(cEngine))
	var res C.lockdc_pouch_bench_result
	rc := C.lockdc_pouch_bench_lql_scenario_keys(cRoot, cScenario, cEngine, C.uint64_t(iterations), C.uint64_t(seededRows), &res)
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
		operations:          uint64(res.operations),
		documents:           uint64(res.documents),
		bytes:               uint64(res.bytes),
		pages:               uint64(res.pages),
		firstPageElapsedNS:  uint64(res.first_page_elapsed_ns),
		firstPageCount:      uint64(res.first_page_count),
		nextPageElapsedNS:   uint64(res.next_page_elapsed_ns),
		nextPageCount:       uint64(res.next_page_count),
		queryCandidates:     uint64(res.query_candidates),
		queryCandidatePages: uint64(res.query_candidate_pages),
		resultCacheEntries:  uint64(res.result_cache_entries),
		resultCacheHits:     uint64(res.result_cache_hits),
		resultCacheMisses:   uint64(res.result_cache_misses),
		resultCachePuts:     uint64(res.result_cache_puts),
		elapsedNS:           uint64(res.c_elapsed_ns),
		indexSeq:            uint64(res.index_seq),
		err:                 C.GoString(&res.error[0]),
	}
}
