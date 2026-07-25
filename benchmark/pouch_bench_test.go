package benchmark

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"pkt.systems/lockd/api"
	lockdclient "pkt.systems/lockd/client"
)

const seededRows10k = 10000
const benchmarkNamespace = "bench"
const benchmarkQueryPageLimit = 1000

type queryScenario struct {
	name     string
	lockdLQL func(rows int) string
	expected func(rows int) int
}

var queryScenarios = []queryScenario{
	{
		name: "EqSparse",
		lockdLQL: func(rows int) string {
			return "eq{field=/bucket,value=needle}"
		},
		expected: func(rows int) int {
			return countMatching(rows, func(i int) bool { return i == targetRow(rows) })
		},
	},
	{
		name: "EqDense",
		lockdLQL: func(rows int) string {
			return "eq{field=/group,value=even}"
		},
		expected: func(rows int) int {
			return countMatching(rows, func(i int) bool { return i%2 == 0 })
		},
	},
	{
		name: "RangeHalf",
		lockdLQL: func(rows int) string {
			return fmt.Sprintf("range{field=/value,gte=%d}", targetRow(rows))
		},
		expected: func(rows int) int {
			return countMatching(rows, func(i int) bool { return i >= targetRow(rows) })
		},
	},
	{
		name: "InRegion",
		lockdLQL: func(rows int) string {
			return "in{field=/region,any=us|eu}"
		},
		expected: func(rows int) int {
			return countMatching(rows, func(i int) bool { return i%3 == 0 || i%3 == 1 })
		},
	},
	{
		name: "InRegionSingle",
		lockdLQL: func(rows int) string {
			return "in{field=/region,any=us}"
		},
		expected: func(rows int) int {
			return countMatching(rows, func(i int) bool { return i%3 == 0 })
		},
	},
	{
		name: "InTags",
		lockdLQL: func(rows int) string {
			return "in{field=/tags[],any=planning|finance}"
		},
		expected: func(rows int) int {
			return countMatching(rows, func(i int) bool { return i%3 == 0 || i%5 == 0 })
		},
	},
	{
		name: "ExistsFlag",
		lockdLQL: func(rows int) string {
			return "exists{/flag}"
		},
		expected: func(rows int) int {
			return countMatching(rows, func(i int) bool { return i%5 == 0 })
		},
	},
	{
		name: "PrefixOwner",
		lockdLQL: func(rows int) string {
			return "prefix{field=/owner,value=bench-owner-00}"
		},
		expected: func(rows int) int {
			return countMatching(rows, func(i int) bool { return i%10 == 0 })
		},
	},
	{
		name: "ContainsMessage",
		lockdLQL: func(rows int) string {
			return "contains{field=/details/message,value=timeout}"
		},
		expected: func(rows int) int {
			return countMatching(rows, func(i int) bool { return i%8 == 0 })
		},
	},
	{
		name: "AndEvenRange",
		lockdLQL: func(rows int) string {
			return fmt.Sprintf("and.eq{field=/group,value=even},and.range{field=/value,gte=%d}", targetRow(rows))
		},
		expected: func(rows int) int {
			return countMatching(rows, func(i int) bool { return i%2 == 0 && i >= targetRow(rows) })
		},
	},
	{
		name: "OrSparseOrFlag",
		lockdLQL: func(rows int) string {
			return "or.eq{field=/bucket,value=needle},or.exists{/flag}"
		},
		expected: func(rows int) int {
			return countMatching(rows, func(i int) bool { return i == targetRow(rows) || i%5 == 0 })
		},
	},
}

func reportCResult(b *testing.B, res pouchResult) {
	b.Helper()
	ops := res.operations
	if ops == 0 {
		ops = 1
	}
	cNsPerOp := float64(res.elapsedNS) / float64(ops)
	b.ReportMetric(cNsPerOp, "ns/op")
	b.ReportMetric(cNsPerOp, "c-ns/op")
	if res.documents != 0 {
		b.ReportMetric(float64(res.documents), "seeded-documents")
	}
	if res.indexSeq != 0 {
		b.ReportMetric(float64(res.indexSeq), "index-seq")
	}
	if res.bytes != 0 {
		b.ReportMetric(float64(res.bytes)/float64(ops), "stream-bytes/op")
	}
	if res.pages != 0 {
		b.ReportMetric(float64(res.pages)/float64(ops), "query-pages/op")
	}
	if res.firstPageCount != 0 {
		b.ReportMetric(float64(res.firstPageElapsedNS)/float64(res.firstPageCount), "page1-c-ns/op")
		b.ReportMetric(float64(res.firstPageCount)/float64(ops), "page1-count/op")
	}
	if res.nextPageCount != 0 {
		b.ReportMetric(float64(res.nextPageElapsedNS)/float64(res.nextPageCount), "pageN-c-ns/op")
		b.ReportMetric(float64(res.nextPageCount)/float64(ops), "pageN-count/op")
	}
	if res.queryCandidatePages != 0 {
		b.ReportMetric(float64(res.queryCandidates)/float64(res.queryCandidatePages), "query-candidates/page")
		b.ReportMetric(float64(res.queryCandidates)/float64(ops), "query-candidates/op")
	}
}

func seedRows() int {
	value := strings.TrimSpace(os.Getenv("LOCKDC_BENCH_SEED_ROWS"))
	if value == "" {
		return seededRows10k
	}
	rows, err := strconv.Atoi(value)
	if err != nil || rows <= 0 {
		return seededRows10k
	}
	return rows
}

func scaleRows() []int {
	return parsePositiveIntList(os.Getenv("LOCKDC_BENCH_SCALE_ROWS"), []int{64, 1024})
}

func parsePositiveIntList(value string, fallback []int) []int {
	parts := strings.Split(value, ",")
	rows := make([]int, 0, len(parts))
	for _, part := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err == nil && n > 0 {
			rows = append(rows, n)
		}
	}
	if len(rows) == 0 {
		return fallback
	}
	return rows
}

func scaleScenarios() []queryScenario {
	value := strings.TrimSpace(os.Getenv("LOCKDC_BENCH_SCALE_SCENARIOS"))
	if value == "" {
		return queryScenarios
	}
	parts := strings.Split(value, ",")
	scenarios := make([]queryScenario, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		for _, scenario := range queryScenarios {
			if scenario.name == name {
				scenarios = append(scenarios, scenario)
				break
			}
		}
	}
	if len(scenarios) == 0 {
		return queryScenarios
	}
	return scenarios
}

func targetRow(rows int) int {
	if rows > 1 {
		return rows / 2
	}
	return 0
}

func countMatching(rows int, match func(i int) bool) int {
	count := 0
	for i := 0; i < rows; i++ {
		if match(i) {
			count++
		}
	}
	return count
}

func runPouchScenarioBenchmarks(b *testing.B, rows int, keysOnly bool) {
	runPouchScenarioBenchmarksFor(b, rows, "index", queryScenarios, keysOnly)
}

func runPouchScenarioBenchmarksFor(b *testing.B, rows int, engine string, scenarios []queryScenario, keysOnly bool) {
	b.Helper()
	for _, scenario := range scenarios {
		scenario := scenario
		b.Run(scenario.name, func(b *testing.B) {
			root := b.TempDir()
			b.ResetTimer()
			var rc int
			var res pouchResult
			if keysOnly {
				rc, res = runPouchLQLScenarioKeys(root, scenario.name, engine, b.N, rows)
			} else {
				rc, res = runPouchLQLScenarioDocuments(root, scenario.name, engine, b.N, rows)
			}
			b.StopTimer()
			if rc != 0 {
				b.Fatalf("%s", res.err)
			}
			reportCResult(b, res)
			b.ReportMetric(float64(scenario.expected(rows)), "matched-documents")
		})
	}
}

func runPouchScaleBenchmarks(b *testing.B, keysOnly bool) {
	b.Helper()
	for _, rows := range scaleRows() {
		rows := rows
		b.Run(fmt.Sprintf("Docs%d", rows), func(b *testing.B) {
			for _, engine := range []string{"index", "scan"} {
				engine := engine
				b.Run(engine, func(b *testing.B) {
					runPouchScenarioBenchmarksFor(b, rows, engine, scaleScenarios(), keysOnly)
				})
			}
		})
	}
}

func BenchmarkPouchCIndexedLQLDocuments10k(b *testing.B) {
	rows := seedRows()
	runPouchScenarioBenchmarks(b, rows, false)
}

func BenchmarkPouchCFastStateWrite(b *testing.B) {
	root := b.TempDir()
	b.ResetTimer()
	rc, res := runPouchStateWrite(root, b.N)
	b.StopTimer()
	if rc != 0 {
		b.Fatalf("%s", res.err)
	}
	reportCResult(b, res)
}

func BenchmarkPouchCFastStateRead(b *testing.B) {
	root := b.TempDir()
	b.ResetTimer()
	rc, res := runPouchStateRead(root, b.N)
	b.StopTimer()
	if rc != 0 {
		b.Fatalf("%s", res.err)
	}
	reportCResult(b, res)
}

func BenchmarkLockdDiskIndexedLQLDocuments10k(b *testing.B) {
	rows := seedRows()
	runLockdScenarioBenchmarks(b, rows, false)
}

func BenchmarkLockdDiskIndexedLQLKeys10k(b *testing.B) {
	rows := seedRows()
	runLockdScenarioBenchmarks(b, rows, true)
}

func BenchmarkLockdDiskFastStateWrite(b *testing.B) {
	env := startLockdDiskBenchmarkEnv(b)
	benchmarkLockdStateWrite(b, env.client)
}

func BenchmarkLockdDiskFastStateRead(b *testing.B) {
	env := startLockdDiskBenchmarkEnv(b)
	seedLockdState(b, env.client, "bench-read-hot", []byte(`{"bucket":"read","value":1}`))
	benchmarkLockdStateRead(b, env.client)
}

type lockdDiskEnv struct {
	client *lockdclient.Client
}

func startLockdDiskBenchmarkEnv(b *testing.B) *lockdDiskEnv {
	b.Helper()
	binary := strings.TrimSpace(os.Getenv("LOCKDC_BENCH_LOCKD_BIN"))
	if binary == "" {
		binary = filepath.Join("..", ".cache", "go", "bin", "lockd")
	}
	absBinary, err := filepath.Abs(binary)
	if err != nil {
		b.Fatalf("resolve lockd binary: %v", err)
	}
	if _, err := os.Stat(absBinary); err != nil {
		b.Fatalf("lockd binary %q unavailable: run `make benchmark-pouch-go`: %v", absBinary, err)
	}

	listen := freeLoopbackAddress(b)
	root := filepath.Join(b.TempDir(), "lockd-disk")
	if err := os.MkdirAll(root, 0o755); err != nil {
		b.Fatalf("mkdir lockd disk root: %v", err)
	}
	storeURL := (&url.URL{Scheme: "disk", Path: root}).String()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, absBinary,
		"--listen", listen,
		"--store", storeURL,
		"--disable-mtls",
		"--disable-storage-encryption",
		"--metrics-listen", "",
		"--pprof-listen", "",
		"--log-level", "error",
		"--qrf-disabled",
		"--connguard-enabled=false",
	)
	var output strings.Builder
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		cancel()
		b.Fatalf("start lockd: %v", err)
	}
	b.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
	})

	baseURL := "http://" + listen
	waitForHealth(b, baseURL, &output)
	cli, err := lockdclient.New(baseURL,
		lockdclient.WithDisableMTLS(true),
		lockdclient.WithEndpointShuffle(false),
	)
	if err != nil {
		b.Fatalf("open lockd client: %v", err)
	}
	b.Cleanup(func() { cli.Close() })
	configureLockdBenchmarkNamespace(b, cli)
	return &lockdDiskEnv{client: cli}
}

func configureLockdBenchmarkNamespace(b *testing.B, cli *lockdclient.Client) {
	b.Helper()
	ctx := context.Background()
	_, err := cli.UpdateNamespaceConfig(ctx, api.NamespaceConfigRequest{
		Namespace: benchmarkNamespace,
		Query: &api.NamespaceQueryConfig{
			PreferredEngine: "index",
			FallbackEngine:  "scan",
		},
	}, lockdclient.NamespaceConfigOptions{})
	if err != nil {
		b.Fatalf("configure lockd namespace: %v", err)
	}
}

func freeLoopbackAddress(b *testing.B) string {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("allocate loopback port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func waitForHealth(b *testing.B, baseURL string, output *strings.Builder) {
	b.Helper()
	deadline := time.Now().Add(10 * time.Second)
	client := &http.Client{Timeout: 500 * time.Millisecond}
	for time.Now().Before(deadline) {
		resp, err := client.Get(baseURL + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	b.Fatalf("lockd did not become healthy at %s\n%s", baseURL, output.String())
}

func seedLockdRows(b *testing.B, cli *lockdclient.Client, rows int) {
	b.Helper()
	for i := 0; i < rows; i++ {
		key := fmt.Sprintf("bench/query/%08d", i)
		payload := benchmarkRowPayload(i, rows)
		seedLockdState(b, cli, key, payload)
	}
}

func benchmarkRowPayload(i int, rows int) []byte {
	bucket := "haystack"
	if i == targetRow(rows) {
		bucket = "needle"
	}
	group := "odd"
	if i%2 == 0 {
		group = "even"
	}
	region := []string{"us", "eu", "apac"}[i%3]
	primaryTag := "ops"
	if i%3 == 0 {
		primaryTag = "planning"
	}
	secondaryTag := "runtime"
	if i%5 == 0 {
		secondaryTag = "finance"
	}
	message := "normal"
	if i%8 == 0 {
		message = "timeout"
	}
	flag := ""
	if i%5 == 0 {
		flag = `,"flag":true`
	}
	return []byte(fmt.Sprintf(
		`{"bucket":"%s","group":"%s","region":"%s","owner":"bench-owner-%02d","value":%d,"tags":["%s","%s"],"details":{"message":"%s event %d"}%s}`,
		bucket, group, region, i%10, i, primaryTag, secondaryTag, message, i, flag))
}

func seedLockdState(b *testing.B, cli *lockdclient.Client, key string, payload []byte) {
	b.Helper()
	ctx := context.Background()
	lease, err := cli.Acquire(ctx, api.AcquireRequest{
		Namespace:  benchmarkNamespace,
		Key:        key,
		Owner:      "bench-owner",
		TTLSeconds: 3600,
		BlockSecs:  api.BlockNoWait,
	})
	if err != nil {
		b.Fatalf("acquire %s: %v", key, err)
	}
	if _, err := lease.UpdateBytes(ctx, payload); err != nil {
		_ = lease.Release(ctx)
		b.Fatalf("update %s: %v", key, err)
	}
	if err := lease.Release(ctx); err != nil {
		b.Fatalf("release %s: %v", key, err)
	}
}

func benchmarkLockdStateWrite(b *testing.B, cli *lockdclient.Client) {
	b.Helper()
	ctx := context.Background()
	b.SetBytes(int64(len([]byte(`{"bucket":"write","value":0}`))))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench/write/%08d", i)
		payload := []byte(fmt.Sprintf(`{"bucket":"write","value":%d}`, i))
		lease, err := cli.Acquire(ctx, api.AcquireRequest{
			Namespace:  benchmarkNamespace,
			Key:        key,
			Owner:      fmt.Sprintf("bench-writer-%02d", i%10),
			TTLSeconds: 3600,
			BlockSecs:  api.BlockNoWait,
		})
		if err != nil {
			b.Fatalf("acquire write %d: %v", i, err)
		}
		if _, err := lease.UpdateBytes(ctx, payload); err != nil {
			_ = lease.Release(ctx)
			b.Fatalf("update write %d: %v", i, err)
		}
		if err := lease.Release(ctx); err != nil {
			b.Fatalf("release write %d: %v", i, err)
		}
	}
	b.StopTimer()
}

func benchmarkLockdStateRead(b *testing.B, cli *lockdclient.Client) {
	b.Helper()
	ctx := context.Background()
	b.SetBytes(int64(len([]byte(`{"bucket":"read","value":1}`))))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := cli.Get(ctx, "bench-read-hot", lockdclient.WithGetNamespace(benchmarkNamespace))
		if err != nil {
			b.Fatalf("get read %d: %v", i, err)
		}
		if !resp.HasState {
			_ = resp.Close()
			b.Fatalf("get read %d returned no state", i)
		}
		if _, err := io.Copy(io.Discard, resp.Reader()); err != nil {
			_ = resp.Close()
			b.Fatalf("drain read %d: %v", i, err)
		}
		if err := resp.Close(); err != nil {
			b.Fatalf("close read %d: %v", i, err)
		}
	}
	b.StopTimer()
}

func runLockdScenarioBenchmarks(b *testing.B, rows int, keysOnly bool) {
	b.Helper()
	for _, scenario := range queryScenarios {
		scenario := scenario
		b.Run(scenario.name, func(b *testing.B) {
			env := startLockdDiskBenchmarkEnv(b)
			seedLockdRows(b, env.client, rows)
			benchmarkLockdQuery(b, env.client, rows, scenario, "index", keysOnly)
		})
	}
}

func runLockdScaleBenchmarks(b *testing.B, keysOnly bool) {
	b.Helper()
	for _, rows := range scaleRows() {
		rows := rows
		b.Run(fmt.Sprintf("Docs%d", rows), func(b *testing.B) {
			for _, engine := range []string{"index", "scan"} {
				engine := engine
				b.Run(engine, func(b *testing.B) {
					env := startLockdDiskBenchmarkEnv(b)
					seedLockdRows(b, env.client, rows)
					for _, scenario := range scaleScenarios() {
						scenario := scenario
						b.Run(scenario.name, func(b *testing.B) {
							benchmarkLockdQuery(b, env.client, rows, scenario, engine, keysOnly)
						})
					}
				})
			}
		})
	}
}

func benchmarkLockdQuery(b *testing.B, cli *lockdclient.Client, seededRows int, scenario queryScenario, engine string, keysOnly bool) {
	b.Helper()
	ctx := context.Background()
	returnMode := lockdclient.QueryReturnDocuments
	if keysOnly {
		returnMode = lockdclient.QueryReturnKeys
	}
	expectedRows := scenario.expected(seededRows)

	b.ResetTimer()
	totalPages := 0
	firstPageElapsed := time.Duration(0)
	firstPageCount := 0
	nextPageElapsed := time.Duration(0)
	nextPageCount := 0
	for i := 0; i < b.N; i++ {
		cursor := ""
		rows := 0
		pages := 0
		maxIndexSeq := uint64(0)
		for {
			opts := []lockdclient.QueryOption{
				lockdclient.WithQueryNamespace(benchmarkNamespace),
				lockdclient.WithQuery(scenario.lockdLQL(seededRows)),
				lockdclient.WithQueryLimit(benchmarkQueryPageLimit),
				lockdclient.WithQueryEngine(engine),
				lockdclient.WithQueryRefreshWaitFor(),
				lockdclient.WithQueryReturn(returnMode),
			}
			if cursor != "" {
				opts = append(opts, lockdclient.WithQueryCursor(cursor))
			}
			pageStart := time.Now()
			resp, err := cli.Query(ctx, opts...)
			if err != nil {
				b.Fatalf("query: %v", err)
			}
			if keysOnly {
				rows += len(resp.Keys())
			} else {
				err = resp.ForEach(func(row lockdclient.QueryRow) error {
					rows++
					reader, err := row.DocumentReader()
					if err != nil {
						return err
					}
					_, copyErr := io.Copy(io.Discard, reader)
					closeErr := reader.Close()
					if copyErr != nil {
						return copyErr
					}
					return closeErr
				})
				if err != nil {
					_ = resp.Close()
					b.Fatalf("drain query documents: %v", err)
				}
			}
			if resp.IndexSeq > maxIndexSeq {
				maxIndexSeq = resp.IndexSeq
			}
			cursor = resp.Cursor
			if err := resp.Close(); err != nil {
				b.Fatalf("close query response: %v", err)
			}
			pageElapsed := time.Since(pageStart)
			if pages == 0 {
				firstPageElapsed += pageElapsed
				firstPageCount++
			} else {
				nextPageElapsed += pageElapsed
				nextPageCount++
			}
			pages++
			totalPages++
			if cursor == "" {
				break
			}
			if pages > seededRows/benchmarkQueryPageLimit+2 {
				b.Fatalf("query pagination exceeded expected page count")
			}
		}
		if rows != expectedRows {
			b.Fatalf("query matched %d documents, expected %d", rows,
				expectedRows)
		}
		if engine == "index" && maxIndexSeq == 0 {
			b.Fatalf("query did not report index sequence")
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(seededRows), "seeded-documents")
	b.ReportMetric(float64(expectedRows), "matched-documents")
	if b.N > 0 {
		b.ReportMetric(float64(totalPages)/float64(b.N), "query-pages/op")
	}
	if firstPageCount != 0 {
		b.ReportMetric(float64(firstPageElapsed.Nanoseconds())/float64(firstPageCount), "page1-ns/op")
		b.ReportMetric(float64(firstPageCount)/float64(b.N), "page1-count/op")
	}
	if nextPageCount != 0 {
		b.ReportMetric(float64(nextPageElapsed.Nanoseconds())/float64(nextPageCount), "pageN-ns/op")
		b.ReportMetric(float64(nextPageCount)/float64(b.N), "pageN-count/op")
	}
}

func BenchmarkPouchCIndexedLQLKeys10k(b *testing.B) {
	rows := seedRows()
	runPouchScenarioBenchmarks(b, rows, true)
}

func BenchmarkPouchCFastIndexedLQLDocuments(b *testing.B) {
	rows := seedRows()
	runPouchScenarioBenchmarks(b, rows, false)
}

func BenchmarkPouchCFastIndexedLQLKeys(b *testing.B) {
	rows := seedRows()
	runPouchScenarioBenchmarks(b, rows, true)
}

func BenchmarkLockdDiskFastIndexedLQLDocuments(b *testing.B) {
	rows := seedRows()
	runLockdScenarioBenchmarks(b, rows, false)
}

func BenchmarkLockdDiskFastIndexedLQLKeys(b *testing.B) {
	rows := seedRows()
	runLockdScenarioBenchmarks(b, rows, true)
}

func BenchmarkPouchCMediumLQLDocuments(b *testing.B) {
	runPouchScaleBenchmarks(b, false)
}

func BenchmarkPouchCMediumLQLKeys(b *testing.B) {
	runPouchScaleBenchmarks(b, true)
}

func BenchmarkLockdDiskMediumLQLDocuments(b *testing.B) {
	runLockdScaleBenchmarks(b, false)
}

func BenchmarkLockdDiskMediumLQLKeys(b *testing.B) {
	runLockdScaleBenchmarks(b, true)
}
