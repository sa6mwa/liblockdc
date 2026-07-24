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
const benchmarkSelectorExpr = "eq{field=/bucket,value=needle}"

func reportCResult(b *testing.B, res pouchResult) {
	b.Helper()
	ops := res.operations
	if ops == 0 {
		ops = 1
	}
	cNsPerOp := float64(res.elapsedNS) / float64(ops)
	b.ReportMetric(cNsPerOp, "ns/op")
	b.ReportMetric(cNsPerOp, "c-ns/op")
	b.ReportMetric(float64(res.rows), "seeded-rows")
	if res.indexSeq != 0 {
		b.ReportMetric(float64(res.indexSeq), "index-seq")
	}
	if res.bytes != 0 {
		b.ReportMetric(float64(res.bytes)/float64(ops), "stream-bytes/op")
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

func BenchmarkPouchCIndexedLQLRows10k(b *testing.B) {
	rows := seedRows()
	root := b.TempDir()
	b.ResetTimer()
	rc, res := runPouchIndexedLQLRows(root, b.N, rows)
	b.StopTimer()
	if rc != 0 {
		b.Fatalf("%s", res.err)
	}
	reportCResult(b, res)
}

func BenchmarkLockdDiskIndexedLQLRows10k(b *testing.B) {
	rows := seedRows()
	env := startLockdDiskBenchmarkEnv(b)
	seedLockdRows(b, env.client, rows)
	benchmarkLockdQuery(b, env.client, rows, false)
}

func BenchmarkLockdDiskIndexedLQLKeys10k(b *testing.B) {
	rows := seedRows()
	env := startLockdDiskBenchmarkEnv(b)
	seedLockdRows(b, env.client, rows)
	benchmarkLockdQuery(b, env.client, rows, true)
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
	return &lockdDiskEnv{client: cli}
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
	ctx := context.Background()
	for i := 0; i < rows; i++ {
		key := fmt.Sprintf("bench/query/%08d", i)
		lease, err := cli.Acquire(ctx, api.AcquireRequest{
			Namespace:  benchmarkNamespace,
			Key:        key,
			Owner:      fmt.Sprintf("bench-owner-%02d", i%10),
			TTLSeconds: 3600,
			BlockSecs:  api.BlockNoWait,
		})
		if err != nil {
			b.Fatalf("acquire seed row %d: %v", i, err)
		}
		payload := []byte(fmt.Sprintf(`{"bucket":"%s","value":%d}`,
			map[bool]string{true: "needle", false: "haystack"}[i == rows/2], i))
		if _, err := lease.UpdateBytes(ctx, payload); err != nil {
			_ = lease.Release(ctx)
			b.Fatalf("update seed row %d: %v", i, err)
		}
		if err := lease.Release(ctx); err != nil {
			b.Fatalf("release seed row %d: %v", i, err)
		}
	}
}

func benchmarkLockdQuery(b *testing.B, cli *lockdclient.Client, seededRows int, keysOnly bool) {
	b.Helper()
	ctx := context.Background()
	returnMode := lockdclient.QueryReturnDocuments
	if keysOnly {
		returnMode = lockdclient.QueryReturnKeys
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := cli.Query(ctx,
			lockdclient.WithQueryNamespace(benchmarkNamespace),
			lockdclient.WithQuery(benchmarkSelectorExpr),
			lockdclient.WithQueryLimit(seededRows),
			lockdclient.WithQueryEngineIndex(),
			lockdclient.WithQueryRefreshWaitFor(),
			lockdclient.WithQueryReturn(returnMode),
		)
		if err != nil {
			b.Fatalf("query: %v", err)
		}
		rows := 0
		if keysOnly {
			rows = len(resp.Keys())
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
		if rows != 1 {
			_ = resp.Close()
			b.Fatalf("query matched %d rows, expected 1", rows)
		}
		if resp.IndexSeq == 0 {
			_ = resp.Close()
			b.Fatalf("query did not report index sequence")
		}
		if err := resp.Close(); err != nil {
			b.Fatalf("close query response: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(seededRows), "seeded-rows")
}

func BenchmarkPouchCIndexedLQLKeys10k(b *testing.B) {
	rows := seedRows()
	root := b.TempDir()
	b.ResetTimer()
	rc, res := runPouchIndexedLQLKeys(root, b.N, rows)
	b.StopTimer()
	if rc != 0 {
		b.Fatalf("%s", res.err)
	}
	reportCResult(b, res)
}
