package benchmark

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
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

const lockdDiskBenchNamespace = "bench"

func lockdDiskBenchBinary() string {
	if raw := strings.TrimSpace(os.Getenv("LOCKDC_BENCH_LOCKD_BIN")); raw != "" {
		return raw
	}
	return filepath.Join("..", ".cache", "go", "bin", "lockd")
}

func lockdBenchLQL(scenario string) string {
	switch scenario {
	case "", "EqSparse":
		return "eq{field=/bucket,value=needle}"
	case "EqDense":
		return "eq{field=/group,value=even}"
	case "RangeHalf":
		return "range{field=/value,gte=0}"
	case "InRegionSingle":
		return "in{field=/region,any=us}"
	case "InTags":
		return "in{field=/tags[],any=planning|finance}"
	case "ContainsMessage":
		return "contains{field=/details/message,value=timeout}"
	case "IprefixTags":
		return "iprefix{field=/tags[],value=FIN}"
	case "IcontainsTags":
		return "icontains{field=/tags[],value=INA}"
	case "DateAfter":
		return "date{field=/created_at,after=2025-01-01T00:00:00Z}"
	case "RecursiveExists":
		return "exists{/details/**}"
	case "OrSparseOrFlag":
		return "or.eq{field=/bucket,value=needle},or.eq{field=/flag,value=true}"
	default:
		return "eq{field=/bucket,value=needle}"
	}
}

func lockdBenchDocument(i int64) []byte {
	bucket := "haystack"
	if i%64 == 0 {
		bucket = "needle"
	}
	group := "odd"
	if i%2 == 0 {
		group = "even"
	}
	region := "apac"
	if i%3 == 0 {
		region = "us"
	} else if i%3 == 1 {
		region = "eu"
	}
	tag0 := "runtime"
	if i%2 == 0 {
		tag0 = "planning"
	}
	tag1 := "ops"
	if i%4 == 0 {
		tag1 = "finance"
	}
	createdAt := "2024-01-01T00:00:00Z"
	if i%5 == 0 {
		createdAt = "2026-01-01T00:00:00Z"
	} else if i%5 == 1 {
		createdAt = "not-a-date"
	}
	message := "ordinary"
	if i%8 == 0 {
		message = "timeout"
	}
	flag := "false"
	if i%7 == 0 {
		flag = "true"
	}
	return []byte(fmt.Sprintf(
		`{"bucket":"%s","group":"%s","region":"%s","value":%d,"tags":["%s","%s"],"created_at":"%s","details":{"message":"%s benchmark document %d"},"flag":%s}`,
		bucket, group, region, i, tag0, tag1, createdAt, message, i, flag,
	))
}

type lockdDiskHarness struct {
	client *lockdclient.Client
	logs   *bytes.Buffer
}

func startLockdDiskHarness(tb testing.TB) *lockdDiskHarness {
	tb.Helper()

	bin := lockdDiskBenchBinary()
	if st, err := os.Stat(bin); err != nil || st.IsDir() {
		tb.Skipf("lockd disk benchmark requires lockd binary at %s; run through make benchmark-pouch-go* or set LOCKDC_BENCH_LOCKD_BIN", bin)
	}
	root, err := os.MkdirTemp("", "liblockdc-lockd-disk-bench-")
	if err != nil {
		tb.Fatalf("create lockd disk temp root: %v", err)
	}
	tb.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			tb.Errorf("remove lockd disk temp root %s: %v", root, err)
		}
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("allocate lockd disk listen address: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		tb.Fatalf("release lockd disk listen address: %v", err)
	}

	authRoot := filepath.Join(root, "auth")
	dataRoot := filepath.Join(root, "data")
	if err := os.MkdirAll(dataRoot, 0o755); err != nil {
		tb.Fatalf("create lockd disk data root: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	logs := &bytes.Buffer{}
	cmd := exec.CommandContext(ctx, bin,
		"--bootstrap", authRoot,
		"--store", "disk://"+dataRoot,
		"--listen", addr,
		"--disable-mtls",
		"--disable-storage-encryption",
		"--log-level", "error",
		"--default-namespace", lockdDiskBenchNamespace,
		"--qrf-disabled",
		"--disk-retention", "0",
		"--indexer-flush-docs", "64",
		"--indexer-flush-interval", "1s",
	)
	cmd.Stdout = logs
	cmd.Stderr = logs
	if err := cmd.Start(); err != nil {
		cancel()
		tb.Fatalf("start lockd disk benchmark server: %v", err)
	}
	tb.Cleanup(func() {
		cancel()
		if err := cmd.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.Success() {
				tb.Errorf("wait for lockd disk benchmark server: %v", err)
			}
		}
	})

	cli, err := lockdclient.New("http://"+addr,
		lockdclient.WithDisableMTLS(true),
		lockdclient.WithEndpointShuffle(false),
	)
	if err != nil {
		tb.Fatalf("create lockd disk benchmark client: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		readyCtx, readyCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, lastErr = cli.FlushIndex(readyCtx, lockdDiskBenchNamespace, lockdclient.WithFlushModeWait())
		readyCancel()
		if lastErr == nil {
			configCtx, configCancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err := cli.UpdateNamespaceConfig(configCtx, api.NamespaceConfigRequest{
				Namespace: lockdDiskBenchNamespace,
				Query: &api.NamespaceQueryConfig{
					PreferredEngine: "index",
					FallbackEngine:  "scan",
				},
			}, lockdclient.NamespaceConfigOptions{})
			configCancel()
			if err != nil {
				cancel()
				tb.Fatalf("configure lockd disk benchmark namespace query engines: %v\n%s", err, logs.String())
			}
			return &lockdDiskHarness{client: cli, logs: logs}
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatalf("lockd disk benchmark server did not become ready: %v\n%s", lastErr, logs.String())
	return nil
}

func seedLockdDisk(tb testing.TB, h *lockdDiskHarness, rows int64) {
	tb.Helper()

	for i := int64(0); i < rows; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		session, err := h.client.Acquire(ctx, api.AcquireRequest{
			Namespace:  lockdDiskBenchNamespace,
			Key:        fmt.Sprintf("doc/%08d", i),
			Owner:      "liblockdc-benchmark",
			TTLSeconds: 60,
			BlockSecs:  lockdclient.BlockNoWait,
		})
		cancel()
		if err != nil {
			tb.Fatalf("lockd disk acquire row %d: %v\n%s", i, err, h.logs.String())
		}
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		_, err = session.UpdateBytes(ctx, lockdBenchDocument(i))
		cancel()
		if err != nil {
			_ = session.Release(context.Background())
			tb.Fatalf("lockd disk update row %d: %v\n%s", i, err, h.logs.String())
		}
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		err = session.Release(ctx)
		cancel()
		if err != nil {
			tb.Fatalf("lockd disk release row %d: %v\n%s", i, err, h.logs.String())
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	_, err := h.client.FlushIndex(ctx, lockdDiskBenchNamespace, lockdclient.WithFlushModeWait())
	cancel()
	if err != nil {
		tb.Fatalf("lockd disk flush index: %v\n%s", err, h.logs.String())
	}
}

func runLockdDiskQuery(tb testing.TB, h *lockdDiskHarness, rows int64, engine, scenario string, documents bool) int {
	tb.Helper()

	opts := []lockdclient.QueryOption{
		lockdclient.WithQueryNamespace(lockdDiskBenchNamespace),
		lockdclient.WithQuery(lockdBenchLQL(scenario)),
		lockdclient.WithQueryEngine(engine),
		lockdclient.WithQueryLimit(int(rows)),
	}
	if documents {
		opts = append(opts, lockdclient.WithQueryReturnDocuments())
	} else {
		opts = append(opts, lockdclient.WithQueryReturnKeys())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	resp, err := h.client.Query(ctx, opts...)
	if err != nil {
		cancel()
		tb.Fatalf("lockd disk query scenario=%s engine=%s documents=%t: %v\n%s", scenario, engine, documents, err, h.logs.String())
	}
	defer cancel()
	defer resp.Close()
	if !documents {
		return len(resp.Keys())
	}
	count := 0
	if err := resp.ForEach(func(lockdclient.QueryRow) error {
		count++
		return nil
	}); err != nil {
		tb.Fatalf("lockd disk drain documents scenario=%s engine=%s: %v\n%s", scenario, engine, err, h.logs.String())
	}
	return count
}

func benchmarkLockdDisk(b *testing.B, documents bool) {
	rowsList := envRows("LOCKDC_BENCH_SCALE_ROWS", []int64{1024})
	scenarios := envList("LOCKDC_BENCH_SCALE_SCENARIOS", []string{
		"EqSparse",
		"EqDense",
		"RangeHalf",
		"InRegionSingle",
		"InTags",
		"ContainsMessage",
		"IprefixTags",
		"IcontainsTags",
		"DateAfter",
		"OrSparseOrFlag",
		"RecursiveExists",
	})
	for _, rows := range rowsList {
		rows := rows
		b.Run("Docs"+strconv.FormatInt(rows, 10), func(b *testing.B) {
			h := startLockdDiskHarness(b)
			seedLockdDisk(b, h, rows)
			for _, engine := range []string{"index", "scan"} {
				engine := engine
				b.Run(engine, func(b *testing.B) {
					for _, scenario := range scenarios {
						scenario := scenario
						b.Run(scenario, func(b *testing.B) {
							matched := runLockdDiskQuery(b, h, rows, engine, scenario, documents)
							b.StopTimer()
							b.ResetTimer()
							b.StartTimer()
							for i := 0; i < b.N; i++ {
								matched = runLockdDiskQuery(b, h, rows, engine, scenario, documents)
							}
							b.StopTimer()
							if !documents {
								b.ReportMetric(float64(matched), "rows/op")
							}
						})
					}
				})
			}
		})
	}
}

func BenchmarkFastLockdDisk(b *testing.B) {
	rows := envInt64("LOCKDC_BENCH_SEED_ROWS", 64)
	h := startLockdDiskHarness(b)
	seedLockdDisk(b, h, rows)
	for _, scenario := range []string{"EqSparse", "InTags", "OrSparseOrFlag",
		"RecursiveExists"} {
		scenario := scenario
		b.Run("Keys/Docs"+strconv.FormatInt(rows, 10)+"/index/"+scenario, func(b *testing.B) {
			matched := runLockdDiskQuery(b, h, rows, "index", scenario, false)
			b.StopTimer()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				matched = runLockdDiskQuery(b, h, rows, "index", scenario, false)
			}
			b.StopTimer()
			b.ReportMetric(float64(matched), "rows/op")
		})
	}
}

func BenchmarkMediumLockdDiskKeys(b *testing.B) {
	benchmarkLockdDisk(b, false)
}

func BenchmarkMediumLockdDiskDocuments(b *testing.B) {
	benchmarkLockdDisk(b, true)
}
