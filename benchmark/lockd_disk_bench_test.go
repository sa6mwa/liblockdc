package benchmark

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
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
	case "TenantEnterprise":
		return "eq{field=/tenant/tier,value=enterprise}"
	case "WorkflowEscalated":
		return "in{field=/workflow/stage,any=review|escalated}"
	case "AmountBand":
		return "range{field=/metrics/amount_usd,gte=10000,lt=90000}"
	case "RiskSignal":
		return "icontains{field=/risk/summary,value=timeout}"
	case "NarrativeSummary":
		return "icontains{field=/narrative/summary,value=remediation}"
	case "NarrativeDescription":
		return "contains{field=/narrative/description,value=audit}"
	case "FullTextAny":
		return "icontains{field=/...,value=audit}"
	default:
		return "eq{field=/bucket,value=needle}"
	}
}

func lockdBenchDocument(i int64) []byte {
	return productionDocument(i, 0, 4096)
}

type lockdDiskHarness struct {
	client             *lockdclient.Client
	logs               *bytes.Buffer
	bin                string
	root               string
	authRoot           string
	dataRoot           string
	addr               string
	cryptoEnabled      bool
	segmentTargetBytes int64
	haMode             string
	cancel             context.CancelFunc
	cmd                *exec.Cmd
}

const (
	lockdDiskFailoverHAMode = "failover"
	lockdDiskDurableHAMode  = "auto"
)

func startLockdDiskHarness(tb testing.TB) *lockdDiskHarness {
	return startLockdDiskHarnessWithOptions(tb, false, lockdDiskDefaultLogstoreSegmentSize)
}

func startLockdDiskHarnessWithCrypto(tb testing.TB, cryptoEnabled bool) *lockdDiskHarness {
	return startLockdDiskHarnessWithOptions(tb, cryptoEnabled, lockdDiskDefaultLogstoreSegmentSize)
}

func startLockdDiskHarnessWithSegmentTarget(tb testing.TB, segmentTargetBytes int64) *lockdDiskHarness {
	return startLockdDiskHarnessWithOptions(tb, false, segmentTargetBytes)
}

func startLockdDiskHarnessWithOptions(tb testing.TB, cryptoEnabled bool, segmentTargetBytes int64) *lockdDiskHarness {
	return startLockdDiskHarnessWithOptionsAndHAMode(tb, cryptoEnabled, segmentTargetBytes, lockdDiskFailoverHAMode)
}

func startLockdDiskHarnessWithOptionsAndHAMode(tb testing.TB, cryptoEnabled bool, segmentTargetBytes int64, haMode string) *lockdDiskHarness {
	tb.Helper()
	if segmentTargetBytes <= 0 {
		segmentTargetBytes = lockdDiskDefaultLogstoreSegmentSize
	}
	if haMode == "" {
		haMode = lockdDiskFailoverHAMode
	}

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

	h := &lockdDiskHarness{
		bin:                bin,
		root:               root,
		authRoot:           authRoot,
		dataRoot:           dataRoot,
		addr:               addr,
		cryptoEnabled:      cryptoEnabled,
		segmentTargetBytes: segmentTargetBytes,
		haMode:             haMode,
		logs:               &bytes.Buffer{},
	}
	h.start(tb)
	tb.Cleanup(func() {
		h.stop(tb)
	})
	return h
}

func (h *lockdDiskHarness) commandArgs() []string {
	return []string{
		"--bootstrap", h.authRoot,
		"--store", "disk://" + h.dataRoot,
		"--listen", h.addr,
		"--disable-mtls",
		"--log-level", "error",
		"--default-namespace", lockdDiskBenchNamespace,
		"--qrf-disabled",
		"--disk-retention", "0",
		"--indexer-flush-docs", "64",
		"--indexer-flush-interval", "1s",
		"--logstore-segment-size", strconv.FormatInt(h.segmentTargetBytes, 10),
		"--ha", h.haMode,
	}
}

func TestLockdDiskHarnessCommandArgsPreserveHAMode(t *testing.T) {
	h := &lockdDiskHarness{
		authRoot:           "/tmp/auth",
		dataRoot:           "/tmp/data",
		addr:               "127.0.0.1:12345",
		segmentTargetBytes: 65536,
		haMode:             lockdDiskDurableHAMode,
	}
	args := h.commandArgs()
	for index := 0; index+1 < len(args); index++ {
		if args[index] == "--ha" {
			if args[index+1] != lockdDiskDurableHAMode {
				t.Fatalf("lockd disk harness --ha=%q, want %q", args[index+1], lockdDiskDurableHAMode)
			}
			return
		}
	}
	t.Fatal("lockd disk harness command is missing --ha")
}

func (h *lockdDiskHarness) start(tb testing.TB) {
	tb.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	args := h.commandArgs()
	if !h.cryptoEnabled {
		args = append(args, "--disable-storage-encryption")
	}
	cmd := exec.CommandContext(ctx, h.bin, args...)
	cmd.Stdout = h.logs
	cmd.Stderr = h.logs
	if err := cmd.Start(); err != nil {
		cancel()
		tb.Fatalf("start lockd disk benchmark server: %v", err)
	}
	h.cancel = cancel
	h.cmd = cmd

	cli, err := lockdclient.New("http://"+h.addr,
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
				tb.Fatalf("configure lockd disk benchmark namespace query engines: %v\n%s", err, h.logs.String())
			}
			h.client = cli
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatalf("lockd disk benchmark server did not become ready: %v\n%s", lastErr, h.logs.String())
}

func (h *lockdDiskHarness) stop(tb testing.TB) {
	tb.Helper()

	if h.cancel != nil {
		h.cancel()
		h.cancel = nil
	}
	if h.cmd != nil {
		if err := h.cmd.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.Success() {
				tb.Errorf("wait for lockd disk benchmark server: %v", err)
			}
		}
		h.cmd = nil
	}
	h.client = nil
}

func (h *lockdDiskHarness) restart(tb testing.TB) {
	tb.Helper()

	h.stop(tb)
	h.start(tb)
}

func countLockdDiskLogstoreSegments(tb testing.TB, h *lockdDiskHarness) int64 {
	tb.Helper()

	var count int64
	err := filepath.WalkDir(h.dataRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Base(filepath.Dir(path)) != "segments" ||
			filepath.Base(filepath.Dir(filepath.Dir(path))) != "logstore" {
			return nil
		}
		matched, err := filepath.Match("seg-*.log", filepath.Base(path))
		if err != nil {
			return err
		}
		if matched {
			count++
		}
		return nil
	})
	if err != nil {
		tb.Fatalf("count lockd disk logstore segments: %v\n%s", err, h.logs.String())
	}
	return count
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
	if err := resp.ForEach(func(row lockdclient.QueryRow) error {
		reader, err := row.DocumentReader()
		if err != nil {
			return err
		}
		if _, err = io.Copy(io.Discard, reader); err != nil {
			closeErr := reader.Close()
			if closeErr != nil {
				return fmt.Errorf("drain query document: %w; close: %v", err, closeErr)
			}
			return err
		}
		if err = reader.Close(); err != nil {
			return err
		}
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
		"TenantEnterprise",
		"WorkflowEscalated",
		"AmountBand",
		"RiskSignal",
		"NarrativeSummary",
		"NarrativeDescription",
		"FullTextAny",
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
