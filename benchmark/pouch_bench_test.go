package benchmark

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

func envInt64(name string, fallback int64) int64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envList(name string, fallback []string) []string {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func envRows(name string, fallback []int64) []int64 {
	raw := envList(name, nil)
	if len(raw) == 0 {
		return fallback
	}
	out := make([]int64, 0, len(raw))
	for _, part := range raw {
		value, err := strconv.ParseInt(part, 10, 64)
		if err == nil && value > 0 {
			out = append(out, value)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func benchmarkMediumLQL(b *testing.B, documents bool) {
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
			fixture := newPouchFixture(b, rows)
			defer fixture.close()
			for _, engine := range []string{"index", "scan"} {
				engine := engine
				b.Run(engine, func(b *testing.B) {
					for _, scenario := range scenarios {
						scenario := scenario
						b.Run(scenario, func(b *testing.B) {
							runPouchFixtureC(b, fixture, engine, scenario, documents)
						})
					}
				})
			}
		})
	}
}

func BenchmarkFastPouch(b *testing.B) {
	rows := envInt64("LOCKDC_BENCH_SEED_ROWS", 64)
	for _, scenario := range []string{"EqSparse", "InTags", "OrSparseOrFlag",
		"RecursiveExists"} {
		scenario := scenario
		b.Run("Keys/Docs"+strconv.FormatInt(rows, 10)+"/index/"+scenario, func(b *testing.B) {
			runPouchC(b, rows, "index", scenario, false)
		})
	}
}

func BenchmarkMediumLQLKeys(b *testing.B) {
	benchmarkMediumLQL(b, false)
}

func BenchmarkMediumLQLDocuments(b *testing.B) {
	benchmarkMediumLQL(b, true)
}
