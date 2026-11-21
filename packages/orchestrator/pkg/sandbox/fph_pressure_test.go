//go:build linux

package sandbox

import (
	"testing"
	"time"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

func TestEffectiveHintInterval(t *testing.T) {
	t.Parallel()

	base := 30 * time.Second
	fast := 5 * time.Second
	cfg := func(pressure time.Duration, thr int) featureflags.PeriodicHintingConfig {
		return featureflags.PeriodicHintingConfig{Interval: base, PressureInterval: pressure, PressureThresholdPct: thr}
	}

	tests := []struct {
		name    string
		cfg     featureflags.PeriodicHintingConfig
		poolPct int
		want    time.Duration
	}{
		{"adaptation disabled (no pressure interval) keeps base", cfg(0, 80), 95, base},
		{"adaptation disabled (no threshold) keeps base", cfg(fast, 0), 95, base},
		{"below threshold keeps base", cfg(fast, 80), 79, base},
		{"at threshold uses fast interval", cfg(fast, 80), 80, fast},
		{"above threshold uses fast interval", cfg(fast, 80), 99, fast},
		{"unknown pool pressure (-1) keeps base, fails safe", cfg(fast, 80), -1, base},
		{"pressure interval not shorter than base never slows down", cfg(60*time.Second, 80), 95, base},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := effectiveHintInterval(tt.cfg, tt.poolPct); got != tt.want {
				t.Errorf("effectiveHintInterval(%v, %d) = %v, want %v", tt.cfg, tt.poolPct, got, tt.want)
			}
		})
	}
}

func TestHugePageUsagePct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		total, free uint64
		ok          bool
		want        int
	}{
		{"half used", 64, 32, true, 50},
		{"full", 64, 0, true, 100},
		{"empty", 64, 64, true, 0},
		{"no pool (total 0) is unknown", 0, 0, true, -1},
		{"unreadable is unknown", 64, 32, false, -1},
		{"free>total is unknown (inconsistent)", 64, 65, true, -1},
		{"69 of 100", 100, 31, true, 69},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := hugePageUsagePct(tt.total, tt.free, tt.ok); got != tt.want {
				t.Errorf("hugePageUsagePct(%d,%d,%v) = %d, want %d", tt.total, tt.free, tt.ok, got, tt.want)
			}
		})
	}
}

// the cached reader serves a stable value within the TTL from one underlying read.
func TestHugePagePressureCachesWithinTTL(t *testing.T) {
	t.Parallel()

	reads := 0
	h := &hugePagePressure{readFile: func() (uint64, uint64, bool) {
		reads++
		return 100, uint64(100 - reads*10), true // free drops each read
	}}

	t0 := time.Now()
	first := h.usagePct(t0)
	again := h.usagePct(t0.Add(hugePagePressureTTL / 2))
	if first != again {
		t.Errorf("within TTL expected cached %d, got %d", first, again)
	}
	if reads != 1 {
		t.Errorf("expected 1 underlying read within TTL, got %d", reads)
	}

	later := h.usagePct(t0.Add(hugePagePressureTTL * 2))
	if reads != 2 {
		t.Errorf("expected a fresh read after TTL, got %d reads", reads)
	}
	if later == first {
		t.Errorf("expected a refreshed value after TTL")
	}
}
