//go:build linux

package sandbox

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

// effectiveHintInterval is the interval the periodic free-page-hinting loop
// waits before its next tick. It is the configured Interval unless the node's
// hugepage pool is under pressure AND an adaptation interval is configured, in
// which case the shorter of the two is used so just-freed guest pages return to
// the pool sooner (shrinking the reclaim-lag window). Pure for testing.
//
// poolUsagePct is the node hugepage pool utilization (0-100); a negative value
// means "unknown" (meminfo unreadable or no pool), which never triggers the
// adaptation — the fixed Interval is used, failing safe toward current
// behaviour.
func effectiveHintInterval(cfg featureflags.PeriodicHintingConfig, poolUsagePct int) time.Duration {
	if cfg.PressureInterval <= 0 || cfg.PressureThresholdPct <= 0 {
		return cfg.Interval
	}
	if poolUsagePct < cfg.PressureThresholdPct {
		return cfg.Interval
	}
	// Under pressure: hint on the shorter cadence. Guard against a misconfigured
	// PressureInterval that is somehow longer than Interval — never slow down
	// under pressure.
	if cfg.PressureInterval < cfg.Interval {
		return cfg.PressureInterval
	}

	return cfg.Interval
}

// hugePagePressure caches the node hugepage pool utilization so the per-sandbox
// hinting loops do not each read /proc/meminfo every tick. One read per TTL
// serves every sandbox on the node.
type hugePagePressure struct {
	mu       sync.Mutex
	at       time.Time
	pct      int
	readFile func() (total, free uint64, ok bool)
}

const hugePagePressureTTL = time.Second

var nodeHugePagePressure = &hugePagePressure{readFile: readMeminfoHugePages}

// nodeHugePagePressurePct returns the node hugepage pool utilization in percent
// (0-100), or -1 when it cannot be determined (no pool, or meminfo unreadable).
func nodeHugePagePressurePct() int { return nodeHugePagePressure.usagePct(time.Now()) }

func (h *hugePagePressure) usagePct(now time.Time) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.at.IsZero() && now.Sub(h.at) < hugePagePressureTTL {
		return h.pct
	}

	h.at = now
	total, free, ok := h.readFile()
	h.pct = hugePageUsagePct(total, free, ok)

	return h.pct
}

// hugePageUsagePct turns a (total, free) pool reading into a 0-100 utilization,
// or -1 when unknown. Pure for testing.
func hugePageUsagePct(total, free uint64, ok bool) int {
	if !ok || total == 0 || free > total {
		return -1
	}

	used := total - free

	return int(used * 100 / total)
}

// readMeminfoHugePages reads HugePages_Total and HugePages_Free from
// /proc/meminfo. ok is false when either line is missing or the file cannot be
// read, so the caller treats pool pressure as unknown.
func readMeminfoHugePages() (total, free uint64, ok bool) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()

	var haveTotal, haveFree bool
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "HugePages_Total:"):
			if v, parsed := parseMeminfoCount(line); parsed {
				total, haveTotal = v, true
			}
		case strings.HasPrefix(line, "HugePages_Free:"):
			if v, parsed := parseMeminfoCount(line); parsed {
				free, haveFree = v, true
			}
		}
		if haveTotal && haveFree {
			break
		}
	}

	return total, free, haveTotal && haveFree
}

// parseMeminfoCount pulls the integer out of a "Key: <n>" meminfo line (the
// HugePages_ counts are bare counts, no unit).
func parseMeminfoCount(line string) (uint64, bool) {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0, false
	}
	v, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, false
	}

	return v, true
}
