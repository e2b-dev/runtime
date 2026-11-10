package sandbox

import (
	"testing"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCPUSampleFromTimes(t *testing.T) {
	t.Parallel()

	s := cpuSampleFromTimes(cpu.TimesStat{
		User: 100, Nice: 10, System: 50, Idle: 800, Iowait: 20,
		Irq: 5, Softirq: 5, Steal: 10, Guest: 20,
	})
	assert.Equal(t, cpuSample{busy: 170, steal: 10, total: 1000}, s)
}

func TestCPUSampleSince(t *testing.T) {
	t.Parallel()

	prev := cpuSample{busy: 100, steal: 10, total: 1000}
	share, ok := cpuSample{busy: 160, steal: 30, total: 1200}.since(prev)
	require.True(t, ok)
	assert.InDelta(t, 30, share.busyPct, 1e-9)
	assert.InDelta(t, 10, share.stealPct, 1e-9)

	_, ok = prev.since(prev)
	assert.False(t, ok, "an empty window has no share")
	_, ok = cpuSample{busy: 50, steal: 10, total: 1200}.since(prev)
	assert.False(t, ok, "counters going backwards are not a share")
}

func BenchmarkSampleHostCPU(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := sampleHostCPU(); !ok {
			b.Fatal("host CPU sample unavailable")
		}
	}
}
