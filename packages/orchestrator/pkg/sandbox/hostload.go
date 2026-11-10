package sandbox

import "github.com/shirou/gopsutil/v4/cpu"

// cpuSample is the host's cumulative CPU time in seconds at one instant.
type cpuSample struct {
	busy, steal, total float64
}

type cpuShare struct {
	busyPct, stealPct float64
}

// sampleHostCPU reads the aggregate host counters without using the shared,
// stateful cpu.Percent baseline. An unavailable sample is omitted from the span.
func sampleHostCPU() (cpuSample, bool) {
	times, err := cpu.Times(false)
	if err != nil || len(times) != 1 {
		return cpuSample{}, false
	}

	return cpuSampleFromTimes(times[0]), true
}

func cpuSampleFromTimes(t cpu.TimesStat) cpuSample {
	// Linux guest time is already included in user/nice.
	busy := t.User + t.Nice + t.System + t.Irq + t.Softirq

	return cpuSample{
		busy:  busy,
		steal: t.Steal,
		total: busy + t.Idle + t.Iowait + t.Steal,
	}
}

// since is the CPU share between two samples; invalid deltas are omitted.
func (s cpuSample) since(prev cpuSample) (cpuShare, bool) {
	if s.total <= prev.total || s.busy < prev.busy || s.steal < prev.steal {
		return cpuShare{}, false
	}
	window := s.total - prev.total

	return cpuShare{
		busyPct:  100 * (s.busy - prev.busy) / window,
		stealPct: 100 * (s.steal - prev.steal) / window,
	}, true
}
