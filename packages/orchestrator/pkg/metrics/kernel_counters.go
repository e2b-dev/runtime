//go:build linux

package metrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

type kernelSample struct {
	counter telemetry.ObservableCounterType
	attrs   attribute.Set
	value   int64
}

// kernelFile is one /proc file; it is read and parsed once per collection and
// its samples fan out to the counters they name.
type kernelFile struct {
	path  string
	parse func([]byte) ([]kernelSample, error)
}

func kernelFiles() []kernelFile {
	files := make([]kernelFile, 0, len(pressureResources)+2)
	for _, resource := range pressureResources {
		files = append(files, kernelFile{path: "/proc/pressure/" + resource, parse: pressureParser(resource)})
	}

	return append(files,
		kernelFile{path: "/proc/vmstat", parse: parseVMStat},
		kernelFile{path: "/proc/interrupts", parse: interruptsParser(runtime.GOARCH)},
	)
}

var kernelCounterNames = []telemetry.ObservableCounterType{
	telemetry.OrchestratorHostPressureStall,
	telemetry.OrchestratorHostReclaimStalls,
	telemetry.OrchestratorHostReclaimPages,
	telemetry.OrchestratorHostInterrupts,
}

// KernelCounters exports host kernel counters from /proc.
type KernelCounters struct {
	readFile func(string) ([]byte, error)
	log      logger.Logger
	files    []kernelFile

	mu        sync.Mutex
	disabled  map[string]bool
	failing   map[string]bool
	baselines map[string]int64

	registration metric.Registration
	closeOnce    sync.Once
	closeErr     error
}

func NewKernelCounters(provider metric.MeterProvider) (*KernelCounters, error) {
	return newKernelCounters(provider, os.ReadFile, logger.L())
}

func newKernelCounters(provider metric.MeterProvider, readFile func(string) ([]byte, error), log logger.Logger) (*KernelCounters, error) {
	k := &KernelCounters{
		readFile:  readFile,
		log:       log,
		files:     kernelFiles(),
		disabled:  make(map[string]bool),
		failing:   make(map[string]bool),
		baselines: make(map[string]int64),
	}

	meter := provider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics")
	counters := make(map[telemetry.ObservableCounterType]metric.Int64ObservableCounter, len(kernelCounterNames))
	observables := make([]metric.Observable, 0, len(kernelCounterNames))
	for _, name := range kernelCounterNames {
		counter, err := telemetry.GetObservableCounter(meter, name)
		if err != nil {
			return nil, fmt.Errorf("creating kernel counter %s: %w", name, err)
		}
		counters[name] = counter
		observables = append(observables, counter)
	}

	registration, err := meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		k.collect(ctx, func(sample kernelSample) {
			observer.ObserveInt64(counters[sample.counter], sample.value, metric.WithAttributeSet(sample.attrs))
		})

		return nil
	}, observables...)
	if err != nil {
		return nil, fmt.Errorf("registering kernel counters: %w", err)
	}
	k.registration = registration

	return k, nil
}

// A missing file means the kernel lacks it (e.g. PSI off), so the file is
// dropped; values are relative to the first reading because the kernel counts
// from boot while the SDK stamps the series start at process start.
func (k *KernelCounters) collect(ctx context.Context, report func(kernelSample)) {
	k.mu.Lock()
	defer k.mu.Unlock()

	for _, file := range k.files {
		if k.disabled[file.path] {
			continue
		}

		samples, err := k.read(file)
		if errors.Is(err, fs.ErrNotExist) {
			k.disabled[file.path] = true
			k.log.Info(ctx, "kernel counter source not available, not exported", zap.String("path", file.path))

			continue
		}
		if err != nil {
			if !k.failing[file.path] {
				k.failing[file.path] = true
				k.log.Warn(ctx, "kernel counter source failing", zap.String("path", file.path), zap.Error(err))
			}

			continue
		}
		if k.failing[file.path] {
			k.failing[file.path] = false
			k.log.Info(ctx, "kernel counter source recovered", zap.String("path", file.path))
		}

		for _, sample := range samples {
			key := string(sample.counter) + "|" + sample.attrs.Encoded(attribute.DefaultEncoder())
			baseline, seen := k.baselines[key]
			if !seen {
				baseline = sample.value
				k.baselines[key] = baseline
			}
			sample.value = max(sample.value-baseline, 0)
			report(sample)
		}
	}
}

func (k *KernelCounters) read(file kernelFile) ([]kernelSample, error) {
	raw, err := k.readFile(file.path)
	if err != nil {
		return nil, err
	}

	return file.parse(raw)
}

func (k *KernelCounters) Close(context.Context) error {
	k.closeOnce.Do(func() {
		k.closeErr = k.registration.Unregister()
	})

	return k.closeErr
}

// The cpu "full" line (5.13+) is always zero at the system level; it is kept
// on purpose as a negative control, since a non-zero value means a bad read.
var pressureResources = []string{"cpu", "memory", "io"}

func pressureParser(resource string) func([]byte) ([]kernelSample, error) {
	return func(raw []byte) ([]kernelSample, error) {
		var samples []kernelSample
		for line := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 0 {
				continue
			}
			scope := fields[0]
			if scope != "some" && scope != "full" {
				continue
			}

			total, found := int64(0), false
			for _, field := range fields[1:] {
				value, ok := strings.CutPrefix(field, "total=")
				if !ok {
					continue
				}
				parsed, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("parsing pressure total %q: %w", field, err)
				}
				total, found = parsed, true
			}
			if !found {
				return nil, fmt.Errorf("pressure line without total: %q", line)
			}

			samples = append(samples, kernelSample{
				counter: telemetry.OrchestratorHostPressureStall,
				attrs:   attribute.NewSet(attribute.String("resource", resource), attribute.String("scope", scope)),
				value:   total,
			})
		}
		if len(samples) == 0 {
			return nil, errors.New("empty pressure file")
		}

		return samples, nil
	}
}

// allocstall_<zone> lines are summed so the series does not depend on the
// host's zone layout.
var vmStatFields = map[string]kernelSample{
	"allocstall":     {counter: telemetry.OrchestratorHostReclaimStalls, attrs: *attribute.EmptySet()},
	"pgscan_direct":  {counter: telemetry.OrchestratorHostReclaimPages, attrs: attribute.NewSet(attribute.String("kind", "scan"))},
	"pgsteal_direct": {counter: telemetry.OrchestratorHostReclaimPages, attrs: attribute.NewSet(attribute.String("kind", "steal"))},
}

func parseVMStat(raw []byte) ([]kernelSample, error) {
	values := make(map[string]int64)
	for line := range strings.SplitSeq(string(raw), "\n") {
		name, value, ok := strings.Cut(strings.TrimSpace(line), " ")
		if !ok {
			continue
		}
		if strings.HasPrefix(name, "allocstall_") {
			name = "allocstall"
		}
		if _, wanted := vmStatFields[name]; !wanted {
			continue
		}
		parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing vmstat %s: %w", name, err)
		}
		values[name] += parsed
	}
	if len(values) == 0 {
		return nil, errors.New("no wanted vmstat fields")
	}

	samples := make([]kernelSample, 0, len(values))
	for name, value := range values {
		sample := vmStatFields[name]
		sample.value = value
		samples = append(samples, sample)
	}

	return samples, nil
}

var interruptKinds = map[string]attribute.Set{
	"TLB:": attribute.NewSet(attribute.String("kind", "tlb_shootdown")),
	"CAL:": attribute.NewSet(attribute.String("kind", "function_call")),
}

// Per-CPU counts are 32-bit and wrap, so each CPU adds its difference modulo
// 2^32, counted from the first reading. A difference above 2^31 cannot be a
// wrap (that would be 2^31 interrupts on one CPU in one tick), so it is read
// as a count that went back and rejected. The last good reading is kept, so
// reads keep failing until that CPU's count passes it again; the kernel's
// per-CPU counts only grow until reboot, so this needs a bad earlier read.
const maxInterruptStep = uint32(1) << 31

// Every wanted row is parsed before prev and totals change, so a bad read
// leaves them at the last good reading. Only amd64 always has TLB and CAL
// rows (arm64 names its IPIs differently), so their absence means a missing
// source elsewhere but a bad read on amd64, where the source is retried.
func interruptsParser(goarch string) func([]byte) ([]kernelSample, error) {
	prev := make(map[string]map[string]uint32)
	totals := make(map[string]int64)

	return func(raw []byte) ([]kernelSample, error) {
		header, rows, _ := bytes.Cut(raw, []byte("\n"))
		names := strings.Fields(string(header))

		var labels []string
		current := make(map[string]map[string]uint32, len(interruptKinds))
		added := make(map[string]int64, len(interruptKinds))
		for len(rows) > 0 {
			var line []byte
			line, rows, _ = bytes.Cut(rows, []byte("\n"))
			row := bytes.TrimLeft(line, " ")
			label, _, _ := bytes.Cut(row, []byte(" "))
			if _, wanted := interruptKinds[string(label)]; !wanted {
				continue
			}

			name := string(label)
			counts, err := parseInterruptCounts(name, strings.Fields(string(row))[1:], len(names))
			if err != nil {
				return nil, err
			}

			last := prev[name]
			byCPU := make(map[string]uint32, len(counts))
			for cpu, count := range counts {
				byCPU[names[cpu]] = count
				// Nothing to add on the first reading or for a CPU that just came online.
				before, known := last[names[cpu]]
				if !known {
					continue
				}
				diff := count - before
				if diff > maxInterruptStep {
					return nil, fmt.Errorf("interrupt row %s %s went back from %d to %d", name, names[cpu], before, count)
				}
				added[name] += int64(diff)
			}
			current[name] = byCPU
			labels = append(labels, name)
		}
		if len(labels) == 0 {
			err := errors.New("no TLB or CAL interrupt rows")
			if goarch != "amd64" {
				err = fmt.Errorf("%w: %w", err, fs.ErrNotExist)
			}

			return nil, err
		}

		samples := make([]kernelSample, 0, len(labels))
		for _, name := range labels {
			prev[name] = current[name]
			totals[name] += added[name]
			samples = append(samples, kernelSample{counter: telemetry.OrchestratorHostInterrupts, attrs: interruptKinds[name], value: totals[name]})
		}

		return samples, nil
	}
}

// A real row ends with a description ("TLB:  5  7  TLB shootdowns"), so a
// number right after the last CPU's count means the row has more counts than
// the header has CPUs, e.g. a CPU came online between the two lines.
func parseInterruptCounts(label string, fields []string, cpus int) ([]uint32, error) {
	if len(fields) < cpus {
		return nil, fmt.Errorf("interrupt row %s has %d fields for %d CPUs", label, len(fields), cpus)
	}
	if len(fields) > cpus {
		if _, err := strconv.ParseUint(fields[cpus], 10, 64); err == nil {
			return nil, fmt.Errorf("interrupt row %s has more counts than its %d CPUs", label, cpus)
		}
	}

	counts := make([]uint32, cpus)
	for cpu := range counts {
		count, err := strconv.ParseUint(fields[cpu], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("parsing interrupt row %s CPU %d: %w", label, cpu, err)
		}
		counts[cpu] = uint32(count)
	}

	return counts, nil
}
