//go:build linux

package metrics

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
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
	files := make([]kernelFile, 0, len(pressureResources)+1)
	for _, resource := range pressureResources {
		files = append(files, kernelFile{path: "/proc/pressure/" + resource, parse: pressureParser(resource)})
	}

	return append(files, kernelFile{path: "/proc/vmstat", parse: parseVMStat})
}

var kernelCounterNames = []telemetry.ObservableCounterType{
	telemetry.OrchestratorHostPressureStall,
	telemetry.OrchestratorHostReclaimStalls,
	telemetry.OrchestratorHostReclaimPages,
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
