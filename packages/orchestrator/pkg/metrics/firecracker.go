//go:build linux

package metrics

import (
	"context"
	"fmt"
	"maps"
	"math"
	"os"
	"sync"
	"time"

	"github.com/tklauser/go-sysconf"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	firecrackerPollInterval = 30 * time.Second
	untrackedGrace          = 5 * time.Minute
	trackerLogBudget        = 10
)

type trackerSnapshot struct {
	total       int64
	untracked   int64
	overMax     int64
	unknownMax  int64
	success     bool
	lastSuccess time.Time
}

type processObservation struct {
	socket    string
	metadata  sandbox.ProcessMetadata
	untracked bool
	overMax   bool
	reported  bool
}

type FirecrackerTracker struct {
	scan     func(context.Context) ([]firecrackerProcess, error)
	metadata func() map[string]sandbox.ProcessMetadata
	seen     map[processIdentity]processObservation

	mu           sync.RWMutex
	snapshot     trackerSnapshot
	registration metric.Registration
	closed       chan struct{}
	closeOnce    sync.Once
	closeErr     error
}

func NewFirecrackerTracker(provider metric.MeterProvider, sandboxes *sandbox.Map) (*FirecrackerTracker, error) {
	hz, err := sysconf.Sysconf(sysconf.SC_CLK_TCK)
	if err != nil {
		return nil, fmt.Errorf("reading process clock frequency: %w", err)
	}
	reader := firecrackerProcReader{root: "/proc", clockHz: float64(hz), readFile: os.ReadFile}

	return newFirecrackerTracker(provider, reader.scan, sandboxes.ProcessMetadata)
}

func newFirecrackerTracker(provider metric.MeterProvider, scan func(context.Context) ([]firecrackerProcess, error), metadata func() map[string]sandbox.ProcessMetadata) (*FirecrackerTracker, error) {
	c := &FirecrackerTracker{
		scan:     scan,
		metadata: metadata,
		seen:     make(map[processIdentity]processObservation),
		closed:   make(chan struct{}),
	}

	meter := provider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics")

	var gauges [6]metric.Int64ObservableGauge
	for i, name := range []telemetry.GaugeIntType{
		telemetry.FirecrackerProcesses, telemetry.FirecrackerProcessesUntracked,
		telemetry.FirecrackerProcessesOverMaxLength, telemetry.FirecrackerProcessesUnknownMaxLength,
		telemetry.FirecrackerTrackerSuccess, telemetry.FirecrackerTrackerLastSuccessAge,
	} {
		gauge, err := telemetry.GetGaugeInt(meter, name)
		if err != nil {
			return nil, fmt.Errorf("creating Firecracker tracker metric: %w", err)
		}
		gauges[i] = gauge
	}

	registration, err := meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		c.mu.RLock()
		sample := c.snapshot
		c.mu.RUnlock()

		var success int64
		if sample.success {
			success = 1
			observer.ObserveInt64(gauges[0], sample.total)
			observer.ObserveInt64(gauges[1], sample.untracked)
			observer.ObserveInt64(gauges[2], sample.overMax)
			observer.ObserveInt64(gauges[3], sample.unknownMax)
		}

		observer.ObserveInt64(gauges[4], success)
		if !sample.lastSuccess.IsZero() {
			age := max(time.Since(sample.lastSuccess), 0)
			observer.ObserveInt64(gauges[5], int64(age.Seconds()))
		}

		return nil
	}, gauges[0], gauges[1], gauges[2], gauges[3], gauges[4], gauges[5])
	if err != nil {
		return nil, fmt.Errorf("registering Firecracker tracker: %w", err)
	}

	c.registration = registration

	return c, nil
}

func (c *FirecrackerTracker) Start(ctx context.Context) error {
	ticker := time.NewTicker(firecrackerPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.closed:
			return nil
		default:
		}

		c.sample(ctx, time.Now())

		select {
		case <-ctx.Done():
			return nil
		case <-c.closed:
			return nil
		case <-ticker.C:
		}
	}
}

func (c *FirecrackerTracker) Close(context.Context) error {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.closeErr = c.registration.Unregister()
	})

	return c.closeErr
}

func (c *FirecrackerTracker) sample(ctx context.Context, now time.Time) {
	before := maps.Clone(c.metadata())
	processes, err := c.scan(ctx)
	if err != nil {
		c.mu.Lock()
		c.snapshot.success = false
		c.mu.Unlock()

		logger.L().Warn(ctx, "Firecracker tracker failed", zap.Error(err))

		return
	}

	metadata := maps.Clone(c.metadata())
	if metadata == nil {
		metadata = make(map[string]sandbox.ProcessMetadata)
	}

	// A lifecycle retiring during the OS scan was still owned when sampled.
	for socket, old := range before {
		if current, ok := metadata[socket]; ok {
			current.Tracked = current.Tracked || old.Tracked
			metadata[socket] = current
		} else {
			metadata[socket] = old
		}
	}

	result := trackerSnapshot{success: true}
	next := make(map[processIdentity]processObservation, len(processes))
	logged := 0

	for _, process := range processes {
		result.total++

		old := c.seen[process.identity]
		observation := processObservation{socket: process.socket}
		if old.socket == process.socket {
			observation.metadata = old.metadata
		}

		current, exists := metadata[process.socket]
		if exists {
			observation.metadata = current
		}

		info := observation.metadata
		observation.untracked = (!exists || !current.Tracked) && process.age > untrackedGrace
		if observation.untracked {
			result.untracked++
		}

		if !info.Temporary && !info.Build {
			if info.StartedAt.IsZero() || info.MaxLengthHours <= 0 || info.MaxLengthHours > math.MaxInt64/int64(time.Hour) {
				result.unknownMax++
			} else if now.After(info.StartedAt.Add(time.Duration(info.MaxLengthHours) * time.Hour)) {
				observation.overMax = true
				result.overMax++
			}
		}

		observation.reported = old.reported && old.untracked == observation.untracked && old.overMax == observation.overMax
		if (observation.untracked || observation.overMax) && !observation.reported {
			if logged < trackerLogBudget {
				logger.L().Warn(ctx, "Firecracker process tracker anomaly",
					zap.Int("pid", process.identity.pid), zap.Uint64("process_start_ticks", process.identity.startTicks),
					zap.Duration("process_age", process.age), logger.WithSandboxID(info.SandboxID), logger.WithLifecycleID(info.LifecycleID),
					zap.Bool("untracked", observation.untracked), zap.Bool("over_max_length", observation.overMax))
				logged++
				observation.reported = true
			}
		}

		next[process.identity] = observation
	}

	c.seen = next

	c.mu.Lock()
	result.lastSuccess = time.Now()
	c.snapshot = result
	c.mu.Unlock()
}
