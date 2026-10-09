//go:build linux

package metrics

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	maxAcceptableSandboxClockDriftSec = 5

	sbxMemThresholdPct = 80
	sbxCpuThresholdPct = 80
	// A CPU online/offline write older than this is stuck in the guest kernel.
	sbxCpuWriteStuckMs = 10_000

	// Caps the guest-supplied process name, in runes. Real names are escaped to ASCII, 60 at most.
	maxOomProcessLen = 64

	// Caps the kill lines one poll writes.
	maxOomKillsLogged = 10

	minEnvdVersionForMetrics         = "0.1.5"
	minEnvVersionForMetricsTimestamp = "0.1.3"
	minEnvdVersionForMemoryMetrics   = "0.2.4"
	minEnvdVersionForDiskMetrics     = "0.2.4"
	minEnvdVersionForCacheMetrics    = "0.5.9"

	timeoutGetMetrics         = 100 * time.Millisecond
	metricsParallelismFactor  = 5 // Used to calculate number of concurrently sandbox metrics requests
	sandboxMetricExportPeriod = 5 * time.Second
)

type (
	GetSandboxMetricsFunc func(ctx context.Context) (*sandbox.Metrics, error)
)

type SandboxObserver struct {
	meterExporter  sdkmetric.Exporter
	registration   metric.Registration
	exportInterval time.Duration

	sandboxes *sandbox.Map

	meter       metric.Meter
	cpuTotal    metric.Int64ObservableGauge
	cpuUsed     metric.Float64ObservableGauge
	cpuPossible metric.Int64ObservableGauge
	cpuTarget   metric.Int64ObservableGauge
	cpuAttempts metric.Int64ObservableGauge
	cpuPending  metric.Int64ObservableGauge
	memoryTotal metric.Int64ObservableGauge
	memoryUsed  metric.Int64ObservableGauge
	memoryCache metric.Int64ObservableGauge
	diskTotal   metric.Int64ObservableGauge
	diskUsed    metric.Int64ObservableGauge
}

func NewSandboxObserver(ctx context.Context, nodeID, serviceName, serviceCommit, serviceVersion, serviceInstanceID string, sandboxes *sandbox.Map) (*SandboxObserver, error) {
	deltaTemporality := otlpmetricgrpc.WithTemporalitySelector(func(kind sdkmetric.InstrumentKind) metricdata.Temporality {
		// Use delta temporality for gauges and cumulative for all other instrument kinds.
		// This is used to prevent reporting sandbox metrics indefinitely.
		if kind == sdkmetric.InstrumentKindGauge {
			return metricdata.DeltaTemporality
		}

		return metricdata.CumulativeTemporality
	})

	externalMeterExporter, err := telemetry.NewMeterExporter(ctx, deltaTemporality)
	if err != nil {
		return nil, fmt.Errorf("failed to create external meter exporter: %w", err)
	}

	res, err := telemetry.GetResource(ctx, nodeID, serviceName, serviceCommit, serviceVersion, serviceInstanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	meterProvider, err := telemetry.NewMeterProvider(
		externalMeterExporter,
		sandboxMetricExportPeriod,
		res,
		// No limit on the number of metrics to be exported, as we want to export all sandbox metrics
		sdkmetric.WithCardinalityLimit(0),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create external metric provider: %w", err)
	}

	meter := meterProvider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics")
	cpuTotal, err := telemetry.GetGaugeInt(meter, telemetry.SandboxCpuTotalGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create CPU total gauge: %w", err)
	}

	cpuUsed, err := telemetry.GetGaugeFloat(meter, telemetry.SandboxCpuUsedGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create CPU used gauge: %w", err)
	}

	cpuPossible, err := telemetry.GetGaugeInt(meter, telemetry.SandboxCpuPossibleGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create CPU possible gauge: %w", err)
	}

	cpuTarget, err := telemetry.GetGaugeInt(meter, telemetry.SandboxCpuTargetGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create CPU target gauge: %w", err)
	}

	cpuAttempts, err := telemetry.GetGaugeInt(meter, telemetry.SandboxCpuTargetAttemptsGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create CPU target attempts gauge: %w", err)
	}

	cpuPending, err := telemetry.GetGaugeInt(meter, telemetry.SandboxCpuWritePendingGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create CPU write pending gauge: %w", err)
	}

	memoryTotal, err := telemetry.GetGaugeInt(meter, telemetry.SandboxRamTotalGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create memory total gauge: %w", err)
	}

	memoryUsed, err := telemetry.GetGaugeInt(meter, telemetry.SandboxRamUsedGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create memory used gauge: %w", err)
	}

	memoryCache, err := telemetry.GetGaugeInt(meter, telemetry.SandboxRamCacheGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create memory cache gauge: %w", err)
	}

	diskTotal, err := telemetry.GetGaugeInt(meter, telemetry.SandboxDiskTotalGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create disk total gauge: %w", err)
	}

	diskUsed, err := telemetry.GetGaugeInt(meter, telemetry.SandboxDiskUsedGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create disk used gauge: %w", err)
	}

	so := &SandboxObserver{
		exportInterval: sandboxMetricExportPeriod,
		meterExporter:  externalMeterExporter,
		sandboxes:      sandboxes,
		meter:          meter,
		cpuTotal:       cpuTotal,
		cpuUsed:        cpuUsed,
		cpuPossible:    cpuPossible,
		cpuTarget:      cpuTarget,
		cpuAttempts:    cpuAttempts,
		cpuPending:     cpuPending,
		memoryTotal:    memoryTotal,
		memoryUsed:     memoryUsed,
		memoryCache:    memoryCache,
		diskTotal:      diskTotal,
		diskUsed:       diskUsed,
	}

	registration, err := so.startObserving()
	if err != nil {
		return nil, fmt.Errorf("failed to start observing sandbox metrics: %w", err)
	}

	// Register the callback to start observing sandbox metrics
	so.registration = registration

	return so, nil
}

func (so *SandboxObserver) startObserving() (metric.Registration, error) {
	unregister, err := so.meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			sbxCount := so.sandboxes.Count()

			wg := errgroup.Group{}
			// Run concurrently to prevent blocking if there are many sandboxes other callbacks
			limit := math.Ceil(float64(sbxCount) / metricsParallelismFactor)
			wg.SetLimit(int(limit))

			for _, sbx := range so.sandboxes.Items() {
				ok, err := utils.IsGTEVersion(sbx.Config.Envd.Version, minEnvdVersionForMetrics)
				if err != nil {
					logger.L().Error(ctx, "Failed to check envd version", zap.Error(err), logger.WithSandboxID(sbx.Runtime.SandboxID))

					continue
				}
				if !ok {
					continue
				}

				wg.Go(func() error {
					// Make sure the sandbox doesn't change while we are getting metrics (the slot could be assigned to another sandbox)
					sbxMetrics, err := sbx.Checks.GetMetrics(ctx, timeoutGetMetrics)
					if err != nil {
						// Sandbox has stopped
						if errors.Is(err, sandbox.ErrChecksStopped) {
							return nil
						}

						return err
					}

					attributes := metric.WithAttributes(attribute.String("sandbox_id", sbx.Runtime.SandboxID), attribute.String("team_id", sbx.Runtime.TeamID), attribute.String("build_id", sbx.Runtime.BuildID), attribute.String("sandbox_type", sbx.Runtime.SandboxType.String()))

					ok, err = utils.IsGTEVersion(sbx.Config.Envd.Version, minEnvVersionForMetricsTimestamp)
					if err != nil {
						logger.L().Error(ctx, "Failed to check envd version for timestamp in metrics", zap.Error(err), logger.WithSandboxID(sbx.Runtime.SandboxID))
					}

					// Check if sandbox clock are in acceptable drift from orchestrator host clock
					// We want to do it asap so gap between getting metrics and logging is minimal
					if ok {
						hostTmSec := time.Now().UTC().Unix()
						sbxTmSec := sbxMetrics.Timestamp
						sbxDrift := math.Abs(float64(hostTmSec - sbxTmSec))

						if sbxDrift > maxAcceptableSandboxClockDriftSec {
							logger.L().Warn(ctx, "Significant clock drift detected between sandbox and host",
								logger.WithSandboxID(sbx.Runtime.SandboxID),
								logger.WithTeamID(sbx.Runtime.TeamID),
								logger.WithTemplateID(sbx.Runtime.TemplateID),
								logger.WithEnvdVersion(sbx.Config.Envd.Version),
								logger.Time("sandbox_start", sbx.GetStartedAt()),
								zap.Int64("clock_host", hostTmSec),
								zap.Int64("clock_sbx", sbxTmSec),
								zap.Float64("clock_drift_seconds", sbxDrift),
							)
						}
					}

					o.ObserveInt64(so.cpuTotal, sbxMetrics.CPUCount, attributes)
					o.ObserveFloat64(so.cpuUsed, sbxMetrics.CPUUsedPercent, attributes)
					// An envd that predates CPU hotplug reports none of these. One that does reports cpu_possible,
					// except when its sysfs read fails: then it still carries the target, so both are checked.
					if sbxMetrics.CPUPossible > 0 || sbxMetrics.CPUTarget > 0 {
						o.ObserveInt64(so.cpuPossible, sbxMetrics.CPUPossible, attributes)
						o.ObserveInt64(so.cpuTarget, sbxMetrics.CPUTarget, attributes)
						o.ObserveInt64(so.cpuAttempts, sbxMetrics.CPUTargetAttempts, attributes)
						o.ObserveInt64(so.cpuPending, sbxMetrics.CPUWritePendingMs, attributes)

						// Logged on entering the stuck state only; the gauge carries it from there.
						stuck := sbxMetrics.CPUWritePendingMs >= sbxCpuWriteStuckMs
						if sbx.CPUWriteStuck.Swap(stuck) != stuck && stuck {
							sbxlogger.E(sbx).Warn(ctx, "CPU hotplug write stuck in the guest kernel",
								zap.Int64("cpu_write_pending_ms", sbxMetrics.CPUWritePendingMs),
								zap.Int64("cpu_online", sbxMetrics.CPUCount),
								zap.Int64("cpu_target", sbxMetrics.CPUTarget),
							)
						}
					}

					memoryTotal, memoryUsed, memoryReported, err := sandboxMemory(sbx.Config.Envd.Version, sbxMetrics)
					if err != nil {
						logger.L().Error(ctx, "Failed to check envd version for memory metrics", zap.Error(err), logger.WithSandboxID(sbx.Runtime.SandboxID))
					}
					if memoryReported {
						o.ObserveInt64(so.memoryTotal, memoryTotal, attributes)
						o.ObserveInt64(so.memoryUsed, memoryUsed, attributes)
					}

					ok, err := utils.IsGTEVersion(sbx.Config.Envd.Version, minEnvdVersionForCacheMetrics)
					if err != nil {
						logger.L().Error(ctx, "Failed to check envd version for cache metrics", zap.Error(err), logger.WithSandboxID(sbx.Runtime.SandboxID))
					}
					if ok {
						o.ObserveInt64(so.memoryCache, sbxMetrics.MemCache, attributes)
					}

					ok, err = utils.IsGTEVersion(sbx.Config.Envd.Version, minEnvdVersionForDiskMetrics)
					if err != nil {
						logger.L().Error(ctx, "Failed to check envd version for disk metrics", zap.Error(err), logger.WithSandboxID(sbx.Runtime.SandboxID))
					}
					if ok {
						o.ObserveInt64(so.diskTotal, sbxMetrics.DiskTotal, attributes)
						o.ObserveInt64(so.diskUsed, sbxMetrics.DiskUsed, attributes)
					}

					// Log warnings if memory or CPU usage exceeds thresholds
					// Round percentage to 2 decimal places
					if memoryReported {
						memUsedPct := float32(math.Floor(float64(memoryUsed)/float64(memoryTotal)*10000) / 100)
						if memUsedPct >= sbxMemThresholdPct {
							sbxlogger.E(sbx).Warn(ctx, "Memory usage threshold exceeded",
								zap.Float32("mem_used_percent", memUsedPct),
								zap.Float32("mem_threshold_percent", sbxMemThresholdPct),
							)
						}
					}

					if sbxMetrics.CPUUsedPercent >= sbxCpuThresholdPct {
						sbxlogger.E(sbx).Warn(ctx, "CPU usage threshold exceeded",
							zap.Float32("cpu_used_percent", float32(sbxMetrics.CPUUsedPercent)),
							zap.Float32("cpu_threshold_percent", sbxCpuThresholdPct),
						)
					}

					kills, more := unseenOOMKills(sbx, sbxMetrics)
					for _, kill := range kills {
						sbxlogger.E(sbx).Warn(ctx, "Out of memory: process was killed", zap.String("process", kill.Process))
					}
					if more > 0 {
						// envd lists only its latest kills, so there may have been more.
						sbxlogger.E(sbx).Warn(ctx, "Out of memory: at least this many more processes were killed",
							zap.Int("processes", more))
					}

					return nil
				})
			}

			err := wg.Wait()
			if err != nil {
				// Log the error but observe other sandboxes
				logger.L().Warn(ctx, "error during observing sandbox metrics", zap.Error(err))
			}

			return nil
		}, so.cpuTotal, so.cpuUsed, so.cpuPossible, so.cpuTarget, so.cpuAttempts, so.cpuPending,
		so.memoryTotal, so.memoryUsed, so.memoryCache, so.diskTotal, so.diskUsed)
	if err != nil {
		return nil, err
	}

	return unregister, nil
}

const meterExporterShutdownTimeout = 10 * time.Second

func (so *SandboxObserver) Close(ctx context.Context) error {
	if so.meterExporter == nil {
		return nil
	}

	var errs []error

	if so.registration != nil {
		if err := so.registration.Unregister(); err != nil {
			errs = append(errs, fmt.Errorf("failed to unregister sandbox observer callback: %w", err))
		}
	}

	// Use a timeout to prevent hanging on meter exporter shutdown
	shutdownCtx, cancel := context.WithTimeout(ctx, meterExporterShutdownTimeout)
	defer cancel()

	if err := so.meterExporter.Shutdown(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("failed to shutdown sandbox observer meter provider: %w", err))
	}

	return errors.Join(errs...)
}

// unseenOOMKills returns up to maxOomKillsLogged kills not yet logged for sbx, names capped, and
// how many more it saw; envd lists only its latest kills, so there may have been more. Builds log none.
func unseenOOMKills(sbx *sandbox.Sandbox, m *sandbox.Metrics) ([]envd.OOMKill, int) {
	if !sbx.LogsOOMKills() || m.OomKills == nil {
		return nil, 0
	}

	kills := sbx.OOMKills.Unseen(*m.OomKills)
	for i := range kills {
		kills[i].Process = utils.Truncate(kills[i].Process, maxOomProcessLen)
	}

	if len(kills) > maxOomKillsLogged {
		return kills[:maxOomKillsLogged], len(kills) - maxOomKillsLogged
	}

	return kills, 0
}

// sandboxMemory returns the sandbox's memory in bytes. envd reports bytes since
// minEnvdVersionForMemoryMetrics; older envd reports only MiB, so reported is false.
func sandboxMemory(envdVersion string, m *sandbox.Metrics) (total, used int64, reported bool, err error) {
	reported, err = utils.IsGTEVersion(envdVersion, minEnvdVersionForMemoryMetrics)
	if err != nil || !reported {
		return 0, 0, false, err
	}

	return m.MemTotal, m.MemUsed, true, nil
}
