package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/orchestrator/cmd/clean-nfs-cache/cleaner"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/version"
	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	serviceName = "clean-nfs-cache"
	commitSHA   = ""
)

var serviceVersion = version.Version

func main() {
	ctx := context.Background()

	opts, log, tel, metrics, meterProvider, err := configure(ctx)
	if err != nil {
		fmt.Println("NFS cache cleaner failed:", err)
		if tel != nil {
			tel.Shutdown(ctx)
		}
		os.Exit(1)
	}

	err = run(ctx, opts, log, metrics)
	if err != nil {
		log.Error(ctx, "NFS cache cleaner failed", zap.Error(err))
	}

	// Bounded shutdown so a slow/unreachable collector can't hang the job. Shutdown
	// flushes the final metrics/logs (no separate ForceFlush needed).
	log.Sync()
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	if meterProvider != nil {
		_ = meterProvider.Shutdown(shutdownCtx)
	}
	tel.Shutdown(shutdownCtx)
	cancel()

	if err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, opts cleaner.Options, log logger.Logger, metrics *cleaner.Metrics) error {
	if opts.TargetBytesToDelete == 0 {
		log.Info(ctx, "disk already at or below target, nothing to do",
			zap.Float64("target_disk_usage_percent", opts.TargetDiskUsagePercent))

		return nil
	}

	if err := cleaner.VerifyChunksCacheRoot(opts.Path); err != nil {
		return err
	}

	c := cleaner.NewCleaner(opts, log, metrics)

	if err := c.Clean(ctx); err != nil {
		return err
	}

	if c.Deleted.Load() == 0 {
		log.Info(ctx, "no builds removed")
	}

	return nil
}

func configure(ctx context.Context) (cleaner.Options, logger.Logger, *telemetry.Client, *cleaner.Metrics, *sdkmetric.MeterProvider, error) {
	var opts cleaner.Options
	var featureFlagPresent bool

	flags := flag.NewFlagSet("clean-nfs-cache", flag.ExitOnError)
	flags.Uint64Var(&opts.TargetBytesToDelete, "target-bytes-to-delete", 0, "target number of bytes to delete (overrides disk-usage-target-percent)")
	flags.Float64Var(&opts.TargetDiskUsagePercent, "disk-usage-target-percent", 90, "disk usage target as a % (0-100)")
	flags.BoolVar(&opts.DryRun, "dry-run", true, "dry run")
	flags.IntVar(&opts.MaxConcurrentStat, "max-concurrent-stat", 32, "number of concurrent statx goroutines (the NFS-latency-bound step — keep many RPCs in flight)")
	flags.IntVar(&opts.MaxConcurrentScan, "max-concurrent-scan", 32, "number of concurrent build readdir goroutines")
	flags.IntVar(&opts.MaxConcurrentDelete, "max-concurrent-delete", 16, "number of concurrent RemoveAll goroutines")
	flags.IntVar(&opts.SampleMinFiles, "sample-min", 8, "minimum chunk-atime samples per build")
	flags.IntVar(&opts.SamplePercent, "sample-pct", 10, "target chunk-atime samples per build as a percent of its chunk count")
	flags.IntVar(&opts.SampleMaxFiles, "sample-max", 64, "maximum chunk-atime samples per build (caps cost on huge builds)")
	flags.IntVar(&opts.BuildSampleMin, "build-sample-min", 0, "floor of the per-run survey sample (0 = no floor); only applies when build-sample-max > 0")
	flags.IntVar(&opts.BuildSamplePercent, "build-sample-pct", 100, "target build dirs to scan per run, as a percent of the root's build count; only applies when build-sample-max > 0")
	flags.IntVar(&opts.BuildSampleMax, "build-sample-max", 0, "cap on build dirs scanned per run; 0 (default) = no limit (scan every build). Set > 0 to bound readdir/stat at huge caches, then min/pct apply")
	flags.DurationVar(&opts.Grace, "grace", cleaner.GracePeriod, "never delete a build that has been warm within this window — warmth is the most-recent sampled chunk access (atime), or create time (btime) for a build with no chunks yet. Skips new and recently-used builds; 0 = no floor")
	flags.BoolVar(&opts.Verify, "verify", false, "before each cold delete, stat the build's non-chunk files and record their size vs the flat estimate (chunk sizes are trusted from filenames); off by default")

	args := os.Args[1:] // skip the command name
	if err := flags.Parse(args); err != nil {
		return opts, nil, nil, nil, nil, fmt.Errorf("could not parse flags: %w", err)
	}

	args = flags.Args()
	if len(args) != 1 {
		return opts, nil, nil, nil, nil, cleaner.ErrUsage
	}
	opts.Path = args[0]

	ffc, err := featureflags.NewClient()
	if err != nil {
		return opts, nil, nil, nil, nil, err
	}
	defer ffc.Close(ctx)

	v := ffc.JSONFlag(ctx, featureflags.CleanNFSCache)
	featureFlagPresent = v.Type() == ldvalue.ObjectType
	if featureFlagPresent {
		m := v.AsValueMap()
		if m.Get("maxConcurrentDelete").IsNumber() {
			opts.MaxConcurrentDelete = m.Get("maxConcurrentDelete").IntValue()
		}
		if m.Get("maxConcurrentScan").IsNumber() {
			opts.MaxConcurrentScan = m.Get("maxConcurrentScan").IntValue()
		}
		if m.Get("maxConcurrentStat").IsNumber() {
			opts.MaxConcurrentStat = m.Get("maxConcurrentStat").IntValue()
		}
		if m.Get("sampleMin").IsNumber() {
			opts.SampleMinFiles = m.Get("sampleMin").IntValue()
		}
		if m.Get("samplePct").IsNumber() {
			opts.SamplePercent = m.Get("samplePct").IntValue()
		}
		if m.Get("sampleMax").IsNumber() {
			opts.SampleMaxFiles = m.Get("sampleMax").IntValue()
		}
		if m.Get("buildSampleMin").IsNumber() {
			opts.BuildSampleMin = m.Get("buildSampleMin").IntValue()
		}
		if m.Get("buildSamplePct").IsNumber() {
			opts.BuildSamplePercent = m.Get("buildSamplePct").IntValue()
		}
		if m.Get("buildSampleMax").IsNumber() {
			opts.BuildSampleMax = m.Get("buildSampleMax").IntValue()
		}
		if m.Get("graceSeconds").IsNumber() {
			opts.Grace = time.Duration(m.Get("graceSeconds").IntValue()) * time.Second
		}
		if m.Get("targetBytesToDelete").IsNumber() {
			opts.TargetBytesToDelete = uint64(m.Get("targetBytesToDelete").Float64Value())
		}
		if m.Get("verify").IsBool() {
			opts.Verify = m.Get("verify").BoolValue()
		}
	}

	tel, err := telemetry.New(ctx, env.GetNodeID(), serviceName, commitSHA, serviceVersion, "")
	if err != nil {
		return opts, nil, nil, nil, nil, fmt.Errorf("failed to set up telemetry: %w", err)
	}

	endpoint := os.Getenv("OTEL_COLLECTOR_GRPC_ENDPOINT")
	var cores []zapcore.Core
	if endpoint != "" {
		cores = append(cores, logger.GetOTELCore(tel.LogsProvider, serviceName))
	}

	// Build the cleaner's own meter provider (short export period) and hand its
	// meter to the instruments; telemetry.New's default provider exports too
	// rarely for this short-lived job (see meterExportPeriod).
	metrics := cleaner.NoopMetrics()
	var meterProvider *sdkmetric.MeterProvider
	if endpoint != "" {
		mp, m, merr := newMeterProvider(ctx, endpoint)
		if merr != nil {
			tel.Shutdown(ctx)

			return opts, nil, nil, nil, nil, fmt.Errorf("failed to set up cleaner metrics: %w", merr)
		}
		meterProvider, metrics = mp, m
	}

	l := utils.Must(logger.NewLogger(logger.LoggerConfig{
		ServiceName:   serviceName,
		IsInternal:    true,
		IsDebug:       env.IsDebug(),
		Cores:         cores,
		EnableConsole: true,
	}))

	if featureFlagPresent {
		l.Info(ctx, "feature flag present", zap.String("flag", featureflags.CleanNFSCache.String()))
	}

	if opts.TargetBytesToDelete == 0 && opts.TargetDiskUsagePercent > 0 {
		diskInfo, derr := cleaner.GetDiskInfo(ctx, opts.Path)
		if derr != nil {
			tel.Shutdown(ctx)

			return opts, nil, nil, nil, nil, fmt.Errorf("could not get disk info: %w", derr)
		}
		targetDiskUsage := uint64(opts.TargetDiskUsagePercent / 100 * float64(diskInfo.Total))
		if uint64(diskInfo.Used) > targetDiskUsage {
			opts.TargetBytesToDelete = uint64(diskInfo.Used) - targetDiskUsage
		}
	}

	l.Info(ctx, "configured",
		zap.Bool("dry_run", opts.DryRun),
		zap.Uint64("target_bytes_to_delete", opts.TargetBytesToDelete),
		zap.Float64("target_disk_usage_percent", opts.TargetDiskUsagePercent),
		zap.Int("sample_min", opts.SampleMinFiles),
		zap.Int("sample_pct", opts.SamplePercent),
		zap.Int("sample_max", opts.SampleMaxFiles),
		zap.Int("build_sample_min", opts.BuildSampleMin),
		zap.Int("build_sample_pct", opts.BuildSamplePercent),
		zap.Int("build_sample_max", opts.BuildSampleMax),
		zap.Duration("grace", opts.Grace),
		zap.String("path", opts.Path),
		zap.Int("max_concurrent_stat", opts.MaxConcurrentStat),
		zap.Int("max_concurrent_scan", opts.MaxConcurrentScan),
		zap.Int("max_concurrent_delete", opts.MaxConcurrentDelete),
	)

	return opts, l, tel, metrics, meterProvider, nil
}
