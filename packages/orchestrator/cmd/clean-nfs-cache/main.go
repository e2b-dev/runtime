package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	envconfig "github.com/caarlos0/env/v11"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/redis/go-redis/v9"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/orchestrator/cmd/clean-nfs-cache/cleaner"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/version"
	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	"github.com/e2b-dev/infra/packages/shared/pkg/factories"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/cacheindex"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	serviceName = "clean-nfs-cache"
	commitSHA   = ""
)

// indexConnectTimeout keeps an unreachable Redis from eating the run budget:
// past it the run samples without the index.
var indexConnectTimeout = 10 * time.Second

// redisEnv is the Redis connection the orchestrators use, read from the same
// variables.
type redisEnv struct {
	RedisURL         string `env:"REDIS_URL"`
	RedisClusterURL  string `env:"REDIS_CLUSTER_URL"`
	RedisTLSCABase64 string `env:"REDIS_TLS_CA_BASE64"`
	RedisTLSEnabled  bool   `env:"REDIS_TLS_ENABLED"`
	RedisPassword    string `env:"REDIS_PASSWORD"`
}

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

	indexClient := openIndexClient(ctx, log, opts)
	err = run(ctx, opts, log, metrics, indexClient, redisConfigured())
	if err != nil {
		log.Error(ctx, "NFS cache cleaner failed", zap.Error(err))
	}
	if indexClient != nil {
		_ = factories.CloseCleanly(indexClient)
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

func run(ctx context.Context, opts cleaner.Options, log logger.Logger, metrics *cleaner.Metrics, indexClient redis.UniversalClient, hasRedis bool) error {
	if err := opts.Validate(); err != nil {
		return err
	}

	// Writers record nothing until the marker exists, so with Redis configured a
	// real run creates it whatever the index mode: the index fills before it is used.
	createMarker := hasRedis && !opts.DryRun

	// The byte budget resolves its target after the scan, so only the
	// percent/bytes-to-delete modes can be decided here.
	nothingToDelete := opts.TargetBytesToDelete == 0 && opts.MaxCacheBytes == 0
	if nothingToDelete && indexClient == nil && !createMarker {
		log.Info(ctx, "disk already at or below target, nothing to do",
			zap.Float64("target_disk_usage_percent", opts.TargetDiskUsagePercent))

		return nil
	}

	if err := cleaner.VerifyChunksCacheRoot(opts.Path); err != nil {
		if nothingToDelete && indexClient == nil {
			// Only here for the marker: never write it outside a cache, and keep
			// the quiet exit a run with nothing to delete had before.
			log.Warn(ctx, "cache index marker skipped: not a chunks-cache root", zap.Error(err))

			return nil
		}

		return err
	}

	if createMarker {
		if _, err := cacheindex.EnsureID(opts.Path); err != nil {
			log.Warn(ctx, "cache index marker unavailable; writers stay idle", zap.Error(err))
		}
	}

	c := cleaner.NewCleaner(opts, log, metrics)
	if indexClient != nil {
		idx, err := openIndex(opts, indexClient)
		if err != nil {
			log.Warn(ctx, "cache index unavailable; sampling without it", zap.Error(err))
		} else {
			c.SetIndex(idx)
		}
	}

	if nothingToDelete {
		log.Info(ctx, "disk already at or below target, nothing to do",
			zap.Float64("target_disk_usage_percent", opts.TargetDiskUsagePercent))

		return nil
	}

	if err := c.Clean(ctx); err != nil {
		return err
	}

	if c.Deleted.Load() == 0 {
		log.Info(ctx, "no builds removed")
	}

	return nil
}

// openIndex returns the index named by the cache's marker. A dry run only
// reads the marker; a real run creates it on first use.
func openIndex(opts cleaner.Options, client redis.UniversalClient) (*cacheindex.Index, error) {
	read := cacheindex.EnsureID
	if opts.DryRun {
		read = cacheindex.ReadID
	}
	id, err := read(opts.Path)
	if err != nil {
		return nil, err
	}

	return cacheindex.Open(client, id), nil
}

// redisConfigured reports whether a Redis is set for the index, without
// connecting to it.
func redisConfigured() bool {
	cfg, err := envconfig.ParseAs[redisEnv]()

	return err == nil && (cfg.RedisURL != "" || cfg.RedisClusterURL != "")
}

// openIndexClient connects to Redis when an index mode is on. Any failure,
// including no Redis configured, returns nil: the run samples as before.
func openIndexClient(ctx context.Context, log logger.Logger, opts cleaner.Options) redis.UniversalClient {
	mode := opts.IndexMode
	if mode == "" || mode == cleaner.IndexOff {
		return nil
	}
	cfg, err := envconfig.ParseAs[redisEnv]()
	if err != nil {
		log.Warn(ctx, "cache index: bad Redis config; sampling without it", zap.Error(err))

		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, indexConnectTimeout)
	defer cancel()
	client, err := factories.NewRedisClient(ctx, factories.RedisConfig{
		RedisURL:         cfg.RedisURL,
		RedisClusterURL:  cfg.RedisClusterURL,
		RedisTLSCABase64: cfg.RedisTLSCABase64,
		RedisTLSEnabled:  cfg.RedisTLSEnabled,
		RedisPassword:    cfg.RedisPassword,
		PoolSize:         opts.MaxConcurrentDelete + 4, // each delete worker holds one Redis call at a time
		// Without it a URL read_timeout outlasts indexConnectTimeout on a stalled endpoint.
		ContextTimeoutEnabled: true,
	})
	if err != nil {
		log.Warn(ctx, "cache index: Redis unavailable; sampling without it",
			zap.String("index_mode", mode), zap.Error(err))

		return nil
	}

	return client
}

func configure(ctx context.Context) (cleaner.Options, logger.Logger, *telemetry.Client, *cleaner.Metrics, *sdkmetric.MeterProvider, error) {
	var opts cleaner.Options
	var featureFlagPresent bool
	var badIndexMode string

	flags := flag.NewFlagSet("clean-nfs-cache", flag.ExitOnError)
	flags.Uint64Var(&opts.TargetBytesToDelete, "target-bytes-to-delete", 0, "target number of bytes to delete (overrides disk-usage-target-percent)")
	flags.Float64Var(&opts.TargetDiskUsagePercent, "disk-usage-target-percent", 90, "disk usage target as a % (0-100); needs a real disk — errors out on an elastic filesystem (EFS) unless max-cache-bytes is set")
	flags.Uint64Var(&opts.MaxCacheBytes, "max-cache-bytes", 0, "byte budget for elastic filesystems (EFS): when the cache, sized from chunk filenames, exceeds this, evict coldest-first down to it; replaces disk-usage-target-percent (0 = off)")
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
	flags.StringVar(&opts.IndexMode, "index-mode", cleaner.IndexOff, "last-use index in Redis (REDIS_URL / REDIS_CLUSTER_URL): off = sample as before; index = check the coldest builds the index proposes on disk and delete the cold ones, then sample whatever is left")
	flags.DurationVar(&opts.RunBudget, "run-budget", 0, "wall-time budget for one run: once spent, stop queueing deletes and starting batches; with build-sample-max > 0 and no max-cache-bytes, a run that misses its target scans further samples while time remains. Set it below the schedule interval so the next run starts on time, and well above the time to list the root and scan one sample, or the first batch has little time to delete; 0 = no budget, one sample")

	args := os.Args[1:] // skip the command name
	if err := flags.Parse(args); err != nil {
		return opts, nil, nil, nil, nil, fmt.Errorf("could not parse flags: %w", err)
	}

	args = flags.Args()
	if len(args) != 1 {
		return opts, nil, nil, nil, nil, cleaner.ErrUsage
	}
	opts.Path = args[0]

	ffConfig, err := envconfig.ParseAs[featureflags.Config]()
	if err != nil {
		return opts, nil, nil, nil, nil, fmt.Errorf("could not parse feature flags config: %w", err)
	}

	ffc, err := featureflags.NewClient(ffConfig.DeploymentEnvironment, serviceName, serviceVersion)
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
		if m.Get("maxCacheBytes").IsNumber() {
			opts.MaxCacheBytes = uint64(m.Get("maxCacheBytes").Float64Value())
		}
		if m.Get("verify").IsBool() {
			opts.Verify = m.Get("verify").BoolValue()
		}
		if m.Get("runBudgetSeconds").IsNumber() {
			opts.RunBudget = time.Duration(m.Get("runBudgetSeconds").IntValue()) * time.Second
		}
		if mode, ok := indexModeSetting(m); !ok {
			opts.IndexMode, badIndexMode = cleaner.IndexOff, mode
		} else if mode != "" {
			opts.IndexMode = mode
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
	if badIndexMode != "" {
		l.Warn(ctx, "unknown indexMode in the feature flag; sampling without the index", zap.String("index_mode", badIndexMode))
	}

	// Percent mode: resolve the target from the filesystem size. Skipped when a byte budget is set
	// (the budget replaces it — and on EFS the reported size is meaningless anyway).
	if opts.TargetBytesToDelete == 0 && opts.MaxCacheBytes == 0 && opts.TargetDiskUsagePercent > 0 {
		diskInfo, derr := cleaner.GetDiskInfo(opts.Path)
		if derr != nil {
			tel.Shutdown(ctx)

			return opts, nil, nil, nil, nil, fmt.Errorf("could not get disk info: %w", derr)
		}
		if diskInfo.Elastic() {
			// Fail the job rather than no-op forever: on EFS "used percent" is ~0
			// however large the cache grows, and every GB is billed monthly.
			tel.Shutdown(ctx)

			return opts, nil, nil, nil, nil, fmt.Errorf(
				"statfs reports %d bytes total for %s — an elastic filesystem (Amazon EFS reports 8 EiB), so disk-usage-target-percent can never trigger; set --max-cache-bytes instead",
				diskInfo.Total, opts.Path)
		}
		targetDiskUsage := uint64(opts.TargetDiskUsagePercent / 100 * float64(diskInfo.Total))
		if diskInfo.Used > targetDiskUsage {
			opts.TargetBytesToDelete = diskInfo.Used - targetDiskUsage
		}
	}

	l.Info(ctx, "configured",
		zap.Bool("dry_run", opts.DryRun),
		zap.Uint64("target_bytes_to_delete", opts.TargetBytesToDelete),
		zap.Float64("target_disk_usage_percent", opts.TargetDiskUsagePercent),
		zap.Uint64("max_cache_bytes", opts.MaxCacheBytes),
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
		zap.Duration("run_budget", opts.RunBudget),
		zap.String("index_mode", opts.IndexMode),
	)

	return opts, l, tel, metrics, meterProvider, nil
}

// indexModeSetting reads indexMode from the cleaner's flag ("" when absent). ok
// is false for an unknown mode: a typo must leave cleaning on sampling, not fail
// validation and stop every run.
func indexModeSetting(m ldvalue.ValueMap) (mode string, ok bool) {
	if v := m.Get("indexMode"); v.IsString() {
		mode = v.StringValue()
	}
	switch mode {
	case "", cleaner.IndexOff, cleaner.IndexOn:
		return mode, true
	default:
		return mode, false
	}
}
