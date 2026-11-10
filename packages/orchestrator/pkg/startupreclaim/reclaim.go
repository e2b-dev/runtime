//go:build linux

package startupreclaim

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	resourceFirecracker = "firecracker"
	resourceNBD         = "nbd"
	resourceNetwork     = "network"
	resourceCgroup      = "cgroup"
	resourceFile        = "file"
	resourceCacheDir    = "cache_dir"

	procDir = "/proc"
)

var (
	meter = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/startupreclaim")

	reclaimedCounter = utils.Must(meter.Int64Counter("orchestrator.startup_reclaim.reclaimed",
		metric.WithDescription("Startup reclaim resources successfully reclaimed."),
		metric.WithUnit("{resource}"),
	))
	failedCounter = utils.Must(meter.Int64Counter("orchestrator.startup_reclaim.failed",
		metric.WithDescription("Startup reclaim resource cleanup failures."),
		metric.WithUnit("{resource}"),
	))
)

type Config struct {
	NetworkConfig network.Config
	EgressProxy   network.EgressProxy
	CgroupManager cgroup.Manager
	StorageConfig storage.Config
	TempDir       string
	ProcDir       string
	NetnsDir      string
	CgroupRoot    string

	// CacheDirs are emptied; only the previous process wrote to them.
	CacheDirs []string
	// KeepPaths survive the sweep: a cache dir that is or holds one is skipped.
	KeepPaths []string
}

type Summary struct {
	Reclaimed map[string]int
	Failed    map[string]int
}

func (s Summary) totalReclaimed() int {
	return total(s.Reclaimed)
}

func (s Summary) totalFailed() int {
	return total(s.Failed)
}

// HasFailures reports whether any resource failed to reclaim.
func (s Summary) HasFailures() bool {
	return s.totalFailed() > 0
}

// HasRuntimeFailures reports whether any reclaimer other than the cache-dir
// sweep failed. Those leave processes, devices, slots or cgroups behind; a
// cache dir the sweep refuses or cannot empty only keeps its files.
func (s Summary) HasRuntimeFailures() bool {
	for resource, failed := range s.Failed {
		if resource != resourceCacheDir && failed > 0 {
			return true
		}
	}

	return false
}

func total(values map[string]int) int {
	total := 0
	for _, value := range values {
		total += value
	}

	return total
}

// reclaimer reclaims leaked resources of a single kind. reclaim is best-effort
// and never fatal, returning the count reclaimed and any failures.
type reclaimer struct {
	resource string
	reclaim  func(ctx context.Context) (int, []error)
}

func Run(ctx context.Context, config Config) Summary {
	config = config.withDefaults()

	// No egress proxy wired: slots are still torn down, but the egress firewall
	// cleanup (OnSlotDelete) is skipped and those iptables rules may leak. Log it
	// instead of substituting silently; callers with no egress firewall can pass
	// network.NewNoopEgressProxy() to opt in quietly.
	if config.EgressProxy == nil {
		logger.L().Error(ctx, "startup reclaim: no egress proxy provided; egress firewall cleanup will be skipped for reclaimed slots")
		config.EgressProxy = network.NewNoopEgressProxy()
	}

	summary := Summary{Reclaimed: map[string]int{}, Failed: map[string]int{}}

	// Order matters: firecracker runs first so the VMMs are killed before the
	// network reclaim tears down the slots they used.
	reclaimers := []reclaimer{
		{resourceFirecracker, func(ctx context.Context) (int, []error) {
			return reclaimFirecrackers(ctx, config.ProcDir)
		}},
		{resourceNBD, nbd.ReclaimLeaked},
		{resourceNetwork, func(ctx context.Context) (int, []error) {
			return network.ReclaimLeakedSlots(ctx, config.NetnsDir, config.NetworkConfig, config.EgressProxy)
		}},
		{resourceCgroup, func(ctx context.Context) (int, []error) {
			return cgroup.ReclaimLeaked(ctx, config.CgroupManager, config.CgroupRoot)
		}},
		{resourceFile, func(context.Context) (int, []error) {
			return storage.ReclaimSandboxFiles(config.TempDir, config.StorageConfig.SandboxCacheDir)
		}},
		{resourceCacheDir, func(ctx context.Context) (int, []error) {
			reset, stale, failures := resetCacheDirs(config.CacheDirs, config.KeepPaths)
			if len(stale) > 0 {
				go removeStale(context.WithoutCancel(ctx), stale)
			}

			return reset, failures
		}},
	}

	for _, r := range reclaimers {
		reclaimed, failures := r.reclaim(ctx)
		record(ctx, &summary, r.resource, reclaimed, failures)
	}

	fields := []zap.Field{
		zap.Any("reclaimed", summary.Reclaimed),
		zap.Any("failed", summary.Failed),
		zap.Int("total_reclaimed", summary.totalReclaimed()),
		zap.Int("total_failed", summary.totalFailed()),
	}
	if summary.totalReclaimed() > 0 || summary.totalFailed() > 0 {
		logger.L().Warn(ctx, "startup resource reclaim completed with leftover resources", fields...)
	} else {
		logger.L().Info(ctx, "startup resource reclaim completed cleanly", fields...)
	}

	return summary
}

func (c Config) withDefaults() Config {
	if c.TempDir == "" {
		c.TempDir = os.TempDir()
	}
	if c.ProcDir == "" {
		c.ProcDir = procDir
	}
	if c.NetnsDir == "" {
		c.NetnsDir = network.NetNamespacesDir
	}
	if c.CgroupRoot == "" {
		c.CgroupRoot = cgroup.RootCgroupPath
	}

	return c
}

func record(ctx context.Context, summary *Summary, resource string, reclaimed int, failures []error) {
	if reclaimed > 0 {
		summary.Reclaimed[resource] += reclaimed
		reclaimedCounter.Add(ctx, int64(reclaimed), metric.WithAttributes(attribute.String("resource_type", resource)))
	}

	for _, err := range failures {
		summary.Failed[resource]++
		failedCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("resource_type", resource)))
		logger.L().Warn(ctx, "startup resource reclaim failed", zap.String("resource_type", resource), zap.Error(err))
	}
}
