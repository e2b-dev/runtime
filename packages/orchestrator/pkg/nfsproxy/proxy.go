//go:build linux

package nfsproxy

import (
	"cmp"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/willscott/go-nfs"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/chroot"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/dirverifier"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/logged"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/mountcache"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/recovery"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/tracing"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
)

var setLogLevelOnce sync.Once

type Proxy struct {
	config     cfg.Config
	server     *nfs.Server
	cache      *mountcache.Handler
	chroots    *chroot.NFSHandler
	gate       *mountcache.LifecycleGate
	retirement *mountLifecycleSubscriber
}

func NewProxy(ctx context.Context, builder *chrooted.Builder, sandboxes *sandbox.Map, config cfg.Config) (*Proxy, error) {
	return newProxy(ctx, builder, sandboxes, config, cmp.Or(config.HandleCacheLimit, cfg.DefaultHandleCacheLimit))
}

func newProxy(ctx context.Context, builder chroot.Builder, sandboxes *sandbox.Map, config cfg.Config, handleCacheLimit int) (*Proxy, error) {
	if handleCacheLimit < 2 {
		return nil, fmt.Errorf("nfs proxy handle cache limit must be at least 2, got %d", handleCacheLimit)
	}

	setLogLevelOnce.Do(func() {
		nfs.Log.SetLevel(config.NFSLogLevel)
	})

	gate := mountcache.NewLifecycleGate()

	var (
		handler nfs.Handler
		err     error
	)
	chrootHandler, err := chroot.NewNFSHandler(builder, sandboxes, gate)
	if err != nil {
		return nil, fmt.Errorf("failed to create chroot NFS handler: %w", err)
	}
	handler = chrootHandler

	// wrap the handler in middleware
	cachingHandler := mountcache.NewHandler(handler, handleCacheLimit)
	handler = cachingHandler

	if config.Tracing {
		handler = tracing.WrapWithTracing(handler, config)
	}

	if config.Metrics {
		handler = metrics.WrapWithMetrics(handler, config)
	}

	if config.Logging {
		handler = logged.WrapWithLogging(ctx, handler, config)
	}

	handler = recovery.WrapWithRecovery(ctx, handler)
	handler, err = dirverifier.Wrap(handler, cmp.Or(config.DirVerifierLimit, cfg.DefaultDirVerifierLimit))
	if err != nil {
		return nil, fmt.Errorf("failed to create nfs directory verifier cache: %w", err)
	}

	retirement, err := newMountLifecycleSubscriber(gate, chrootHandler, cachingHandler)
	if err != nil {
		return nil, err
	}
	sandboxes.Subscribe(retirement)

	s := &nfs.Server{
		Handler:      handler,
		Context:      ctx,
		OnConnect:    onConnect,
		OnDisconnect: onDisconnect,
	}

	return &Proxy{
		config:     config,
		server:     s,
		cache:      cachingHandler,
		chroots:    chrootHandler,
		gate:       gate,
		retirement: retirement,
	}, nil
}

func (p *Proxy) Serve(lis net.Listener) error {
	if err := p.server.Serve(lis); err != nil {
		if strings.Contains(err.Error(), "use of closed network connection") {
			return nil
		}

		return err
	}

	return nil
}
