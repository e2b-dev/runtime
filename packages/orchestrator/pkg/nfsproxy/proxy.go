//go:build linux

package nfsproxy

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/willscott/go-nfs"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/chroot"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/logged"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/mountcache"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/recovery"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/tracing"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
)

const cacheLimit = 1024

var setLogLevelOnce sync.Once

type Proxy struct {
	config cfg.Config
	server *nfs.Server
}

type cacheLifecycleSubscriber struct {
	handler *mountcache.Handler
}

func (s cacheLifecycleSubscriber) OnInsert(_ context.Context, _ *sandbox.Sandbox) {}

func (s cacheLifecycleSubscriber) OnNetworkRelease(_ context.Context, sbx *sandbox.Sandbox) {
	s.handler.RemoveOwner(mountcache.Owner{
		SandboxID:   sbx.Runtime.SandboxID,
		LifecycleID: sbx.LifecycleID,
	})
}

func NewProxy(ctx context.Context, builder *chrooted.Builder, sandboxes *sandbox.Map, config cfg.Config) (*Proxy, error) {
	setLogLevelOnce.Do(func() {
		nfs.Log.SetLevel(config.NFSLogLevel)
	})

	// actual nfs handler
	var (
		handler nfs.Handler
		err     error
	)
	handler, err = chroot.NewNFSHandler(builder, sandboxes)
	if err != nil {
		return nil, fmt.Errorf("failed to create chroot NFS handler: %w", err)
	}

	// wrap the handler in middleware
	cachingHandler := mountcache.NewHandler(handler, cacheLimit)
	sandboxes.Subscribe(cacheLifecycleSubscriber{handler: cachingHandler})
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

	s := &nfs.Server{
		Handler:      handler,
		Context:      ctx,
		OnConnect:    onConnect,
		OnDisconnect: onDisconnect,
	}

	return &Proxy{
		config: config,
		server: s,
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
