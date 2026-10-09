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
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/handlecache"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/logged"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/recovery"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/tracing"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
)

var setLogLevelOnce sync.Once

type Proxy struct {
	config cfg.Config
	server *nfs.Server
}

func NewProxy(ctx context.Context, builder *chrooted.Builder, sandboxes *sandbox.Map, config cfg.Config) (*Proxy, error) {
	handleCacheLimit := cmp.Or(config.HandleCacheLimit, cfg.DefaultHandleCacheLimit)
	if handleCacheLimit < 2 {
		// go-nfs needs room for a directory and an entry in it
		return nil, fmt.Errorf("nfs proxy handle cache limit must be at least 2, got %d", handleCacheLimit)
	}

	setLogLevelOnce.Do(func() {
		nfs.Log.SetLevel(config.NFSLogLevel)
	})

	chrootHandler, err := chroot.NewNFSHandler(builder, sandboxes)
	if err != nil {
		return nil, fmt.Errorf("failed to create chroot NFS handler: %w", err)
	}

	handles := handlecache.Wrap(chrootHandler, handleCacheLimit)
	chrootHandler.OnMount(handles.Register)
	chrootHandler.OnRelease(handles.Release)

	// wrap the handler in middleware
	var handler nfs.Handler = handles

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

	// go-nfs looks for the verifier cache on the outermost handler only
	handler, err = dirverifier.Wrap(handler, cmp.Or(config.DirVerifierLimit, cfg.DefaultDirVerifierLimit))
	if err != nil {
		return nil, fmt.Errorf("failed to create nfs directory verifier cache: %w", err)
	}

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
