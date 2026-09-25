package metrics

import (
	"context"
	"fmt"
	iofs "io/fs"
	"net"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
)

type metricsHandler struct {
	inner  nfs.Handler
	config cfg.Config
}

var (
	_ nfs.Handler        = (*metricsHandler)(nil)
	_ nfs.CachingHandler = (*metricsHandler)(nil)
)

func WrapWithMetrics(handler nfs.Handler, config cfg.Config) nfs.Handler {
	return &metricsHandler{inner: handler, config: config}
}

func (m *metricsHandler) Mount(ctx context.Context, conn net.Conn, request nfs.MountRequest) (s nfs.MountStatus, fs billy.Filesystem, auth []nfs.AuthFlavor) {
	finish := recordCall(ctx, "NFS.Mount")

	defer func() {
		var err error
		if s != nfs.MountStatusOk {
			err = fmt.Errorf("mount status = %d", s)
		}
		finish(err)
	}()

	s, fs, auth = m.inner.Mount(ctx, conn, request)
	if fs != nil {
		fs = wrapFS(ctx, fs, m.config)
	}

	return
}

func (m *metricsHandler) Change(ctx context.Context, filesystem billy.Filesystem) billy.Change {
	finish := recordCall(ctx, "NFS.Change")
	defer finish(nil)

	change := m.inner.Change(ctx, filesystem)

	return newChange(ctx, change)
}

func (m *metricsHandler) FSStat(ctx context.Context, filesystem billy.Filesystem, stat *nfs.FSStat) (err error) {
	finish := recordCall(ctx, "NFS.FSStat")
	defer func() { finish(err) }()

	return m.inner.FSStat(ctx, filesystem, stat)
}

func (m *metricsHandler) ToHandle(ctx context.Context, fs billy.Filesystem, path []string) (fh []byte) {
	if !m.config.RecordHandleCalls {
		return m.inner.ToHandle(ctx, fs, path)
	}

	finish := recordCall(ctx, "NFS.ToHandle")
	defer finish(nil)

	return m.inner.ToHandle(ctx, fs, path)
}

func (m *metricsHandler) FromHandle(ctx context.Context, fh []byte) (fs billy.Filesystem, paths []string, err error) {
	if !m.config.RecordHandleCalls {
		return m.inner.FromHandle(ctx, fh)
	}

	finish := recordCall(ctx, "NFS.FromHandle")
	defer func() { finish(err) }()

	fs, paths, err = m.inner.FromHandle(ctx, fh)
	if fs != nil {
		fs = wrapFS(ctx, fs, m.config)
	}

	return
}

func (m *metricsHandler) InvalidateHandle(ctx context.Context, filesystem billy.Filesystem, bytes []byte) (err error) {
	if !m.config.RecordHandleCalls {
		return m.inner.InvalidateHandle(ctx, filesystem, bytes)
	}

	finish := recordCall(ctx, "NFS.InvalidateHandle")
	defer func() { finish(err) }()

	return m.inner.InvalidateHandle(ctx, filesystem, bytes)
}

func (m *metricsHandler) HandleLimit() int {
	return m.inner.HandleLimit()
}

func (m *metricsHandler) VerifierFor(path string, contents []iofs.FileInfo) uint64 {
	handler, ok := m.inner.(nfs.CachingHandler)
	if !ok {
		return 0
	}

	return handler.VerifierFor(path, contents)
}

func (m *metricsHandler) DataForVerifier(path string, verifier uint64) []iofs.FileInfo {
	handler, ok := m.inner.(nfs.CachingHandler)
	if !ok {
		return nil
	}

	return handler.DataForVerifier(path, verifier)
}
