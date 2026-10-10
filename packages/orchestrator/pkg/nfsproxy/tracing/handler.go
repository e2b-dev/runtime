package tracing

import (
	"context"
	"fmt"
	iofs "io/fs"
	"net"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
)

type tracingHandler struct {
	inner  nfs.Handler
	config cfg.Config
}

var (
	_ nfs.Handler        = (*tracingHandler)(nil)
	_ nfs.CachingHandler = (*tracingHandler)(nil)
)

func WrapWithTracing(handler nfs.Handler, config cfg.Config) nfs.Handler {
	return &tracingHandler{inner: handler, config: config}
}

func (e *tracingHandler) Mount(ctx context.Context, conn net.Conn, request nfs.MountRequest) (s nfs.MountStatus, fs billy.Filesystem, auth []nfs.AuthFlavor) {
	ctx, finish := startSpan(ctx, "NFS.Mount",
		attribute.String("net.conn.remote_addr", conn.RemoteAddr().String()),
		attribute.String("nfs.mount.dirpath", string(request.Dirpath)))

	defer func() {
		var err error
		if s != nfs.MountStatusOk {
			err = fmt.Errorf("mount status = %d", s)
		}
		finish(err)
	}()

	s, fs, auth = e.inner.Mount(ctx, conn, request)
	if fs != nil {
		fs = wrapFS(ctx, fs, e.config)
	}

	return
}

func (e *tracingHandler) Change(ctx context.Context, filesystem billy.Filesystem) billy.Change {
	ctx, finish := startSpan(ctx, "NFS.Change")
	defer finish(nil)

	change := e.inner.Change(ctx, filesystem)

	return newChange(ctx, change)
}

func (e *tracingHandler) FSStat(ctx context.Context, filesystem billy.Filesystem, stat *nfs.FSStat) (err error) {
	ctx, finish := startSpan(ctx, "NFS.FSStat")
	defer func() { finish(err) }()

	return e.inner.FSStat(ctx, filesystem, stat)
}

func (e *tracingHandler) ToHandle(ctx context.Context, fs billy.Filesystem, path []string) (fh []byte) {
	if !e.config.RecordHandleCalls {
		return e.inner.ToHandle(ctx, fs, path)
	}

	_, finish := startSpan(ctx, "NFS.ToHandle", attribute.StringSlice("nfs.path", path))
	defer finish(nil)

	return e.inner.ToHandle(ctx, fs, path)
}

func (e *tracingHandler) FromHandle(ctx context.Context, fh []byte) (fs billy.Filesystem, paths []string, err error) {
	if !e.config.RecordHandleCalls {
		return e.inner.FromHandle(ctx, fh)
	}

	ctx, finish := startSpan(ctx, "NFS.FromHandle")
	defer func() { finish(err) }()

	fs, paths, err = e.inner.FromHandle(ctx, fh)
	if fs != nil {
		fs = wrapFS(ctx, fs, e.config)
	}

	return
}

func (e *tracingHandler) InvalidateHandle(ctx context.Context, filesystem billy.Filesystem, bytes []byte) (err error) {
	if !e.config.RecordHandleCalls {
		return e.inner.InvalidateHandle(ctx, filesystem, bytes)
	}

	ctx, finish := startSpan(ctx, "NFS.InvalidateHandle")
	defer func() { finish(err) }()

	return e.inner.InvalidateHandle(ctx, filesystem, bytes)
}

func (e *tracingHandler) HandleLimit() int {
	return e.inner.HandleLimit()
}

func (e *tracingHandler) VerifierFor(path string, contents []iofs.FileInfo) uint64 {
	handler, ok := e.inner.(nfs.CachingHandler)
	if !ok {
		return 0
	}

	return handler.VerifierFor(path, contents)
}

func (e *tracingHandler) DataForVerifier(path string, verifier uint64) []iofs.FileInfo {
	handler, ok := e.inner.(nfs.CachingHandler)
	if !ok {
		return nil
	}

	return handler.DataForVerifier(path, verifier)
}
