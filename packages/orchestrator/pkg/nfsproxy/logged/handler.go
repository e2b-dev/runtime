package logged

import (
	"context"
	"fmt"
	iofs "io/fs"
	"net"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
)

type loggedHandler struct {
	ctx    context.Context //nolint:containedctx // can't change the API, still need it
	inner  nfs.Handler
	config cfg.Config
}

var (
	_ nfs.Handler        = (*loggedHandler)(nil)
	_ nfs.CachingHandler = (*loggedHandler)(nil)
)

func WrapWithLogging(ctx context.Context, handler nfs.Handler, config cfg.Config) nfs.Handler {
	return loggedHandler{ctx: ctx, inner: handler, config: config}
}

func (e loggedHandler) Mount(ctx context.Context, conn net.Conn, request nfs.MountRequest) (s nfs.MountStatus, fs billy.Filesystem, auth []nfs.AuthFlavor) {
	finish := logStart(ctx, "Handler.Mount",
		fmt.Sprintf("net.Conn{LocalAddr=%q, RemoteAddr=%q}", conn.LocalAddr(), conn.RemoteAddr()),
		fmt.Sprintf("nfs.MountRequest{Dirpath=%q}", string(request.Dirpath)))
	defer func() {
		var err error
		if s != nfs.MountStatusOk {
			err = fmt.Errorf("mount status = %d", s)
		}
		finish(ctx, err)
	}()

	s, fs, auth = e.inner.Mount(ctx, conn, request)
	fs = wrapFS(ctx, fs, e.config)

	return
}

func (e loggedHandler) Change(ctx context.Context, filesystem billy.Filesystem) billy.Change {
	finish := logStart(ctx, "Handler.Change")
	defer finish(ctx, nil)

	change := e.inner.Change(ctx, filesystem)

	return newChange(ctx, change)
}

func (e loggedHandler) FSStat(ctx context.Context, filesystem billy.Filesystem, stat *nfs.FSStat) (err error) {
	finish := logStart(ctx, "Handler.FSStat")
	defer func() { finish(ctx, err) }()

	return e.inner.FSStat(ctx, filesystem, stat)
}

func (e loggedHandler) ToHandle(ctx context.Context, fs billy.Filesystem, path []string) (fh []byte) {
	if !e.config.RecordHandleCalls {
		return e.inner.ToHandle(ctx, fs, path)
	}

	finish := logStart(ctx, "Handler.ToHandle", path)
	defer func() { finish(ctx, nil, fh) }()

	return e.inner.ToHandle(ctx, fs, path)
}

func (e loggedHandler) FromHandle(ctx context.Context, fh []byte) (fs billy.Filesystem, paths []string, err error) {
	if !e.config.RecordHandleCalls {
		return e.inner.FromHandle(ctx, fh)
	}

	finish := logStart(ctx, "Handler.FromHandle", fh)
	defer func() { finish(ctx, err, paths) }()

	return e.inner.FromHandle(ctx, fh)
}

func (e loggedHandler) InvalidateHandle(ctx context.Context, filesystem billy.Filesystem, bytes []byte) (err error) {
	if !e.config.RecordHandleCalls {
		return e.inner.InvalidateHandle(ctx, filesystem, bytes)
	}

	finish := logStart(ctx, "Handler.InvalidateHandle")
	defer func() { finish(ctx, err) }()

	return e.inner.InvalidateHandle(ctx, filesystem, bytes)
}

func (e loggedHandler) HandleLimit() int {
	finish := logStart(e.ctx, "Handler.HandleLimit")
	defer finish(e.ctx, nil)

	return e.inner.HandleLimit()
}

func (e loggedHandler) VerifierFor(path string, contents []iofs.FileInfo) uint64 {
	handler, ok := e.inner.(nfs.CachingHandler)
	if !ok {
		return 0
	}

	return handler.VerifierFor(path, contents)
}

func (e loggedHandler) DataForVerifier(path string, verifier uint64) []iofs.FileInfo {
	handler, ok := e.inner.(nfs.CachingHandler)
	if !ok {
		return nil
	}

	return handler.DataForVerifier(path, verifier)
}
