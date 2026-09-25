package recovery

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"net"

	"github.com/go-git/go-billy/v5"
	"github.com/willscott/go-nfs"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type Handler struct {
	inner nfs.Handler
	ctx   context.Context //nolint:containedctx // can't change the API, still need it
}

var (
	_ nfs.Handler        = (*Handler)(nil)
	_ nfs.CachingHandler = (*Handler)(nil)
)

func WrapWithRecovery(ctx context.Context, h nfs.Handler) *Handler {
	return &Handler{inner: h, ctx: ctx}
}

func (h Handler) Mount(ctx context.Context, conn net.Conn, request nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	defer tryRecovery(ctx, "Mount")
	s, fs, auth := h.inner.Mount(ctx, conn, request)
	fs = wrapFS(ctx, fs)

	return s, fs, auth
}

func (h Handler) Change(ctx context.Context, filesystem billy.Filesystem) billy.Change {
	defer tryRecovery(ctx, "Change")
	c := h.inner.Change(ctx, filesystem)

	return wrapChange(ctx, c)
}

func (h Handler) FSStat(ctx context.Context, filesystem billy.Filesystem, stat *nfs.FSStat) (e error) {
	defer deferErrRecovery(ctx, "Handler.FSStat", &e)

	return h.inner.FSStat(ctx, filesystem, stat)
}

func (h Handler) ToHandle(ctx context.Context, fs billy.Filesystem, path []string) []byte {
	defer tryRecovery(ctx, "ToHandle")

	return h.inner.ToHandle(ctx, fs, path)
}

func (h Handler) FromHandle(ctx context.Context, fh []byte) (fs billy.Filesystem, path []string, e error) {
	defer deferErrRecovery(ctx, "Handler.FromHandle", &e)

	return h.inner.FromHandle(ctx, fh)
}

func (h Handler) InvalidateHandle(ctx context.Context, filesystem billy.Filesystem, bytes []byte) (e error) {
	defer deferErrRecovery(ctx, "Handler.InvalidateHandle", &e)

	return h.inner.InvalidateHandle(ctx, filesystem, bytes)
}

func (h Handler) HandleLimit() int {
	defer tryRecovery(h.ctx, "HandleLimit")

	return h.inner.HandleLimit()
}

func (h Handler) VerifierFor(path string, contents []iofs.FileInfo) (verifier uint64) {
	defer tryRecovery(h.ctx, "VerifierFor")

	handler, ok := h.inner.(nfs.CachingHandler)
	if !ok {
		return 0
	}

	return handler.VerifierFor(path, contents)
}

func (h Handler) DataForVerifier(path string, verifier uint64) (contents []iofs.FileInfo) {
	defer tryRecovery(h.ctx, "DataForVerifier")

	handler, ok := h.inner.(nfs.CachingHandler)
	if !ok {
		return nil
	}

	return handler.DataForVerifier(path, verifier)
}

// tryRecovery must be called via `defer` directly so that recover() runs in
// the deferred function frame. Nesting it inside another helper would cause
// recover() to return nil and the panic would propagate.
func tryRecovery(ctx context.Context, name string) {
	if r := recover(); r != nil { //nolint:revive // always called via defer
		logger.L().Error(ctx, fmt.Sprintf("panic in %q nfs handler", name),
			zap.Any("panic", r),
			zap.Stack("stack"),
		)
	}
}

var ErrPanic = errors.New("panic")

func deferErrRecovery(ctx context.Context, name string, perr *error) {
	if r := recover(); r != nil { //nolint:revive // always called via defer
		logger.L().Error(ctx, fmt.Sprintf("panic in %q nfs handler", name),
			zap.Any("panic", r),
			zap.Stack("stack"),
		)
		if perr != nil {
			*perr = ErrPanic
		}
	}
}
