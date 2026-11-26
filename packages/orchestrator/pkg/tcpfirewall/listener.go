//go:build linux

package tcpfirewall

import (
	"context"
	"errors"
	"net"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	acceptRetryDelay = 100 * time.Millisecond
)

// resilientListener wraps a net.Listener to handle transient Accept errors.
type resilientListener struct {
	net.Listener

	ctx      context.Context //nolint:containedctx // needed for cancellation in Accept retry loop
	logger   logger.Logger
	onAccept func(net.Conn) bool
}

func (l *resilientListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err == nil {
			if l.onAccept != nil && !l.onAccept(conn) {
				_ = conn.Close()
				continue
			}
			return conn, nil
		}

		if !isTransientAcceptError(err) {
			return nil, err
		}

		l.logger.Error(l.ctx, "tcpfirewall listener accept error, retrying", zap.Error(err))

		select {
		case <-l.ctx.Done():
			return nil, l.ctx.Err()
		case <-time.After(acceptRetryDelay):
		}
	}
}

func isTransientAcceptError(err error) bool {
	if err == nil {
		return false
	}

	return errors.Is(err, syscall.EMFILE) || // too many open files (per-process)
		errors.Is(err, syscall.ENFILE) || // too many open files (system-wide)
		errors.Is(err, syscall.EAGAIN) || // resource temporarily unavailable
		errors.Is(err, syscall.ECONNABORTED) // client closed before accept
}
