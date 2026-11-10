package pool

import (
	"context"
	"errors"
	"sync"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

var errOwnerClosed = errors.New("proxy connection owner is closed")

// Owner is one admission lifetime: the handlers, dials and sockets of the
// requests resolved to it. Stop refuses further admission and interrupts
// what is running; Close also waits for it to finish.
type Owner struct {
	pool   *ProxyPool
	ctx    context.Context //nolint:containedctx // Lifetime cancellation is independent of any one request.
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	client *ProxyClient
	// active counts admitted handlers and dials. done closes when the last
	// of them releases after the stop, or at the stop if none is admitted.
	active int
	done   chan struct{}
	stop   sync.Once
}

func (o *Owner) clientFor(ctx context.Context, d *Destination) *ProxyClient {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.client == nil {
		o.client = o.pool.newClient(ctx, d, o)
		// Built after Stop, it serves only refusals: Stop has run and will
		// not retire it, so it is not counted either.
		if !o.closed {
			o.pool.clients.Add(1)
		}
	}

	return o.client
}

// acquire admits work and returns a context the owner's end cancels.
func (o *Owner) acquire(ctx context.Context) (context.Context, func(), error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil, nil, errOwnerClosed
	}
	o.active++
	ctx, cancel := context.WithCancel(ctx)
	unlink := context.AfterFunc(o.ctx, cancel)

	return ctx, func() {
		unlink()
		cancel()
		o.mu.Lock()
		defer o.mu.Unlock()
		o.active--
		o.settle()
	}, nil
}

// settle closes done once the owner is stopped and nothing is admitted: the
// last release may get there while Stop is still resetting, or Stop itself.
// Runs under mu.
func (o *Owner) settle() {
	if !o.closed || o.active != 0 {
		return
	}
	select {
	case <-o.done:
	default:
		close(o.done)
	}
}

// Stop refuses further admission, resets the admitted sockets, so the guest
// sees a reset rather than an orderly close, then cancels the admitted work.
// A concurrent caller returns once the first has finished.
func (o *Owner) Stop() {
	o.stop.Do(func() {
		o.mu.Lock()
		o.closed = true
		client := o.client
		o.mu.Unlock()
		if client != nil {
			client.closeIdleConnections()
			// Reset closes each socket even when linger or close reports an
			// error, so a failed reset cannot keep a release waiting.
			if err := client.resetAllConnections(); err != nil {
				logger.L().Warn(o.ctx, "failed to reset retired proxy connections", zap.Error(err))
			}
			o.pool.clients.Add(-1)
		}
		o.cancel()
		o.mu.Lock()
		defer o.mu.Unlock()
		o.settle()
	})
}

// refused reports whether a socket registered a moment ago must be closed:
// Stop marks closed before it resets what is registered, so a dial that
// registered after that pass reads closed here.
func (o *Owner) refused() bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.closed
}

// Close stops the owner and waits for the admitted work to finish, or for
// ctx to end, in which case the wait may be retried.
func (o *Owner) Close(ctx context.Context) error {
	o.Stop()
	select {
	case <-o.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Acquire admits a handler for its whole run, limiter release included.
// Dials acquire separately: net/http detaches them from their request.
func (p *ProxyClient) Acquire(ctx context.Context) (context.Context, func(), error) {
	return p.owner.acquire(ctx)
}

// Context ends when the owner stops, so a handler can interrupt frontend I/O.
func (p *ProxyClient) Context() context.Context {
	return p.owner.ctx
}
