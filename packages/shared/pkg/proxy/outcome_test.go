package proxy

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
)

// outcomeRecorder collects the outcomes the proxy reports, concurrency-safe.
type outcomeRecorder struct {
	mu       sync.Mutex
	outcomes []pool.RequestOutcome
}

func (o *outcomeRecorder) add(_ context.Context, outcome pool.RequestOutcome) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.outcomes = append(o.outcomes, outcome)
}

func (o *outcomeRecorder) last() (pool.RequestOutcome, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.outcomes) == 0 {
		return "", false
	}

	return o.outcomes[len(o.outcomes)-1], true
}

// startOutcomeProxy stands up a proxy whose destination resolver returns
// resolve(), recording every request outcome into rec. Returns the proxy URL.
func startOutcomeProxy(t *testing.T, rec *outcomeRecorder, resolve func() (*pool.Destination, error)) string {
	t.Helper()

	var lisCfg net.ListenConfig
	proxyListener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	proxyAddr := proxyListener.Addr().(*net.TCPAddr)
	proxyPort := uint16(proxyAddr.Port) //nolint:gosec // test-only port fits

	proxy := New(proxyPort, 1, time.Second, func(_ *http.Request) (*pool.Destination, error) {
		return resolve()
	}, nil, false, rec.add)

	go func() {
		serveErr := proxy.Serve(proxyListener)
		assert.ErrorIs(t, serveErr, http.ErrServerClosed)
	}()
	t.Cleanup(func() { assert.NoError(t, proxy.Close()) })

	return fmt.Sprintf("http://%s", proxyAddr.String())
}

func TestProxyRecordsOutcome_OK(t *testing.T) {
	t.Parallel()

	var lisCfg net.ListenConfig
	backendListener, err := lisCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	backendPort := uint64(backendListener.Addr().(*net.TCPAddr).Port) //nolint:gosec // test-only
	backendURL, err := url.Parse(fmt.Sprintf("http://%s", backendListener.Addr().String()))
	require.NoError(t, err)

	backend := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})}
	go func() { assert.ErrorIs(t, backend.Serve(backendListener), http.ErrServerClosed) }()
	t.Cleanup(func() { assert.NoError(t, backend.Close()) })

	rec := &outcomeRecorder{}
	proxyURL := startOutcomeProxy(t, rec, func() (*pool.Destination, error) {
		return &pool.Destination{
			Url:           backendURL,
			SandboxId:     "sbx-ok",
			SandboxPort:   backendPort,
			ConnectionKey: "ok",
			RequestLogger: logger.L(),
		}, nil
	})

	resp, err := http.Get(proxyURL) //nolint:noctx // test
	require.NoError(t, err)
	_ = resp.Body.Close()

	require.Eventually(t, func() bool {
		o, ok := rec.last()

		return ok && o == pool.OutcomeOK
	}, 2*time.Second, 10*time.Millisecond, "expected an ok outcome")
}

func TestProxyRecordsOutcome_UpstreamUnreachable(t *testing.T) {
	t.Parallel()

	// Port 1 is reliably closed: the dial fails, so the request is
	// upstream-unreachable, not a routing failure.
	deadURL, err := url.Parse("http://127.0.0.1:1")
	require.NoError(t, err)

	rec := &outcomeRecorder{}
	proxyURL := startOutcomeProxy(t, rec, func() (*pool.Destination, error) {
		return &pool.Destination{
			Url:           deadURL,
			SandboxId:     "sbx-dead",
			SandboxPort:   1,
			ConnectionKey: "dead",
			RequestLogger: logger.L(),
		}, nil
	})

	resp, err := http.Get(proxyURL) //nolint:noctx // test
	require.NoError(t, err)
	_ = resp.Body.Close()

	require.Eventually(t, func() bool {
		o, ok := rec.last()

		return ok && o == pool.OutcomeUpstreamUnreachable
	}, 2*time.Second, 10*time.Millisecond, "expected an upstream_unreachable outcome")
}

func TestProxyRecordsOutcome_SandboxNotFound(t *testing.T) {
	t.Parallel()

	rec := &outcomeRecorder{}
	proxyURL := startOutcomeProxy(t, rec, func() (*pool.Destination, error) {
		return nil, NewErrSandboxNotFound("sbx-missing")
	})

	resp, err := http.Get(proxyURL) //nolint:noctx // test
	require.NoError(t, err)
	_ = resp.Body.Close()

	require.Eventually(t, func() bool {
		o, ok := rec.last()

		return ok && o == pool.OutcomeSandboxNotFound
	}, 2*time.Second, 10*time.Millisecond, "expected a sandbox_not_found outcome")
}
