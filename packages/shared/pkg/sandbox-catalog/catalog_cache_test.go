package sandbox_catalog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeCatalog is a programmable inner catalog: it returns getResult/getErr for
// GetSandbox so a test can simulate a healthy backend then a flap.
type fakeCatalog struct {
	info     *SandboxInfo
	err      error
	getCalls int
}

func (f *fakeCatalog) GetSandbox(_ context.Context, _ string) (*SandboxInfo, error) {
	f.getCalls++
	if f.err != nil {
		return nil, f.err
	}

	return f.info, nil
}

func (f *fakeCatalog) StoreSandbox(_ context.Context, _ string, _ *SandboxInfo, _ time.Duration) error {
	return nil
}
func (f *fakeCatalog) DeleteSandbox(_ context.Context, _ string, _ string) error { return nil }
func (f *fakeCatalog) Close(_ context.Context) error                             { return nil }

func TestCachingCatalog_ServesCachedRouteOnTransientError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	want := &SandboxInfo{OrchestratorIP: "10.0.0.7", ExecutionID: "exec-1"}
	inner := &fakeCatalog{info: want}
	c := NewCachingSandboxCatalog(inner, time.Minute, nil)

	// Healthy: populates the cache.
	got, err := c.GetSandbox(ctx, "sbx-1")
	require.NoError(t, err)
	require.Equal(t, want, got)

	// Flap: inner errors, cache (fresh) serves the last known route.
	inner.info, inner.err = nil, errors.New("redis unavailable")
	got, err = c.GetSandbox(ctx, "sbx-1")
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestCachingCatalog_DoesNotServeNotFoundFromCache(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	inner := &fakeCatalog{info: &SandboxInfo{OrchestratorIP: "10.0.0.7"}}
	c := NewCachingSandboxCatalog(inner, time.Minute, nil)

	_, err := c.GetSandbox(ctx, "sbx-1")
	require.NoError(t, err)

	// A definitive not-found must surface, never a stale cached route.
	inner.info, inner.err = nil, ErrSandboxNotFound
	_, err = c.GetSandbox(ctx, "sbx-1")
	require.ErrorIs(t, err, ErrSandboxNotFound)

	// The entry is evicted, so a later transient error has nothing to serve.
	inner.err = errors.New("redis unavailable")
	_, err = c.GetSandbox(ctx, "sbx-1")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrSandboxNotFound)
}

func TestCachingCatalog_StaleBeyondWindowPropagatesError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	inner := &fakeCatalog{info: &SandboxInfo{OrchestratorIP: "10.0.0.7"}}
	c := NewCachingSandboxCatalog(inner, time.Millisecond, nil)

	_, err := c.GetSandbox(ctx, "sbx-1")
	require.NoError(t, err)

	time.Sleep(5 * time.Millisecond) // age the entry past maxStale

	flap := errors.New("redis unavailable")
	inner.info, inner.err = nil, flap
	_, err = c.GetSandbox(ctx, "sbx-1")
	require.ErrorIs(t, err, flap)
}

func TestCachingCatalog_DeleteEvicts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	inner := &fakeCatalog{info: &SandboxInfo{OrchestratorIP: "10.0.0.7"}}
	c := NewCachingSandboxCatalog(inner, time.Minute, nil)

	_, err := c.GetSandbox(ctx, "sbx-1")
	require.NoError(t, err)

	require.NoError(t, c.DeleteSandbox(ctx, "sbx-1", "exec-1"))

	// After delete, a transient error has no cached route to fall back on.
	inner.info, inner.err = nil, errors.New("redis unavailable")
	_, err = c.GetSandbox(ctx, "sbx-1")
	require.Error(t, err)
}


func TestCachingCatalog_DoesNotServeCacheWhenCallerCancelled(t *testing.T) {
	t.Parallel()

	want := &SandboxInfo{OrchestratorIP: "10.0.0.9", ExecutionID: "exec-9"}
	inner := &fakeCatalog{info: want}
	c := NewCachingSandboxCatalog(inner, time.Minute, nil)

	// Healthy call under a live context populates the cache.
	got, err := c.GetSandbox(context.Background(), "sbx-9")
	require.NoError(t, err)
	require.Equal(t, want, got)

	// The caller's own context is cancelled: the inner error must surface
	// unchanged, with no cached route served (the request is already gone).
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	inner.info, inner.err = nil, errors.New("redis unavailable")
	_, err = c.GetSandbox(ctx, "sbx-9")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrSandboxNotFound)
}
