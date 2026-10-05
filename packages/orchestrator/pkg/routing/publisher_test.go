package routing

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	noopmetric "go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

type memoryCatalog struct {
	mu        sync.Mutex
	records   map[string]*catalog.SandboxInfo
	ttls      map[string]time.Duration
	storeErr  error
	deleteErr error

	// storeStarted is closed when StoreSandbox begins; storeRelease blocks it until closed.
	storeStarted chan struct{}
	storeRelease chan struct{}
}

func newMemoryCatalog() *memoryCatalog {
	return &memoryCatalog{
		records: map[string]*catalog.SandboxInfo{},
		ttls:    map[string]time.Duration{},
	}
}

func (c *memoryCatalog) GetSandbox(_ context.Context, sandboxID string) (*catalog.SandboxInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	info, ok := c.records[sandboxID]
	if !ok {
		return nil, catalog.ErrSandboxNotFound
	}

	return info, nil
}

func (c *memoryCatalog) StoreSandbox(_ context.Context, sandboxID string, info *catalog.SandboxInfo, ttl time.Duration) error {
	if c.storeStarted != nil {
		close(c.storeStarted)
		<-c.storeRelease
	}

	if c.storeErr != nil {
		return c.storeErr
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.records[sandboxID] = info
	c.ttls[sandboxID] = ttl

	return nil
}

func (c *memoryCatalog) DeleteSandbox(_ context.Context, sandboxID string, executionID string) error {
	if c.deleteErr != nil {
		return c.deleteErr
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if info, ok := c.records[sandboxID]; ok && info.ExecutionID == executionID {
		delete(c.records, sandboxID)
	}

	return nil
}

// has reports whether the record of the sandbox every test uses ("sbx-1") exists.
func (c *memoryCatalog) has() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.records[testSandboxID]

	return ok
}

func newPublisher(t *testing.T, c Store) *Publisher {
	t.Helper()

	p, err := New(noopmetric.NewMeterProvider(), c, "orch-1", "10.1.2.3")
	require.NoError(t, err)

	return p
}

const (
	testSandboxID   = "sbx-1"
	testExecutionID = "exec-1"
)

func testSandbox(t *testing.T, sandboxID, lifecycleID string, sandboxType sandboxtypes.SandboxType) *sandbox.Sandbox {
	t.Helper()

	slot, err := network.NewSlot("test", 1, network.Config{}, network.NoopEgressProxy{})
	require.NoError(t, err)

	sbx := &sandbox.Sandbox{
		LifecycleID: lifecycleID,
		Metadata: &sandbox.Metadata{
			Config: sandbox.NewConfig(sandbox.Config{MaxSandboxLengthHours: 2}),
			Runtime: sandboxtypes.RuntimeMetadata{
				SandboxID:   sandboxID,
				ExecutionID: testExecutionID,
				SandboxType: sandboxType,
			},
		},
		Resources: &sandbox.Resources{Slot: slot},
	}
	sbx.SetStartedAt(time.Unix(1_700_000_000, 0).UTC())

	return sbx
}

func TestPublisher_OnInsertWritesRecord(t *testing.T) {
	t.Parallel()

	c := newMemoryCatalog()
	p := newPublisher(t, c)
	sbx := testSandbox(t, "sbx-1", "lc-1", sandboxtypes.SandboxTypeSandbox)

	p.OnInsert(t.Context(), sbx)

	got, err := c.GetSandbox(t.Context(), "sbx-1")
	require.NoError(t, err)
	require.Equal(t, "orch-1", got.OrchestratorID)
	require.Equal(t, "10.1.2.3", got.OrchestratorIP)
	require.Equal(t, "exec-1", got.ExecutionID)
	require.Equal(t, sbx.GetStartedAt(), got.StartedAt)
	require.EqualValues(t, 2, got.MaxLengthInHours)
	require.Equal(t, 2*time.Hour, c.ttls["sbx-1"])
}

func TestPublisher_OnStoppingDeletesRecord(t *testing.T) {
	t.Parallel()

	c := newMemoryCatalog()
	p := newPublisher(t, c)
	sbx := testSandbox(t, "sbx-1", "lc-1", sandboxtypes.SandboxTypeSandbox)

	p.OnInsert(t.Context(), sbx)
	p.OnStopping(t.Context(), sbx)

	_, err := c.GetSandbox(t.Context(), "sbx-1")
	require.ErrorIs(t, err, catalog.ErrSandboxNotFound)
}

func TestPublisher_OnStoppingCollectsLifecycleState(t *testing.T) {
	t.Parallel()

	c := newMemoryCatalog()
	p := newPublisher(t, c)
	sbx := testSandbox(t, "sbx-1", "lc-1", sandboxtypes.SandboxTypeSandbox)

	p.OnInsert(t.Context(), sbx)
	require.Len(t, p.routes, 1, "one entry per live sandbox while it runs")

	p.OnStopping(t.Context(), sbx)
	require.Empty(t, p.routes)
}

func TestPublisher_BuildSandboxIsSkipped(t *testing.T) {
	t.Parallel()

	c := newMemoryCatalog()
	p := newPublisher(t, c)
	sbx := testSandbox(t, "build-1", "lc-1", sandboxtypes.SandboxTypeBuild)

	p.OnInsert(t.Context(), sbx)

	_, err := c.GetSandbox(t.Context(), "build-1")
	require.ErrorIs(t, err, catalog.ErrSandboxNotFound)
}

func TestPublisher_StoreErrorIsSwallowed(t *testing.T) {
	t.Parallel()

	c := newMemoryCatalog()
	c.storeErr = errors.New("redis down")
	p := newPublisher(t, c)
	sbx := testSandbox(t, "sbx-1", "lc-1", sandboxtypes.SandboxTypeSandbox)

	p.OnInsert(t.Context(), sbx)

	// The write was attempted, so the stop still issues a delete (the SET may
	// have landed before a timeout) and then forgets the lifecycle.
	p.OnStopping(t.Context(), sbx)
	require.Empty(t, p.routes)
}

func TestPublisher_StopBeforeInsertLeavesTombstoneAndSkipsWrite(t *testing.T) {
	t.Parallel()

	c := newMemoryCatalog()
	p := newPublisher(t, c)

	// A record owned by another writer for the same sandbox must survive a stop
	// of a lifecycle this publisher never published.
	require.NoError(t, c.StoreSandbox(t.Context(), "sbx-1", &catalog.SandboxInfo{ExecutionID: "exec-1"}, time.Hour))
	sbx := testSandbox(t, "sbx-1", "lc-old", sandboxtypes.SandboxTypeSandbox)

	// MarkStopping can fire before OnInsert (MarkRunning inserts into the live
	// map first). The stop leaves a tombstone ...
	p.OnStopping(t.Context(), sbx)
	require.True(t, c.has())
	require.Len(t, p.routes, 1)

	// ... and the late OnInsert must not write, and must collect the tombstone.
	c.records = map[string]*catalog.SandboxInfo{}
	p.OnInsert(t.Context(), sbx)
	require.False(t, c.has())
	require.Empty(t, p.routes)
}

func TestPublisher_StopDuringInFlightStoreWaitsAndDeletes(t *testing.T) {
	t.Parallel()

	c := newMemoryCatalog()
	c.storeStarted = make(chan struct{})
	c.storeRelease = make(chan struct{})
	p := newPublisher(t, c)
	sbx := testSandbox(t, "sbx-1", "lc-1", sandboxtypes.SandboxTypeSandbox)

	insertDone := make(chan struct{})
	go func() {
		defer close(insertDone)
		p.OnInsert(t.Context(), sbx)
	}()
	<-c.storeStarted

	// The sandbox dies while the SET is in flight.
	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		p.OnStopping(t.Context(), sbx)
	}()

	select {
	case <-stopDone:
		t.Fatal("OnStopping returned while the store was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(c.storeRelease)
	<-insertDone
	<-stopDone

	require.False(t, c.has(), "route must not outlive the sandbox")
	require.Empty(t, p.routes)
}

func TestPublisher_DeleteErrorForgetsLifecycle(t *testing.T) {
	t.Parallel()

	c := newMemoryCatalog()
	p := newPublisher(t, c)
	sbx := testSandbox(t, "sbx-1", "lc-1", sandboxtypes.SandboxTypeSandbox)

	p.OnInsert(t.Context(), sbx)
	c.deleteErr = errors.New("redis down")
	p.OnStopping(t.Context(), sbx)

	// The record stays (it expires via TTL); the publisher does not keep state for it.
	require.True(t, c.has())
	require.Empty(t, p.routes)
}

func TestPublisher_ViaSandboxMap(t *testing.T) {
	t.Parallel()

	c := newMemoryCatalog()
	p := newPublisher(t, c)
	sandboxes := sandbox.NewSandboxesMap()
	sandboxes.Subscribe(p)

	sbx := testSandbox(t, "sbx-1", "lc-1", sandboxtypes.SandboxTypeSandbox)

	sandboxes.MarkRunning(t.Context(), sbx)
	_, err := c.GetSandbox(t.Context(), "sbx-1")
	require.NoError(t, err)

	// A stale lifecycle ID must not remove the live sandbox nor its record.
	require.False(t, sandboxes.MarkStopping(t.Context(), "sbx-1", "lc-stale"))
	_, err = c.GetSandbox(t.Context(), "sbx-1")
	require.NoError(t, err)

	require.True(t, sandboxes.MarkStopping(t.Context(), "sbx-1", "lc-1"))
	_, err = c.GetSandbox(t.Context(), "sbx-1")
	require.ErrorIs(t, err, catalog.ErrSandboxNotFound)
}
