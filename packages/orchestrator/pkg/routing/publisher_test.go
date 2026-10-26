package routing

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	noopmetric "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

type memoryCatalog struct {
	mu              sync.Mutex
	records         map[string]*catalog.SandboxInfo
	ttls            map[string]time.Duration
	storeErr        error
	storeAppliedErr error
	deleteErr       error

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

	return c.storeAppliedErr
}

func (c *memoryCatalog) DeleteSandboxIfOwner(_ context.Context, sandboxID string, owner catalog.SandboxOwner) (catalog.DeleteOutcome, error) {
	if owner.ExecutionID == "" || owner.LifecycleID == "" || owner.OrchestratorID == "" {
		return "", catalog.ErrInvalidSandboxOwner
	}

	if c.deleteErr != nil {
		return "", c.deleteErr
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	info, ok := c.records[sandboxID]
	if !ok {
		return catalog.DeleteOutcomeAbsent, nil
	}
	if info.ExecutionID == "" || info.LifecycleID == "" || info.OrchestratorID == "" {
		return catalog.DeleteOutcomeUnverifiable, nil
	}
	if info.ExecutionID != owner.ExecutionID || info.LifecycleID != owner.LifecycleID || info.OrchestratorID != owner.OrchestratorID {
		return catalog.DeleteOutcomeSuperseded, nil
	}

	delete(c.records, sandboxID)

	return catalog.DeleteOutcomeDeleted, nil
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
	require.Equal(t, "lc-1", got.LifecycleID)
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

func TestPublisher_OnStoppingPreservesReplacementOwner(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name           string
		executionID    string
		lifecycleID    string
		orchestratorID string
	}{
		{
			name:           "new execution",
			executionID:    "exec-2",
			lifecycleID:    "lc-1",
			orchestratorID: "orch-1",
		},
		{
			name:           "new lifecycle in same execution",
			executionID:    "exec-1",
			lifecycleID:    "lc-2",
			orchestratorID: "orch-1",
		},
		{
			name:           "new orchestrator process",
			executionID:    "exec-1",
			lifecycleID:    "lc-1",
			orchestratorID: "orch-2",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			c := newMemoryCatalog()
			p := newPublisher(t, c)
			sbx := testSandbox(t, "sbx-1", "lc-1", sandboxtypes.SandboxTypeSandbox)
			p.OnInsert(t.Context(), sbx)

			// A replacement route lands before this lifecycle's delayed stop.
			replacement := &catalog.SandboxInfo{
				ExecutionID:    test.executionID,
				LifecycleID:    test.lifecycleID,
				OrchestratorID: test.orchestratorID,
				OrchestratorIP: "10.2.3.4",
			}
			require.NoError(t, c.StoreSandbox(t.Context(), "sbx-1", replacement, time.Hour))

			p.OnStopping(t.Context(), sbx)

			got, err := c.GetSandbox(t.Context(), "sbx-1")
			require.NoError(t, err, "a stale stop must preserve the replacement route")
			require.Equal(t, replacement, got)
		})
	}
}

func TestPublisher_OnStoppingReportsDeleteOutcome(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		replacement *catalog.SandboxInfo
		absent      bool
		deleteErr   error
		wantResult  string
	}{
		{name: "deleted", wantResult: "ok"},
		{name: "absent", absent: true, wantResult: "absent"},
		{
			name: "superseded",
			replacement: &catalog.SandboxInfo{
				ExecutionID: "exec-1", LifecycleID: "lc-2", OrchestratorID: "orch-1",
			},
			wantResult: "superseded",
		},
		{
			name: "unverifiable",
			replacement: &catalog.SandboxInfo{
				ExecutionID: "exec-1", OrchestratorID: "orch-1",
			},
			wantResult: "unverifiable",
		},
		{name: "error", deleteErr: errors.New("redis down"), wantResult: "error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
			c := newMemoryCatalog()
			p, err := New(provider, c, "orch-1", "10.1.2.3")
			require.NoError(t, err)
			sbx := testSandbox(t, "sbx-1", "lc-1", sandboxtypes.SandboxTypeSandbox)
			p.OnInsert(t.Context(), sbx)

			if test.absent {
				delete(c.records, "sbx-1")
			}
			if test.replacement != nil {
				require.NoError(t, c.StoreSandbox(t.Context(), "sbx-1", test.replacement, time.Hour))
			}
			c.deleteErr = test.deleteErr
			p.OnStopping(t.Context(), sbx)

			var metrics metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &metrics))
			for _, scope := range metrics.ScopeMetrics {
				for _, metric := range scope.Metrics {
					if metric.Name != string(telemetry.RoutingDeleteTotal) {
						continue
					}

					sum, ok := metric.Data.(metricdata.Sum[int64])
					require.True(t, ok)
					require.Len(t, sum.DataPoints, 1)
					require.EqualValues(t, 1, sum.DataPoints[0].Value)
					result, ok := sum.DataPoints[0].Attributes.Value(attribute.Key("result"))
					require.True(t, ok)
					require.Equal(t, test.wantResult, result.AsString())

					return
				}
			}
			t.Fatal("routing delete outcome was not recorded")
		})
	}
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

func TestPublisher_StopAfterTimedOutStoreDeletesAppliedRecord(t *testing.T) {
	t.Parallel()

	c := newMemoryCatalog()
	c.storeAppliedErr = context.DeadlineExceeded
	p := newPublisher(t, c)
	sbx := testSandbox(t, "sbx-1", "lc-1", sandboxtypes.SandboxTypeSandbox)

	// Redis may apply the SET before the caller sees its timeout.
	p.OnInsert(t.Context(), sbx)
	_, err := c.GetSandbox(t.Context(), "sbx-1")
	require.NoError(t, err)

	p.OnStopping(t.Context(), sbx)
	_, err = c.GetSandbox(t.Context(), "sbx-1")
	require.ErrorIs(t, err, catalog.ErrSandboxNotFound)
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
