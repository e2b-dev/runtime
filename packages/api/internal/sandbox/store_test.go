package sandbox

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	reservationsredis "github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/reservations/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
	sandboxredis "github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/storage/redis"
)

// newTestStorage spins up a redis testcontainer and returns a fresh redis-backed
// sandbox.Storage. The container and storage are cleaned up via t.Cleanup.
func newTestStorage(t *testing.T) Storage {
	t.Helper()

	client := redis_utils.SetupInstance(t)
	storage, err := sandboxredis.NewStorage(client, noop.NewMeterProvider())
	require.NoError(t, err)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	return storage
}

// =============================================================================
// Test Helpers
// =============================================================================

// CallbackTracker tracks callback invocations with synchronization for async callbacks
type CallbackTracker struct {
	mu            sync.Mutex
	calls         map[string][]Sandbox
	expectedCalls int
	actualCalls   atomic.Int32
	done          chan struct{}
	closeOnce     sync.Once
}

func NewCallbackTracker(expectedCalls int) *CallbackTracker {
	return &CallbackTracker{
		calls:         make(map[string][]Sandbox),
		expectedCalls: expectedCalls,
		done:          make(chan struct{}),
	}
}

// TrackCreation returns a CreationCallback that tracks invocations.
func (ct *CallbackTracker) TrackCreation(name string) CreationCallback {
	return func(_ context.Context, sbx Sandbox, _ CreationMetadata) {
		ct.mu.Lock()
		ct.calls[name] = append(ct.calls[name], sbx)
		ct.mu.Unlock()

		if int(ct.actualCalls.Add(1)) >= ct.expectedCalls {
			ct.closeOnce.Do(func() {
				close(ct.done)
			})
		}
	}
}

// WaitForCalls blocks until expected number of callbacks received or timeout
func (ct *CallbackTracker) WaitForCalls(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-ct.done:
		return
	case <-time.After(timeout):
		require.Failf(t, "timeout waiting for callbacks", "expected %d, got %d", ct.expectedCalls, ct.actualCalls.Load())
	}
}

// AssertNotCalled asserts that a callback was never invoked
func (ct *CallbackTracker) AssertNotCalled(t *testing.T, name string) {
	t.Helper()
	ct.mu.Lock()
	defer ct.mu.Unlock()
	assert.Empty(t, ct.calls[name], "expected callback %s not to be called", name)
}

// AssertCallCount asserts exact number of invocations
func (ct *CallbackTracker) AssertCallCount(t *testing.T, name string, count int) {
	t.Helper()
	ct.mu.Lock()
	defer ct.mu.Unlock()
	assert.Len(t, ct.calls[name], count, "expected callback %s to be called %d times", name, count)
}

// NoOpReservationStorage is a no-op implementation for testing
type NoOpReservationStorage struct{}

func (n *NoOpReservationStorage) Reserve(_ context.Context, _ uuid.UUID, _ string, _ int) (bool, error) {
	return true, nil
}

func (n *NoOpReservationStorage) FinishStart(_ context.Context, _ uuid.UUID, _ string, _ []byte) error {
	return nil
}

func (n *NoOpReservationStorage) Release(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

func (n *NoOpReservationStorage) ClaimKill(_ context.Context, _ uuid.UUID, _ string) (string, bool, error) {
	return "", true, nil
}

func (n *NoOpReservationStorage) ReleaseKillClaim(_ context.Context, _ uuid.UUID, _, _ string) error {
	return nil
}

func (n *NoOpReservationStorage) WaitForStart(_ context.Context, _ uuid.UUID, _ string) ([]byte, error) {
	return nil, nil
}

// MockStorage wraps real storage and can inject errors
type MockStorage struct {
	Storage

	addError    error
	removeError error
	mu          sync.Mutex
}

func NewMockStorage(storage Storage) *MockStorage {
	return &MockStorage{
		Storage: storage,
	}
}

// SetAddError configures Add() to return an error
func (m *MockStorage) SetAddError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addError = err
}

// Add wraps Storage.Add() with error injection
func (m *MockStorage) Add(ctx context.Context, sbx Sandbox) error {
	m.mu.Lock()
	err := m.addError
	m.mu.Unlock()

	if err != nil {
		return err
	}

	return m.Storage.Add(ctx, sbx)
}

// SetRemoveError configures Remove() to return an error
func (m *MockStorage) SetRemoveError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeError = err
}

// Remove wraps Storage.Remove() with error injection
func (m *MockStorage) Remove(ctx context.Context, teamID uuid.UUID, sandboxID, expectedExecutionID string) error {
	m.mu.Lock()
	err := m.removeError
	m.mu.Unlock()

	if err != nil {
		return err
	}

	return m.Storage.Remove(ctx, teamID, sandboxID, expectedExecutionID)
}

// createTestSandbox creates a test sandbox with default values
func createTestSandbox() Sandbox {
	return NewSandbox(
		"test-sandbox-"+uuid.New().String()[:8],
		"test-template",
		consts.ClientID,
		nil, // alias
		"",  // executionID
		uuid.New(),
		uuid.New(),
		map[string]string{"test": "metadata"},
		time.Hour,                 // maxInstanceLength
		time.Now(),                // startTime
		time.Now().Add(time.Hour), // endTime
		2,                         // vcpu
		1024,                      // diskMB
		512,                       // ramMB
		"5.10",                    // kernel
		"1.0",                     // firecracker
		"1.0",                     // envd
		"node-1",
		uuid.New(),
		false, // autoPause
		false, // autoPauseFilesystemOnly
		nil,   // autoResume
		nil,   // envdAccessToken
		nil,   // allowInternetAccess
		"base-template",
		nil, // domain
		nil, // network
		nil, // trafficAccessToken
		nil, // volumes
		nil, // iam tokens
	)
}

// =============================================================================
// Test Cases
// =============================================================================

func TestAdd_NewSandbox(t *testing.T) {
	t.Parallel()
	t.Run("success - all callbacks called", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		// Setup
		storage := newTestStorage(t)
		reservations := &NoOpReservationStorage{}

		tracker := NewCallbackTracker(1) // Expect 1 callback
		callbacks := Callbacks{
			AsyncNewlyCreatedSandbox: tracker.TrackCreation("AsyncNewlyCreatedSandbox"),
		}

		store := NewStore(storage, reservations, callbacks)
		sbx := createTestSandbox()

		// Execute
		err := store.Add(ctx, sbx, &CreationMetadata{})

		// Wait for async callbacks
		tracker.WaitForCalls(t, 2*time.Second)

		// Assert
		require.NoError(t, err)

		// Verify all callbacks called exactly once
		tracker.AssertCallCount(t, "AsyncNewlyCreatedSandbox", 1)

		// Verify sandbox in storage
		stored, err := storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)
		assert.Equal(t, sbx.SandboxID, stored.SandboxID)
	})
}

func TestAdd_NotNewlyCreated(t *testing.T) {
	t.Parallel()
	t.Run("not in cache - creation callback not called", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		storage := newTestStorage(t)
		reservations := &NoOpReservationStorage{}

		// Add with newlyCreated=false, expect 0 callbacks
		tracker := NewCallbackTracker(1)
		callbacks := Callbacks{
			AsyncNewlyCreatedSandbox: tracker.TrackCreation("AsyncNewlyCreatedSandbox"),
		}
		store := NewStore(storage, reservations, callbacks)
		sbx := createTestSandbox()

		err := store.Add(ctx, sbx, nil)
		require.NoError(t, err)

		// Give a small delay for any async callbacks (there should be none)
		time.Sleep(100 * time.Millisecond)

		tracker.AssertNotCalled(t, "AsyncNewlyCreatedSandbox")

		stored, err := storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)
		assert.Equal(t, sbx.SandboxID, stored.SandboxID)
	})
}

func TestAdd_StorageErrors(t *testing.T) {
	t.Parallel()
	t.Run("storage returns non-ErrAlreadyExists error", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		storage := newTestStorage(t)
		mockStorage := NewMockStorage(storage)
		customErr := errors.New("storage failure")
		mockStorage.SetAddError(customErr)

		reservations := &NoOpReservationStorage{}

		// Expect 0 callbacks since error should be returned immediately
		tracker := NewCallbackTracker(1)
		callbacks := Callbacks{
			AsyncNewlyCreatedSandbox: tracker.TrackCreation("AsyncNewlyCreatedSandbox"),
		}
		store := NewStore(mockStorage, reservations, callbacks)
		sbx := createTestSandbox()

		err := store.Add(ctx, sbx, &CreationMetadata{})

		// Error should be returned
		require.Error(t, err)
		assert.Equal(t, customErr, err)

		// Give a small delay for any async callbacks (there should be none)
		time.Sleep(100 * time.Millisecond)

		// No callbacks should have been called
		tracker.AssertNotCalled(t, "AsyncNewlyCreatedSandbox")
	})
}

func TestAdd_ConcurrentCalls(t *testing.T) {
	t.Parallel()
	t.Run("concurrent adds for different sandboxes", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		storage := newTestStorage(t)
		reservations := &NoOpReservationStorage{}

		numGoroutines := 100
		tracker := NewCallbackTracker(numGoroutines) // Each add calls 1 callback

		callbacks := Callbacks{
			AsyncNewlyCreatedSandbox: tracker.TrackCreation("AsyncNewlyCreatedSandbox"),
		}
		store := NewStore(storage, reservations, callbacks)

		teamID := uuid.New()
		var wg sync.WaitGroup
		errorsChan := make(chan error, numGoroutines)

		// Launch concurrent adds
		for i := range numGoroutines {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				sbx := createTestSandbox()
				sbx.SandboxID = fmt.Sprintf("concurrent-sandbox-%d", id)
				sbx.TeamID = teamID
				err := store.Add(ctx, sbx, &CreationMetadata{})
				if err != nil {
					errorsChan <- err
				}
			}(i)
		}

		wg.Wait()
		close(errorsChan)

		// Check for errors
		var errs []error
		for err := range errorsChan {
			errs = append(errs, err)
		}
		assert.Empty(t, errs, "expected no errors from concurrent adds")

		// Wait for all callbacks
		tracker.WaitForCalls(t, 5*time.Second)

		// Verify all callbacks were called expected number of times
		tracker.AssertCallCount(t, "AsyncNewlyCreatedSandbox", numGoroutines)

		// Verify all sandboxes are in storage
		for i := range numGoroutines {
			sandboxID := fmt.Sprintf("concurrent-sandbox-%d", i)
			_, err := storage.Get(ctx, teamID, sandboxID)
			assert.NoError(t, err, "expected sandbox %s to be in storage", sandboxID)
		}
	})
}

// A removal naming a predecessor execution that reaches the store after its
// successor wrote its record, and before the successor's start finished, is
// refused by the storage and leaves the successor's start reservation alone:
// a request waiting on that start keeps waiting instead of failing with no
// result. The successor's own removal still releases the reservation, so a
// new start is admitted.
func TestRemove_StaleExecutionKeepsSuccessorReservation(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	client := redis_utils.SetupInstance(t)
	storage, err := sandboxredis.NewStorage(client, noop.NewMeterProvider())
	require.NoError(t, err)
	go storage.Start(ctx)
	t.Cleanup(func() { storage.Close(context.WithoutCancel(ctx)) })
	reservations := reservationsredis.NewReservationStorage(client, storage.Notifier())
	store := NewStore(storage, reservations, Callbacks{})

	successor := createTestSandbox()
	successor.ExecutionID = uuid.NewString()
	teamID, sandboxID := successor.TeamID, successor.SandboxID
	reserved, err := reservations.Reserve(ctx, teamID, sandboxID, 1)
	require.NoError(t, err)
	require.True(t, reserved)
	reserved, err = reservations.Reserve(ctx, teamID, sandboxID, 1)
	require.NoError(t, err)
	require.False(t, reserved)
	require.NoError(t, storage.Add(ctx, successor))

	store.Remove(ctx, teamID, sandboxID, uuid.NewString())

	pending, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, err = reservations.WaitForStart(pending, teamID, sandboxID)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the successor's start is still pending")
	stored, err := storage.Get(ctx, teamID, sandboxID)
	require.NoError(t, err)
	assert.Equal(t, successor.ExecutionID, stored.ExecutionID)

	store.Remove(ctx, teamID, sandboxID, successor.ExecutionID)

	_, err = storage.Get(ctx, teamID, sandboxID)
	require.ErrorIs(t, err, ErrNotFound)
	reserved, err = reservations.Reserve(ctx, teamID, sandboxID, 1)
	require.NoError(t, err)
	require.True(t, reserved, "the successor's removal released its reservation")
}

// The sync hands every sandbox the node lists as powered off, and only those,
// to EndPoweredOffSandbox, before it returns: the caller tracks what the
// callback starts under the sync still running.
func TestReconcile_EndsPoweredOffSandboxes(t *testing.T) {
	t.Parallel()

	var ended []string
	store := NewStore(newTestStorage(t), &NoOpReservationStorage{}, Callbacks{
		EndPoweredOffSandbox: func(_ context.Context, sbx NodeSandbox) { ended = append(ended, sbx.SandboxID) },
	})

	running := createTestSandbox()
	poweredOff := createTestSandbox()
	require.NoError(t, store.Add(t.Context(), running, nil))
	require.NoError(t, store.Add(t.Context(), poweredOff, nil))

	listed := poweredOff.ToNodeSandbox()
	listed.DiskKept = true
	store.Reconcile(t.Context(), []NodeSandbox{running.ToNodeSandbox(), listed}, "node-1")

	assert.Equal(t, []string{poweredOff.SandboxID}, ended)
}

// The sync hands every sandbox the node lists as ended with nothing kept to
// EndEndedSandbox, and never kills one as an orphan, even with its record
// gone; a sandbox the store does not know that is still running is.
func TestReconcile_EndsEndedSandboxes(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.Mutex
		ended    []string
		orphaned []string
		poweroff []string
	)
	record := func(to *[]string) OrphanCallback {
		return func(_ context.Context, sbx NodeSandbox) {
			mu.Lock()
			defer mu.Unlock()
			*to = append(*to, sbx.SandboxID)
		}
	}
	store := NewStore(newTestStorage(t), &NoOpReservationStorage{}, Callbacks{
		KillOrphanSandbox:    record(&orphaned),
		EndEndedSandbox:      record(&ended),
		EndPoweredOffSandbox: record(&poweroff),
	})

	inStore := createTestSandbox()
	require.NoError(t, store.Add(t.Context(), inStore, nil))

	old := time.Now().Add(-time.Hour)
	listed := func(sbx Sandbox, ended bool) NodeSandbox {
		node := sbx.ToNodeSandbox()
		node.StartTime = old
		node.Ended = ended

		return node
	}
	endedInStore := listed(inStore, true)
	endedGone := listed(createTestSandbox(), true)
	orphan := listed(createTestSandbox(), false)

	store.Reconcile(t.Context(), []NodeSandbox{endedInStore, endedGone, orphan}, "node-1")

	// Reconcile waits for both callbacks before it returns.
	mu.Lock()
	defer mu.Unlock()
	assert.ElementsMatch(t, []string{endedInStore.SandboxID, endedGone.SandboxID}, ended)
	assert.Equal(t, []string{orphan.SandboxID}, orphaned)
	assert.Empty(t, poweroff)
}

// A removal refused by a hold, or by a lock lost before the script ran,
// leaves the record in place, and its reservation with it: a start still
// pending keeps waiting.
func TestRemove_HeldRecordKeepsReservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// refuse makes the storage refuse the removal of sbx.
		refuse func(t *testing.T, store *Store, storage *MockStorage, sbx Sandbox)
	}{
		{
			name: "held",
			refuse: func(t *testing.T, store *Store, _ *MockStorage, sbx Sandbox) {
				t.Helper()
				hold := Hold{TeamID: sbx.TeamID, SandboxID: sbx.SandboxID, ExecutionID: sbx.ExecutionID, SourceNodeID: sbx.NodeID, ID: "op"}
				require.NoError(t, store.HoldExecution(t.Context(), hold, time.Now().Add(time.Minute)))
			},
		},
		{
			name: "lock lost",
			refuse: func(t *testing.T, _ *Store, storage *MockStorage, _ Sandbox) {
				t.Helper()
				storage.SetRemoveError(fmt.Errorf("remove: %w", ErrLockLost))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()

			client := redis_utils.SetupInstance(t)
			backend, err := sandboxredis.NewStorage(client, noop.NewMeterProvider())
			require.NoError(t, err)
			storage := NewMockStorage(backend)
			reservations := reservationsredis.NewReservationStorage(client, backend.Notifier())
			store := NewStore(storage, reservations, Callbacks{})

			sbx := createTestSandbox()
			sbx.ExecutionID = uuid.NewString()
			reserved, err := reservations.Reserve(ctx, sbx.TeamID, sbx.SandboxID, 1)
			require.NoError(t, err)
			require.True(t, reserved)
			reserved, err = reservations.Reserve(ctx, sbx.TeamID, sbx.SandboxID, 1)
			require.NoError(t, err)
			require.False(t, reserved)
			require.NoError(t, storage.Add(ctx, sbx))
			tt.refuse(t, store, storage, sbx)

			store.Remove(ctx, sbx.TeamID, sbx.SandboxID, sbx.ExecutionID)

			_, err = storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
			require.NoError(t, err, "the record stays")
			pending, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
			defer cancel()
			_, err = reservations.WaitForStart(pending, sbx.TeamID, sbx.SandboxID)
			require.ErrorIs(t, err, context.DeadlineExceeded, "the start is still pending")
		})
	}
}

// subscribeSignalNotifier closes subscribed on the first Subscribe, so a test
// can publish once its waiter is registered.
type subscribeSignalNotifier struct {
	reservationsredis.Notifier

	subscribed chan struct{}
	once       sync.Once
}

func (n *subscribeSignalNotifier) Subscribe(routingKey string) (<-chan struct{}, func()) {
	ch, cleanup := n.Notifier.Subscribe(routingKey)
	n.once.Do(func() { close(n.subscribed) })

	return ch, cleanup
}

// newRedisStore returns a Store with its own storage instance over client,
// as a separate API instance has, once its pub/sub subscription is live.
func newRedisStore(t *testing.T, client goredis.UniversalClient) (*Store, *subscribeSignalNotifier) {
	t.Helper()

	storage, err := sandboxredis.NewStorage(client, noop.NewMeterProvider())
	require.NoError(t, err)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	notifier := storage.Notifier()
	probeKey := "notifier-probe:" + uuid.NewString()
	probe, cleanup := notifier.Subscribe(probeKey)
	defer cleanup()
	require.Eventually(t, func() bool {
		notifier.Publish(t.Context(), probeKey)

		select {
		case <-probe:
			return true
		case <-time.After(50 * time.Millisecond):
			return false
		}
	}, 10*time.Second, time.Millisecond, "storage notifier never delivered a publish")

	signal := &subscribeSignalNotifier{Notifier: notifier, subscribed: make(chan struct{})}

	return NewStore(storage, reservationsredis.NewReservationStorage(client, signal), Callbacks{}), signal
}

func TestReserve_WaiterOnAnotherStoreGetsStartedSandbox(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	starter, _ := newRedisStore(t, client)
	joiner, _ := newRedisStore(t, client)

	sbx := createTestSandbox()
	sbx.ExecutionID = uuid.NewString()

	finishStart, waitForStart, err := starter.Reserve(t.Context(), sbx.TeamID, sbx.SandboxID, 10)
	require.NoError(t, err)
	require.NotNil(t, finishStart, "the first reserve owns the start")
	require.Nil(t, waitForStart)

	joinFinish, joinWait, err := joiner.Reserve(t.Context(), sbx.TeamID, sbx.SandboxID, 10)
	require.NoError(t, err)
	require.Nil(t, joinFinish)
	require.NotNil(t, joinWait, "a reserve of a pending start joins it")

	finishStart(sbx, nil)

	got, err := joinWait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, sbx.SandboxID, got.SandboxID)
	assert.Equal(t, sbx.TeamID, got.TeamID)
	assert.Equal(t, sbx.ExecutionID, got.ExecutionID)
	assert.Equal(t, sbx.TemplateID, got.TemplateID)
	assert.True(t, sbx.EndTime.Equal(got.EndTime), "end time %s, got %s", sbx.EndTime, got.EndTime)
}

func TestReserve_WaiterGetsStartAPIErrorPromptly(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	starter, _ := newRedisStore(t, client)
	joiner, joinerNotifier := newRedisStore(t, client)

	teamID, sandboxID := uuid.New(), "failed-start"

	finishStart, _, err := starter.Reserve(t.Context(), teamID, sandboxID, 10)
	require.NoError(t, err)
	require.NotNil(t, finishStart)

	_, waitForStart, err := joiner.Reserve(t.Context(), teamID, sandboxID, 10)
	require.NoError(t, err)
	require.NotNil(t, waitForStart)

	waiterErr := make(chan error, 1)
	go func() {
		_, err := waitForStart(t.Context())
		waiterErr <- err
	}()
	select {
	case <-joinerNotifier.subscribed:
	case <-time.After(5 * time.Second):
		require.Fail(t, "waiter did not subscribe")
	}

	startErr := &api.APIError{
		Code:      http.StatusServiceUnavailable,
		ClientMsg: "no capacity",
		ErrorCode: "capacity_unavailable",
		Err:       errors.New("boom"),
	}
	start := time.Now()
	finishStart(Sandbox{}, startErr)

	select {
	case err := <-waiterErr:
		var apiErr *api.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, startErr.Code, apiErr.Code)
		assert.Equal(t, startErr.ClientMsg, apiErr.ClientMsg)
		assert.Equal(t, startErr.ErrorCode, apiErr.ErrorCode)
		assert.Contains(t, err.Error(), "boom")
		assert.Less(t, time.Since(start), 500*time.Millisecond, "the waiter is woken by the publish")
	case <-time.After(3 * time.Second):
		require.Fail(t, "waiter did not return")
	}
}

// finishCall is one FinishTransition call a transitionStorage saw.
type finishCall struct {
	transitionID string
	action       StateAction
	outcome      sandboxtypes.TransitionOutcome
}

// transitionStorage answers BeginRemoving with its fields and records every
// FinishTransition call.
type transitionStorage struct {
	Storage

	transition  StateTransition
	alreadyDone bool
	err         error

	mu       sync.Mutex
	finishes []finishCall
}

func (s *transitionStorage) BeginRemoving(context.Context, uuid.UUID, string, RemoveOpts) (StateTransition, bool, error) {
	return s.transition, s.alreadyDone, s.err
}

func (s *transitionStorage) FinishTransition(_ context.Context, _ uuid.UUID, _ string, transitionID string, action StateAction, outcome sandboxtypes.TransitionOutcome) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finishes = append(s.finishes, finishCall{transitionID: transitionID, action: action, outcome: outcome})

	return nil
}

func TestStartRemoving_FinishCallback(t *testing.T) {
	t.Parallel()

	sbx := createTestSandbox()
	opts := RemoveOpts{Action: StateActionSnapshot}

	t.Run("started", func(t *testing.T) {
		t.Parallel()

		storage := &transitionStorage{transition: StateTransition{Sandbox: sbx, ID: "transition-1"}}
		store := NewStore(storage, &NoOpReservationStorage{}, Callbacks{})

		transition, alreadyDone, finish, err := store.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, opts)
		require.NoError(t, err)
		assert.False(t, alreadyDone)
		assert.Equal(t, storage.transition, transition)
		require.NotNil(t, finish)

		workErr := errors.New("boom")
		finish(t.Context(), workErr)

		assert.Equal(t, []finishCall{{
			transitionID: "transition-1",
			action:       StateActionSnapshot,
			outcome:      sandboxtypes.TransitionOutcomeOf(workErr),
		}}, storage.finishes)
	})

	t.Run("already done", func(t *testing.T) {
		t.Parallel()

		storage := &transitionStorage{transition: StateTransition{Sandbox: sbx}, alreadyDone: true}
		store := NewStore(storage, &NoOpReservationStorage{}, Callbacks{})

		transition, alreadyDone, finish, err := store.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, opts)
		require.NoError(t, err)
		assert.True(t, alreadyDone)
		assert.Equal(t, storage.transition, transition)
		require.NotNil(t, finish)

		finish(t.Context(), nil)
		assert.Empty(t, storage.finishes)
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()

		backendErr := fmt.Errorf("start: %w", ErrLifecycleHeld)
		storage := &transitionStorage{transition: StateTransition{Sandbox: sbx}, err: backendErr}
		store := NewStore(storage, &NoOpReservationStorage{}, Callbacks{})

		transition, alreadyDone, finish, err := store.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, opts)
		require.ErrorIs(t, err, backendErr)
		assert.False(t, alreadyDone)
		assert.Nil(t, finish)
		assert.Equal(t, storage.transition, transition)
		assert.Empty(t, storage.finishes)
	})
}

// holdRefusals calls refused for each begin a lifecycle hold refuses, once
// the storage has answered and before the store sees the refusal.
type holdRefusals struct {
	Storage

	refused func()
}

func (s *holdRefusals) BeginUpdate(ctx context.Context, teamID uuid.UUID, sandboxID string) (Sandbox, sandboxtypes.UpdateToken, error) {
	sbx, token, err := s.Storage.BeginUpdate(ctx, teamID, sandboxID)
	if errors.Is(err, ErrLifecycleHeld) {
		s.refused()
	}

	return sbx, token, err
}

func (s *holdRefusals) BeginRemoving(ctx context.Context, teamID uuid.UUID, sandboxID string, opts RemoveOpts) (StateTransition, bool, error) {
	transition, alreadyDone, err := s.Storage.BeginRemoving(ctx, teamID, sandboxID, opts)
	if errors.Is(err, ErrLifecycleHeld) {
		s.refused()
	}

	return transition, alreadyDone, err
}

func TestWritesWaitOutALifecycleHold(t *testing.T) {
	t.Parallel()

	writes := map[string]func(ctx context.Context, store *Store, sbx Sandbox) (appliedOnNode string, err error){
		"update": func(ctx context.Context, store *Store, sbx Sandbox) (string, error) {
			updated, err := store.Update(ctx, sbx.TeamID, sbx.SandboxID, func(current Sandbox) (Sandbox, error) { return current, nil })

			return updated.NodeID, err
		},
		"removal": func(ctx context.Context, store *Store, sbx Sandbox) (string, error) {
			transition, _, _, err := store.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, RemoveOpts{Action: StateActionKill, WaitForLifecycleHold: true})

			return transition.Sandbox.NodeID, err
		},
	}
	for name, write := range writes {
		for _, moved := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/moved_%t", name, moved), func(t *testing.T) {
				t.Parallel()

				backend := &holdRefusals{Storage: newTestStorage(t)}
				store := NewStore(backend, nil, Callbacks{})
				sbx := createTestSandbox()
				sbx.ExecutionID = uuid.NewString()
				require.NoError(t, backend.Add(t.Context(), sbx))
				hold := Hold{TeamID: sbx.TeamID, SandboxID: sbx.SandboxID, ExecutionID: sbx.ExecutionID, SourceNodeID: sbx.NodeID, ID: uuid.NewString()}
				require.NoError(t, store.HoldExecution(t.Context(), hold, time.Now().Add(time.Minute)))

				// The first refusal either ends the hold or ends the wait.
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				backend.refused = sync.OnceFunc(func() {
					if !moved {
						cancel()

						return
					}
					_, err := store.MoveHeldExecution(t.Context(), hold, "moved-node")
					assert.NoError(t, err)
				})

				node, err := write(ctx, store, sbx)
				if !moved {
					require.ErrorIs(t, err, ErrLifecycleHeld, "a wait that ends is refused as retryable")

					return
				}
				require.NoError(t, err)
				assert.Equal(t, "moved-node", node)
			})
		}
	}
}
