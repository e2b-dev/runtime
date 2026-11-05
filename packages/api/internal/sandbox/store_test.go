package sandbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	reservationsredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/reservations/redis"
	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
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
		t.Fatalf("timeout waiting for callbacks: expected %d, got %d", ct.expectedCalls, ct.actualCalls.Load())
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

func (n *NoOpReservationStorage) Reserve(_ context.Context, _ uuid.UUID, _ string, _ int) (func(Sandbox, error), func(ctx context.Context) (Sandbox, error), error) {
	return nil, nil, nil
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

func (n *NoOpReservationStorage) WaitForStart(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
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
	finishStart, _, err := reservations.Reserve(ctx, teamID, sandboxID, 1)
	require.NoError(t, err)
	require.NotNil(t, finishStart)
	_, waitForStart, err := reservations.Reserve(ctx, teamID, sandboxID, 1)
	require.NoError(t, err)
	require.NoError(t, storage.Add(ctx, successor))

	store.Remove(ctx, teamID, sandboxID, uuid.NewString())

	pending, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, err = waitForStart(pending)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the successor's start is still pending")
	stored, err := storage.Get(ctx, teamID, sandboxID)
	require.NoError(t, err)
	assert.Equal(t, successor.ExecutionID, stored.ExecutionID)

	store.Remove(ctx, teamID, sandboxID, successor.ExecutionID)

	_, err = storage.Get(ctx, teamID, sandboxID)
	require.ErrorIs(t, err, ErrNotFound)
	finishStart, _, err = reservations.Reserve(ctx, teamID, sandboxID, 1)
	require.NoError(t, err)
	require.NotNil(t, finishStart, "the successor's removal released its reservation")
}

// The sync hands every sandbox the node lists as powered off, and only those,
// to EndPoweredOffSandbox.
func TestReconcile_EndsPoweredOffSandboxes(t *testing.T) {
	t.Parallel()

	ended := make(chan NodeSandbox, 2)
	store := NewStore(newTestStorage(t), &NoOpReservationStorage{}, Callbacks{
		EndPoweredOffSandbox: func(_ context.Context, sbx NodeSandbox) { ended <- sbx },
	})

	running := createTestSandbox()
	poweredOff := createTestSandbox()
	require.NoError(t, store.Add(t.Context(), running, nil))
	require.NoError(t, store.Add(t.Context(), poweredOff, nil))

	listed := poweredOff.ToNodeSandbox()
	listed.DiskKept = true
	store.Reconcile(t.Context(), []NodeSandbox{running.ToNodeSandbox(), listed}, "node-1")

	select {
	case got := <-ended:
		assert.Equal(t, poweredOff.SandboxID, got.SandboxID)
	case <-time.After(5 * time.Second):
		t.Fatal("EndPoweredOffSandbox was not called")
	}

	select {
	case got := <-ended:
		t.Fatalf("EndPoweredOffSandbox called for %s, which is not powered off", got.SandboxID)
	case <-time.After(200 * time.Millisecond):
	}
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
				hold := Hold{TeamID: sbx.TeamID, SandboxID: sbx.SandboxID, ExecutionID: sbx.ExecutionID, ID: "op"}
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
			finishStart, _, err := reservations.Reserve(ctx, sbx.TeamID, sbx.SandboxID, 1)
			require.NoError(t, err)
			require.NotNil(t, finishStart)
			_, waitForStart, err := reservations.Reserve(ctx, sbx.TeamID, sbx.SandboxID, 1)
			require.NoError(t, err)
			require.NoError(t, storage.Add(ctx, sbx))
			tt.refuse(t, store, storage, sbx)

			store.Remove(ctx, sbx.TeamID, sbx.SandboxID, sbx.ExecutionID)

			_, err = storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
			require.NoError(t, err, "the record stays")
			pending, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
			defer cancel()
			_, err = waitForStart(pending)
			require.ErrorIs(t, err, context.DeadlineExceeded, "the start is still pending")
		})
	}
}
