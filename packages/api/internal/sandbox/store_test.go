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

	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// newTestStorage spins up a redis testcontainer and returns a fresh redis-backed
// sandbox.Storage. The container and storage are cleaned up via t.Cleanup.
func newTestStorage(t *testing.T) Storage {
	t.Helper()

	client := redis_utils.SetupInstance(t)
	storage, err := sandboxredis.NewStorage(client, noop.NewMeterProvider(), nil)
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

// Track returns a callback function that tracks invocations
func (ct *CallbackTracker) Track(name string) InsertCallback {
	return func(_ context.Context, sbx Sandbox) {
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

func (n *NoOpReservationStorage) ClaimKill(_ context.Context, _ uuid.UUID, _ string) (bool, error) {
	return true, nil
}

func (n *NoOpReservationStorage) ReleaseKillClaim(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}

// MockStorage wraps real storage and can inject errors
type MockStorage struct {
	Storage

	addError error
	mu       sync.Mutex
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

		tracker := NewCallbackTracker(2) // Expect 2 callbacks
		callbacks := Callbacks{
			AddSandboxToRoutingTable: tracker.Track("AddSandboxToRoutingTable"),
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
		tracker.AssertCallCount(t, "AddSandboxToRoutingTable", 1)
		tracker.AssertCallCount(t, "AsyncNewlyCreatedSandbox", 1)

		// Verify sandbox in storage
		stored, err := storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)
		assert.Equal(t, sbx.SandboxID, stored.SandboxID)
	})
}

func TestAdd_NotNewlyCreated(t *testing.T) {
	t.Parallel()
	t.Run("not in cache - AddSandboxToRoutingTable called", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()

		storage := newTestStorage(t)
		reservations := &NoOpReservationStorage{}

		// Add with newlyCreated=false, expect 1 callback
		tracker := NewCallbackTracker(1)
		callbacks := Callbacks{
			AddSandboxToRoutingTable: tracker.Track("AddSandboxToRoutingTable"),
			AsyncNewlyCreatedSandbox: tracker.TrackCreation("AsyncNewlyCreatedSandbox"),
		}
		store := NewStore(storage, reservations, callbacks)
		sbx := createTestSandbox()

		err := store.Add(ctx, sbx, nil)
		tracker.WaitForCalls(t, 2*time.Second)

		require.NoError(t, err)
		tracker.AssertCallCount(t, "AddSandboxToRoutingTable", 1)
		tracker.AssertNotCalled(t, "AsyncNewlyCreatedSandbox")
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
			AddSandboxToRoutingTable: tracker.Track("AddSandboxToRoutingTable"),
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
		tracker.AssertNotCalled(t, "AddSandboxToRoutingTable")
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
		tracker := NewCallbackTracker(numGoroutines * 2) // Each add calls 2 callbacks

		callbacks := Callbacks{
			AddSandboxToRoutingTable: tracker.Track("AddSandboxToRoutingTable"),
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
		tracker.AssertCallCount(t, "AddSandboxToRoutingTable", numGoroutines)
		tracker.AssertCallCount(t, "AsyncNewlyCreatedSandbox", numGoroutines)

		// Verify all sandboxes are in storage
		for i := range numGoroutines {
			sandboxID := fmt.Sprintf("concurrent-sandbox-%d", i)
			_, err := storage.Get(ctx, teamID, sandboxID)
			assert.NoError(t, err, "expected sandbox %s to be in storage", sandboxID)
		}
	})
}
