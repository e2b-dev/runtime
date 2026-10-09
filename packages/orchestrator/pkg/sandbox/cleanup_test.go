//go:build linux

package sandbox

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func TestCleanupRunOrder(t *testing.T) {
	t.Parallel()

	cleanup := NewCleanup()
	order := make([]string, 0, 4)
	add := func(name string, priority bool) {
		f := func(context.Context) error {
			order = append(order, name)

			return nil
		}
		if priority {
			cleanup.AddPriority(t.Context(), f)
		} else {
			cleanup.Add(t.Context(), f)
		}
	}

	add("normal-1", false)
	add("priority-1", true)
	add("normal-2", false)
	add("priority-2", true)

	require.NoError(t, cleanup.Run(t.Context()))
	require.Equal(t, []string{"priority-2", "priority-1", "normal-2", "normal-1"}, order)
}

func TestCleanupCallbackCanAddCleanup(t *testing.T) {
	t.Parallel()

	cleanup := NewCleanup()
	lateCalls := 0
	cleanup.Add(t.Context(), func(ctx context.Context) error {
		cleanup.Add(ctx, func(context.Context) error {
			lateCalls++

			return nil
		})

		return nil
	})

	require.NoError(t, cleanup.Run(t.Context()))
	require.Equal(t, 1, lateCalls)
}

func TestCleanupConcurrentAddAndRunOwnsCallback(t *testing.T) {
	t.Parallel()

	const iterations = 1000

	for _, tc := range []struct {
		name string
		add  func(*Cleanup, context.Context, func(context.Context) error)
	}{
		{name: "normal", add: (*Cleanup).Add},
		{name: "priority", add: (*Cleanup).AddPriority},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			for range iterations {
				cleanup := NewCleanup()
				start := make(chan struct{})
				var calls atomic.Int32
				var runErr error
				var wg sync.WaitGroup
				wg.Add(2)

				go func() {
					defer wg.Done()
					<-start
					tc.add(cleanup, t.Context(), func(context.Context) error {
						calls.Add(1)

						return nil
					})
				}()

				go func() {
					defer wg.Done()
					<-start
					runErr = cleanup.Run(t.Context())
				}()

				close(start)
				wg.Wait()

				require.NoError(t, runErr)
				require.Equal(t, int32(1), calls.Load())
			}
		})
	}
}

type rootfsLifecycleProbe struct {
	startCalled chan struct{}
	closeCalled chan struct{}
	allowReady  chan struct{}
	ready       chan struct{}
	abort       chan struct{}
	starts      int
	closes      int
}

func newRootfsLifecycleProbe() *rootfsLifecycleProbe {
	return &rootfsLifecycleProbe{
		startCalled: make(chan struct{}),
		closeCalled: make(chan struct{}),
		allowReady:  make(chan struct{}),
		ready:       make(chan struct{}),
		abort:       make(chan struct{}),
		starts:      0,
		closes:      0,
	}
}

func (p *rootfsLifecycleProbe) Start(context.Context) error {
	p.starts++
	close(p.startCalled)

	select {
	case <-p.allowReady:
		close(p.ready)
	case <-p.abort:
	}

	return nil
}

func (p *rootfsLifecycleProbe) Close(context.Context) error {
	close(p.closeCalled)

	select {
	case <-p.ready:
		p.closes++
	case <-p.abort:
	}

	return nil
}

func TestStartRootfsProviderAfterCleanup(t *testing.T) {
	t.Parallel()

	cleanup := NewCleanup()
	require.NoError(t, cleanup.Run(t.Context()))

	provider := newRootfsLifecycleProbe()
	t.Cleanup(func() { close(provider.abort) })

	done := make(chan struct{})
	go func() {
		startRootfsProvider(t.Context(), t.Context(), cleanup, provider, logger.NewNopLogger())
		close(done)
	}()

	select {
	case <-provider.startCalled:
	case <-time.After(time.Second):
		t.Fatal("rootfs Start was not launched before late cleanup")
	}

	select {
	case <-provider.closeCalled:
	case <-time.After(time.Second):
		t.Fatal("late cleanup did not call rootfs Close")
	}

	select {
	case <-done:
		t.Fatal("rootfs Close returned before Start published readiness")
	default:
	}

	close(provider.allowReady)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("rootfs Close did not return after Start published readiness")
	}

	require.Equal(t, 1, provider.starts)
	require.Equal(t, 1, provider.closes)
}

//nolint:paralleltest // observes the package-global test logger
func TestCleanupLateCallbackObservability(t *testing.T) {
	const (
		message = "cleanup callback ran after cleanup completed"
		edgeID  = "cleanup-late-observability-test"
	)

	parentCtx := logger.ContextWithEdgeTraceID(t.Context(), edgeID)
	ctx, cancel := context.WithCancel(parentCtx)
	cancel()
	entriesForTest := func() []observer.LoggedEntry {
		var entries []observer.LoggedEntry
		for _, entry := range testLogObserver.FilterMessage(message).All() {
			if entry.ContextMap()["edge_trace_id"] == edgeID {
				entries = append(entries, entry)
			}
		}

		return entries
	}
	requireEdgeID := func(got context.Context) {
		require.NoError(t, got.Err(), "late callback received the canceled parent context")
		gotID, ok := logger.GetEdgeTraceID(got)
		require.True(t, ok)
		require.Equal(t, edgeID, gotID)
	}

	before := len(entriesForTest())
	cleanup := NewCleanup()
	ordinaryCalls := 0
	cleanup.Add(ctx, func(got context.Context) error {
		ordinaryCalls++
		requireEdgeID(got)

		return nil
	})
	require.NoError(t, cleanup.Run(ctx))
	require.Equal(t, 1, ordinaryCalls)
	require.Len(t, entriesForTest(), before, "ordinary registration must not emit a late-cleanup event")

	lateCalls := 0
	cleanup.Add(ctx, func(got context.Context) error {
		lateCalls++
		requireEdgeID(got)

		return nil
	})
	cleanup.AddPriority(ctx, func(got context.Context) error {
		lateCalls++
		requireEdgeID(got)

		return nil
	})
	sentinel := errors.New("late cleanup failed")
	cleanup.Add(ctx, func(got context.Context) error {
		lateCalls++
		requireEdgeID(got)

		return sentinel
	})

	entries := entriesForTest()
	require.Len(t, entries, before+3)
	entries = entries[before:]
	require.Equal(t, 3, lateCalls)

	require.Equal(t, zapcore.InfoLevel, entries[0].Level)
	require.Equal(t, false, entries[0].ContextMap()["priority"])
	require.Contains(t, entries[0].ContextMap(), "duration")
	require.NotContains(t, entries[0].ContextMap(), "error")
	require.Equal(t, zapcore.InfoLevel, entries[1].Level)
	require.Equal(t, true, entries[1].ContextMap()["priority"])
	require.Contains(t, entries[1].ContextMap(), "duration")
	require.NotContains(t, entries[1].ContextMap(), "error")
	require.Equal(t, zapcore.ErrorLevel, entries[2].Level)
	require.Equal(t, false, entries[2].ContextMap()["priority"])
	require.Contains(t, entries[2].ContextMap(), "duration")
	require.Equal(t, sentinel.Error(), entries[2].ContextMap()["error"])

	var errorFieldFound bool
	for _, field := range entries[2].Context {
		if field.Key != "error" {
			continue
		}
		errorFieldFound = true
		require.Equal(t, zapcore.ErrorType, field.Type)
		require.Same(t, sentinel, field.Interface)
	}
	require.True(t, errorFieldFound)
}

func TestCgroupRemovalAcrossCleanupRegistration(t *testing.T) {
	t.Parallel()

	startErr := errors.New("startup failed")
	removeErr := errors.New("remove failed")

	for _, tc := range []struct {
		name            string
		registerRemoval bool
		failStartup     bool
		removeFails     bool
		wantOrder       []string
	}{
		{name: "early failure", failStartup: true, removeFails: true, wantOrder: []string{"remove", "rootfs"}},
		{name: "late failure", registerRemoval: true, failStartup: true, removeFails: true, wantOrder: []string{"stats", "remove", "rootfs"}},
		{name: "successful startup", registerRemoval: true, wantOrder: []string{"stats", "remove", "rootfs"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			cleanup := NewCleanup()
			var order []string
			addStep := func(name string) func(context.Context) error {
				return func(ctx context.Context) error {
					if ctx.Err() != nil {
						t.Fatal("cleanup used the canceled startup context")
					}
					order = append(order, name)

					return nil
				}
			}
			cleanup.Add(ctx, addStep("rootfs"))
			removeCalls := 0
			remove := func(ctx context.Context) error {
				if ctx.Err() != nil {
					t.Fatal("cgroup removal used the canceled startup context")
				}
				removeCalls++
				order = append(order, "remove")
				if tc.removeFails {
					return removeErr
				}

				return nil
			}
			registered := tc.registerRemoval
			if registered {
				cleanup.Add(ctx, remove)
				cleanup.Add(ctx, addStep("stats"))
			}

			var result error
			if tc.failStartup {
				result = startErr
			}
			cleanupIfNotRegistered(ctx, &result, &registered, remove)
			cleanupErr := cleanup.Run(ctx)
			if removeCalls != 1 {
				t.Fatalf("cgroup removal ran %d times, want once", removeCalls)
			}
			if !slices.Equal(order, tc.wantOrder) {
				t.Fatalf("cleanup order = %v, want %v", order, tc.wantOrder)
			}
			if tc.failStartup && !errors.Is(result, startErr) {
				t.Fatalf("startup error lost: %v", result)
			}
			if tc.removeFails && !errors.Is(errors.Join(result, cleanupErr), removeErr) {
				t.Fatalf("cgroup removal error lost: result=%v cleanup=%v", result, cleanupErr)
			}
		})
	}
}
