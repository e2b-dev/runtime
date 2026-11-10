//go:build linux

package sandbox

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNetworkReleaseRetriesOnlyFailedSubscribers(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	sbx := testMapSandbox(t, "old-lifecycle")
	ip := sbx.Slot.HostIPString()
	firstFailure := errors.New("first subscriber still owns users")
	secondFailure := errors.New("second subscriber still owns users")
	var firstCalls, secondCalls, successfulCalls, lateCalls int
	sandboxes.Subscribe(&releaseSubscriber{onRelease: func(got *Sandbox) error {
		require.Same(t, sbx, got)
		_, err := sandboxes.GetByHostPort(net.JoinHostPort(ip, "80"))
		require.Error(t, err, "routing must be removed before subscriber release")
		firstCalls++
		if firstCalls == 1 {
			return firstFailure
		}

		return nil
	}})
	sandboxes.Subscribe(&releaseSubscriber{onRelease: func(got *Sandbox) error {
		require.Same(t, sbx, got)
		secondCalls++
		if secondCalls <= 2 {
			return secondFailure
		}

		return nil
	}})
	sandboxes.Subscribe(&releaseSubscriber{onRelease: func(got *Sandbox) error {
		require.Same(t, sbx, got)
		successfulCalls++

		return nil
	}})
	sandboxes.AssignNetwork(t.Context(), sbx)
	release := sandboxes.NetworkRelease(sbx.Runtime.SandboxID, sbx.LifecycleID)

	err := release(t.Context(), ip)
	require.ErrorIs(t, err, firstFailure)
	require.ErrorIs(t, err, secondFailure, "all failed subscribers must be attempted")
	sandboxes.Subscribe(&releaseSubscriber{onRelease: func(*Sandbox) error {
		lateCalls++

		return nil
	}})
	err = release(t.Context(), ip)
	require.ErrorIs(t, err, secondFailure, "failed subscribers must remain pending after map removal")
	require.NotErrorIs(t, err, firstFailure)
	require.NoError(t, release(t.Context(), ip))
	require.NoError(t, release(t.Context(), ip))
	assert.Equal(t, 2, firstCalls, "successful retry must not replay")
	assert.Equal(t, 3, secondCalls)
	assert.Equal(t, 1, successfulCalls, "successful subscribers must not be replayed")
	assert.Zero(t, lateCalls, "retry must retain the original subscriber snapshot")
}

func TestNetworkReleaseDoesNotRemoveReplacementLifecycle(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	old := testMapSandbox(t, "old-lifecycle")
	replacement := testMapSandbox(t, "replacement-lifecycle")
	replacement.Slot = old.Slot // the exact same slot pointer has been reused
	ip := old.Slot.HostIPString()
	var notified []*Sandbox
	sandboxes.Subscribe(&releaseSubscriber{onRelease: func(sbx *Sandbox) error {
		notified = append(notified, sbx)

		return nil
	}})
	sandboxes.TrackLifecycle(t.Context(), old)
	sandboxes.AssignNetwork(t.Context(), old)
	release := sandboxes.NetworkRelease(old.Runtime.SandboxID, old.LifecycleID)
	sandboxes.AssignNetwork(t.Context(), replacement)

	require.NoError(t, release(t.Context(), ip))
	require.NoError(t, release(t.Context(), ip))
	got, err := sandboxes.GetByHostPort(net.JoinHostPort(ip, "80"))
	require.NoError(t, err, "old release must preserve the replacement route")
	require.Same(t, replacement, got)
	require.Len(t, notified, 1)
	assert.Same(t, old, notified[0], "old release must never notify replacement users")
}

func TestNetworkReleaseRetainsOldFailureAfterReplacement(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	old := testMapSandbox(t, "old-lifecycle")
	replacement := testMapSandbox(t, "replacement-lifecycle")
	replacement.Slot = old.Slot
	ip := old.Slot.HostIPString()
	boom := errors.New("connection retirement failed")
	var failedCalls, successfulCalls int
	sandboxes.Subscribe(&releaseSubscriber{onRelease: func(sbx *Sandbox) error {
		require.Same(t, old, sbx, "retry must keep its original owner after lifecycle removal")
		failedCalls++
		if failedCalls == 1 {
			return boom
		}

		return nil
	}})
	sandboxes.Subscribe(&releaseSubscriber{onRelease: func(sbx *Sandbox) error {
		require.Same(t, old, sbx)
		successfulCalls++

		return nil
	}})
	sandboxes.TrackLifecycle(t.Context(), old)
	sandboxes.AssignNetwork(t.Context(), old)
	release := sandboxes.NetworkRelease(old.Runtime.SandboxID, old.LifecycleID)
	require.ErrorIs(t, release(t.Context(), ip), boom)
	sandboxes.AssignNetwork(t.Context(), replacement)
	sandboxes.MarkStopped(t.Context(), old)

	require.NoError(t, release(t.Context(), ip))
	require.NoError(t, release(t.Context(), ip))
	got, err := sandboxes.GetByHostPort(net.JoinHostPort(ip, "80"))
	require.NoError(t, err)
	require.Same(t, replacement, got)
	assert.Equal(t, 2, failedCalls, "failed old subscriber must retry after replacement")
	assert.Equal(t, 1, successfulCalls, "successful old subscriber must not replay")
}

func TestNetworkReleaseConcurrentFailureJoinsAndCancellationDoesNotAbandon(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		sandboxes := NewSandboxesMap()
		sbx := testMapSandbox(t, "old-lifecycle")
		ip := sbx.Slot.HostIPString()
		boom := errors.New("subscriber still owns users")
		waiterFailure := errors.New("only the waiting caller canceled")
		proceed := make(chan struct{})
		var calls atomic.Int64
		sandboxes.Subscribe(&releaseSubscriber{onRelease: func(got *Sandbox) error {
			assert.Same(t, sbx, got)
			if calls.Add(1) == 1 {
				<-proceed

				return boom
			}

			return nil
		}})
		sandboxes.AssignNetwork(t.Context(), sbx)
		release := sandboxes.NetworkRelease(sbx.Runtime.SandboxID, sbx.LifecycleID)
		first := make(chan error, 1)
		second := make(chan error, 1)
		canceled := make(chan error, 1)
		go func() { first <- release(t.Context(), ip) }()
		synctest.Wait()
		go func() { second <- release(t.Context(), ip) }()
		waitCtx, cancelWait := context.WithCancelCause(t.Context())
		go func() { canceled <- release(waitCtx, ip) }()
		synctest.Wait()
		cancelWait(waiterFailure)
		synctest.Wait()
		require.ErrorIs(t, <-canceled, waiterFailure)
		assert.Equal(t, int64(1), calls.Load(), "waiters must join one physical subscriber attempt")
		select {
		case <-first:
			t.Error("waiter cancellation must not abandon the running subscriber")
		default:
		}
		close(proceed)
		synctest.Wait()
		require.ErrorIs(t, <-first, boom)
		require.ErrorIs(t, <-second, boom, "concurrent callers must observe the same failed attempt")
		assert.Equal(t, int64(1), calls.Load(), "a concurrent waiter must not implicitly retry failure")
		require.NoError(t, release(t.Context(), ip))
		require.NoError(t, release(t.Context(), ip))
		assert.Equal(t, int64(2), calls.Load())
	})
}

func TestNetworkReleaseCanceledBeforeAttemptKeepsRoute(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	sbx := testMapSandbox(t, "old-lifecycle")
	ip := sbx.Slot.HostIPString()
	calls := 0
	sandboxes.Subscribe(&releaseSubscriber{onRelease: func(*Sandbox) error {
		calls++

		return nil
	}})
	sandboxes.AssignNetwork(t.Context(), sbx)
	release := sandboxes.NetworkRelease(sbx.Runtime.SandboxID, sbx.LifecycleID)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, release(ctx, ip), context.Canceled)
	got, err := sandboxes.GetByHostPort(net.JoinHostPort(ip, "80"))
	require.NoError(t, err)
	require.Same(t, sbx, got)
	require.Zero(t, calls)
	require.NoError(t, release(t.Context(), ip))
	assert.Equal(t, 1, calls)
}

func TestNetworkReleaseRequiresIdentity(t *testing.T) {
	t.Parallel()

	for _, identity := range []struct{ sandboxID, lifecycleID string }{{"", "life"}, {"sandbox-1", ""}} {
		t.Run(identity.sandboxID+"/"+identity.lifecycleID, func(t *testing.T) {
			t.Parallel()
			sandboxes := NewSandboxesMap()
			sbx := testMapSandbox(t, "life")
			sandboxes.AssignNetwork(t.Context(), sbx)
			err := sandboxes.NetworkRelease(identity.sandboxID, identity.lifecycleID)(t.Context(), sbx.Slot.HostIPString())
			require.ErrorContains(t, err, "requires sandbox and lifecycle identity")
			got, err := sandboxes.GetByHostPort(net.JoinHostPort(sbx.Slot.HostIPString(), "80"))
			require.NoError(t, err)
			require.Same(t, sbx, got)
		})
	}
}

func TestNetworkReleaseUnpublishedOwnerCannotSelectAnotherSandbox(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	other := testMapSandbox(t, "same-life")
	other.Runtime.SandboxID = "other-sandbox"
	calls := 0
	sandboxes.Subscribe(&releaseSubscriber{onRelease: func(*Sandbox) error {
		calls++

		return nil
	}})
	release := sandboxes.NetworkRelease("unpublished-sandbox", other.LifecycleID)
	sandboxes.AssignNetwork(t.Context(), other)
	require.NoError(t, release(t.Context(), other.Slot.HostIPString()))
	require.NoError(t, release(t.Context(), other.Slot.HostIPString()))
	got, err := sandboxes.GetByHostPort(net.JoinHostPort(other.Slot.HostIPString(), "80"))
	require.NoError(t, err, "matching lifecycle alone must not select a different sandbox")
	require.Same(t, other, got)
	assert.Zero(t, calls)
}
