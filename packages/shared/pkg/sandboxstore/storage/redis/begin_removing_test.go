package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

const hangGuard = 10 * time.Second

func TestBeginRemoving_TransitionInFlight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		inFlight sandboxtypes.StateAction
		opts     sandboxtypes.RemoveOpts
		refused  bool
	}{
		{"same target", sandboxtypes.StateActionPause, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause}, false},
		{"allowed other target", sandboxtypes.StateActionPause, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionKill}, false},
		{"disallowed target", sandboxtypes.StateActionKill, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause}, true},
		{"strict pause on a pause", sandboxtypes.StateActionPause, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause, RequireEnvdHealthy: true}, true},
	}

	storage, client := setupTestStorage(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sbx := createTestSandbox("begin-in-flight-" + tt.name)
			require.NoError(t, storage.Add(t.Context(), sbx))
			open, _, finish, err := storage.startRemovingWithFinish(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: tt.inFlight})
			require.NoError(t, err)
			require.NotEmpty(t, open.ID)
			defer finish(context.WithoutCancel(t.Context()), nil)

			ctx, cancel := context.WithTimeout(t.Context(), hangGuard)
			defer cancel()
			transition, alreadyDone, err := storage.BeginRemoving(ctx, sbx.TeamID, sbx.SandboxID, tt.opts)

			assert.False(t, alreadyDone)
			assert.Empty(t, transition.ID, "no transition is started")
			assert.Equal(t, tt.inFlight.TargetState, transition.Sandbox.State)
			if tt.refused {
				var transErr *sandboxtypes.InvalidStateTransitionError
				require.ErrorAs(t, err, &transErr)
				assert.Equal(t, tt.inFlight.TargetState, transErr.CurrentState)
				assert.Equal(t, tt.opts.Action.TargetState, transErr.TargetState)
				assert.Empty(t, transition.InFlightID)
			} else {
				require.NoError(t, err, "the open transition is never finished, so a call that waited ends on its context")
				assert.Equal(t, open.ID, transition.InFlightID)
			}

			assert.Equal(t, open.ID, pendingTransitionID(t, client, sbx.SandboxID), "the open transition is untouched")
			held, err := client.Exists(t.Context(), redis_utils.GetLockKey(recordKey(sbx.SandboxID))).Result()
			require.NoError(t, err)
			assert.Zero(t, held, "the record's lock is released")
		})
	}
}

func TestWaitForTransition_ResumesWithTheID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		workErr error
		check   func(t *testing.T, err error)
	}{
		{"success", nil, func(t *testing.T, err error) {
			t.Helper()
			require.NoError(t, err)
		}},
		{"failed", errors.New("boom"), func(t *testing.T, err error) {
			t.Helper()
			require.EqualError(t, err, "transition failed: boom")
		}},
		{"restored", &sandboxtypes.RestoredTransitionError{Refusal: sandboxtypes.PauseUnsupportedError{}}, func(t *testing.T, err error) {
			t.Helper()
			var restored *sandboxtypes.RestoredTransitionError
			require.ErrorAs(t, err, &restored)
			assert.Equal(t, sandboxtypes.PauseUnsupportedError{}, restored.Refusal)
		}},
	}

	storage, _ := setupTestStorage(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sbx := createTestSandbox("wait-resume-" + tt.name)
			require.NoError(t, storage.Add(t.Context(), sbx))
			transition, _, finish, err := storage.startRemovingWithFinish(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			id, err := storage.WaitForTransition(ctx, sbx.TeamID, sbx.SandboxID, transition.ID)
			require.ErrorIs(t, err, context.DeadlineExceeded, "the transition is never finished, so only the context ends the wait")
			require.Equal(t, transition.ID, id)

			finish(t.Context(), tt.workErr)

			id, err = storage.WaitForTransition(t.Context(), sbx.TeamID, sbx.SandboxID, transition.ID)
			assert.Equal(t, transition.ID, id)
			tt.check(t, err)
		})
	}
}

func TestWaitForTransition_EmptyID(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)

	t.Run("no open transition", func(t *testing.T) {
		t.Parallel()

		sbx := createTestSandbox("wait-empty-none")
		require.NoError(t, storage.Add(t.Context(), sbx))

		id, err := storage.WaitForTransition(t.Context(), sbx.TeamID, sbx.SandboxID, "")
		require.NoError(t, err)
		assert.Empty(t, id)
	})

	t.Run("open transition", func(t *testing.T) {
		t.Parallel()

		sbx := createTestSandbox("wait-empty-open")
		require.NoError(t, storage.Add(t.Context(), sbx))
		transition, _, finish, err := storage.startRemovingWithFinish(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
		require.NoError(t, err)

		type result struct {
			id  string
			err error
		}
		waiter := make(chan result, 1)
		go func() {
			id, err := storage.WaitForTransition(t.Context(), sbx.TeamID, sbx.SandboxID, "")
			waiter <- result{id, err}
		}()
		waitForTransitionWaiters(t, storage, client, sbx, 1)
		finish(t.Context(), errors.New("boom"))

		select {
		case got := <-waiter:
			assert.Equal(t, transition.ID, got.id)
			require.EqualError(t, got.err, "transition failed: boom")
		case <-time.After(hangGuard):
			require.Fail(t, "the waiter never returned")
		}
	})
}

func TestWaitForTransition_OtherTeam(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)
	sbx := createTestSandbox("wait-other-team")
	require.NoError(t, storage.Add(t.Context(), sbx))
	transition, _, finish, err := storage.startRemovingWithFinish(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)
	defer finish(context.WithoutCancel(t.Context()), nil)

	for _, transitionID := range []string{transition.ID, ""} {
		id, err := storage.WaitForTransition(t.Context(), uuid.New(), sbx.SandboxID, transitionID)
		require.ErrorIs(t, err, sandboxtypes.ErrNotFound)
		assert.Equal(t, transitionID, id)
	}
}

func TestWaitForTransition_UnknownIDIsASuccess(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)
	sbx := createTestSandbox("wait-unknown-id")
	require.NoError(t, storage.Add(t.Context(), sbx))

	unknown := uuid.NewString()
	id, err := storage.WaitForTransition(t.Context(), sbx.TeamID, sbx.SandboxID, unknown)
	require.NoError(t, err)
	assert.Equal(t, unknown, id)
}

func TestWaitForTransition_FinishOnAnotherInstance(t *testing.T) {
	t.Parallel()

	finisher, client := setupTestStorage(t)
	waiting := newTestStorage(t, client)
	startNotifications(t, waiting)

	sbx := createTestSandbox("wait-cross-instance")
	require.NoError(t, finisher.Add(t.Context(), sbx))
	transition, _, finish, err := finisher.startRemovingWithFinish(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)

	waiter := make(chan error, 1)
	go func() {
		_, err := waiting.WaitForTransition(t.Context(), sbx.TeamID, sbx.SandboxID, transition.ID)
		waiter <- err
	}()
	waitForTransitionWaiters(t, waiting, client, sbx, 1)
	finish(t.Context(), errors.New("boom"))

	select {
	case err := <-waiter:
		require.EqualError(t, err, "transition failed: boom")
	case <-time.After(hangGuard):
		require.Fail(t, "the waiter never returned")
	}
}

func TestWaitForTransition_FailureIsTyped(t *testing.T) {
	t.Parallel()

	storage, client := setupTestStorage(t)
	sbx := createTestSandbox("wait-failure-typed")
	require.NoError(t, storage.Add(t.Context(), sbx))
	transition, _, finish, err := storage.startRemovingWithFinish(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)

	joiner := make(chan error, 1)
	go func() {
		_, _, err := sandboxtypes.StartRemoving(t.Context(), storage, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
		joiner <- err
	}()
	waitForTransitionWaiters(t, storage, client, sbx, 1)
	finish(t.Context(), errors.New("boom"))

	var joinErr error
	select {
	case joinErr = <-joiner:
	case <-time.After(hangGuard):
		require.Fail(t, "the joiner never returned")
	}
	require.EqualError(t, joinErr, "failed waiting for transition: transition failed: boom", "the text is unchanged through the helper's wrap")
	failed, ok := errors.AsType[*sandboxtypes.TransitionFailedError](joinErr)
	require.True(t, ok, "the failure is typed through the helper's wrap")
	assert.Equal(t, "boom", failed.Message)

	_, err = storage.WaitForTransition(t.Context(), sbx.TeamID, sbx.SandboxID, transition.ID)
	require.EqualError(t, err, "transition failed: boom", "the text is unchanged")
	failed, ok = errors.AsType[*sandboxtypes.TransitionFailedError](err)
	require.True(t, ok)
	assert.Equal(t, "boom", failed.Message)
}
