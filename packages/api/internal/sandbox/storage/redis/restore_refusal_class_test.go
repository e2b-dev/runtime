package redis

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
)

// A restored pause tells its waiters which refusal it was: a joining pause is
// answered with the refusal the starter got, and a plain waiter still reads a
// restore. Without the class every joiner read a busy node.
func TestStartRemoving_JoinerIsAnsweredWithTheRestoredRefusalsClass(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)
	for name, refusal := range map[string]error{
		"checkpoint": sandboxtypes.PauseRefusedCheckpointInProgressError{},
		"envd":       sandboxtypes.PauseRefusedEnvdUnresponsiveError{},
		"busy":       sandboxtypes.PauseQueueExhaustedError{},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sbx := createTestSandbox("joiner-class-" + name)
			require.NoError(t, storage.Add(t.Context(), sbx))
			transition, alreadyDone, finish, err := storage.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
			require.NoError(t, err)
			require.False(t, alreadyDone)

			joiner := make(chan error, 1)
			waiter := make(chan error, 1)
			go func() {
				_, _, _, err := storage.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
				joiner <- err
			}()
			go func() { waiter <- storage.WaitForStateChange(t.Context(), sbx.TeamID, sbx.SandboxID) }()
			id := pendingTransitionID(t, storage.redisClient, formTeam, sbx.TeamID, sbx.SandboxID)
			routingKey := transitionRoutingKey(transitionResultKeyFor(formTeam, sbx.TeamID.String(), sbx.SandboxID, id))
			require.Eventually(t, func() bool { return waitersOn(storage, routingKey) == 2 },
				5*time.Second, 5*time.Millisecond, "both waiters must be on the transition before it is finished")

			_, err = storage.RestoreRunning(t.Context(), transition, sandboxtypes.RestoreOpts{RetryAfter: 10 * time.Second})
			require.NoError(t, err)
			finish(t.Context(), &sandboxtypes.RestoredTransitionError{Refusal: refusal})

			select {
			case err := <-joiner:
				require.ErrorIs(t, err, refusal, "the joiner must get the starter's refusal")
				require.ErrorIs(t, err, sandboxtypes.PauseQueueExhaustedError{})
				if name != "checkpoint" {
					require.NotErrorIs(t, err, sandboxtypes.PauseRefusedCheckpointInProgressError{})
				}
				if name != "envd" {
					require.NotErrorIs(t, err, sandboxtypes.PauseRefusedEnvdUnresponsiveError{})
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the joiner never returned")
			}
			select {
			case err := <-waiter:
				require.ErrorIs(t, err, sandboxtypes.ErrTransitionRestored)
				require.ErrorIs(t, err, refusal)
			case <-time.After(5 * time.Second):
				t.Fatal("the waiter never returned")
			}
		})
	}
}

// waitersOn counts the waiters subscribed to a transition's routing key.
func waitersOn(storage *Storage, routingKey string) int {
	storage.subManager.mu.RLock()
	defer storage.subManager.mu.RUnlock()

	return len(storage.subManager.waiters[routingKey])
}

// A hold-only restore sets the retry window and nothing else: no refusal
// episode opens, and a live one ends, so the next refusal's budget counts
// from that refusal. A charged refusal keeps the live episode.
func TestRestoreRunning_HoldOnlyEndsTheEpisode(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)
	for name, holdOnly := range map[string]bool{"hold only": true, "charged": false} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			episodeStart := time.Now().Add(-90 * time.Second)
			sbx := createTestSandbox("restore-hold-" + name)
			sbx.RefusedSince = episodeStart
			sbx.RefusedUntil = time.Now().Add(-time.Second)
			require.NoError(t, storage.Add(t.Context(), sbx))
			transition, _, finish, err := storage.StartRemoving(t.Context(), sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
			require.NoError(t, err)
			defer finish(context.WithoutCancel(t.Context()), sandboxtypes.ErrTransitionRestored)

			restored, err := storage.RestoreRunning(t.Context(), transition, sandboxtypes.RestoreOpts{RetryAfter: 10 * time.Second, HoldOnly: holdOnly})
			require.NoError(t, err)
			stored, err := storage.Get(t.Context(), sbx.TeamID, sbx.SandboxID)
			require.NoError(t, err)
			for _, got := range []sandboxtypes.Sandbox{restored, stored} {
				assert.WithinDuration(t, time.Now().Add(10*time.Second), got.RefusedUntil, time.Second, "the sweep is held either way")
				if holdOnly {
					assert.True(t, got.RefusedSince.IsZero(), "the hold ends the episode it found")
				} else {
					assert.WithinDuration(t, episodeStart, got.RefusedSince, time.Millisecond, "the live episode goes on")
				}
			}
		})
	}
}

// A guest-agent refusal is restored hold-only, so the node's episode stays
// empty; the restore stamps the guest agent's own episode instead, which the
// evictor's bypass grace is measured against. A second envd refusal within
// the gap keeps the first stamp, and a restore for any other refusal leaves
// it as it is.
func TestRestoreRunning_EnvdRefusalStampsItsOwnEpisode(t *testing.T) {
	t.Parallel()

	storage, _ := setupTestStorage(t)
	ctx := t.Context()

	sbx := createTestSandbox("restore-envd-episode")
	require.NoError(t, storage.Add(ctx, sbx))

	restore := func(opts sandboxtypes.RestoreOpts) sandboxtypes.Sandbox {
		t.Helper()

		transition, _, finish, err := storage.StartRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
		require.NoError(t, err)
		restored, err := storage.RestoreRunning(ctx, transition, opts)
		require.NoError(t, err)
		finish(ctx, sandboxtypes.ErrTransitionRestored)
		stored, err := storage.Get(ctx, sbx.TeamID, sbx.SandboxID)
		require.NoError(t, err)
		assert.True(t, restored.EnvdRefusedSince.Equal(stored.EnvdRefusedSince), "the returned record is the stored one")

		return stored
	}

	first := restore(sandboxtypes.RestoreOpts{RetryAfter: 10 * time.Second, HoldOnly: true, EnvdRefusal: true})
	assert.True(t, first.RefusedSince.IsZero(), "a hold-only restore opens no node episode")
	assert.WithinDuration(t, time.Now(), first.EnvdRefusedSince, time.Second, "the guest agent's own episode opens at the first envd refusal")
	assert.WithinDuration(t, time.Now().Add(10*time.Second), first.RefusedUntil, time.Second, "the sweep is held for the retry window")
	assert.True(t, first.EnvdRefusedUntil.Equal(first.RefusedUntil), "and envd's own window is the same stamp")

	again := restore(sandboxtypes.RestoreOpts{RetryAfter: 10 * time.Second, HoldOnly: true, EnvdRefusal: true})
	assert.True(t, again.EnvdRefusedSince.Equal(first.EnvdRefusedSince), "a second envd refusal within the gap keeps the first stamp")
	assert.True(t, again.RefusedSince.IsZero())

	other := restore(sandboxtypes.RestoreOpts{RetryAfter: 10 * time.Second})
	assert.True(t, other.EnvdRefusedSince.Equal(first.EnvdRefusedSince), "a restore for another refusal leaves the guest agent's episode untouched")
	assert.True(t, other.EnvdRefusedUntil.Equal(again.EnvdRefusedUntil), "and does not extend envd's window, so only envd refusals keep that episode alive")
	assert.WithinDuration(t, time.Now(), other.RefusedSince, time.Second, "and opens the node's")
}
