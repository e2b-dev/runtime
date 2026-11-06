package redis

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// otherFormAndProbe returns the form a resume moves a record to from `from`,
// and the command an operation's resolution runs on from's key, which is
// where the move is made to land: the sandbox-form GET, or the team-form
// EXISTS after it missed.
func otherFormAndProbe(from keyForm) (keyForm, string) {
	if from == formSandbox {
		return formTeam, "get"
	}

	return formSandbox, "exists"
}

// landSuccessorOnProbe adds successor in form `to` the first time an
// operation probes oldKey, that is between its resolution and its lock, and
// returns the flag that records the landing.
func landSuccessorOnProbe(t *testing.T, client goredis.UniversalClient, storage *Storage, oldKey, probe string, to keyForm, successor sandboxtypes.Sandbox) *atomic.Bool {
	t.Helper()

	fired := &atomic.Bool{}
	client.AddHook(cmdHook{process: func(ctx context.Context, cmd goredis.Cmder, next goredis.ProcessHook) error {
		err := next(ctx, cmd)
		if args := cmd.Args(); cmd.Name() == probe && len(args) > 1 {
			if key, ok := args[1].(string); ok && strings.HasSuffix(key, oldKey) && fired.CompareAndSwap(false, true) {
				storage.setWriteForm(to)
				require.NoError(t, storage.Add(ctx, successor))
			}
		}

		return err
	}})

	return fired
}

// Update and StartRemoving, having resolved a form and lost that form's lock
// to a resume of the same ID writing the other form, find their form empty
// and act on the successor in the form it moved to: an update lands on it
// and an unpinned kill starts its transition, rather than answering not
// found while the resumed sandbox runs. Both directions of the move.
func TestSuccessorMovedToOtherFormIsFollowed(t *testing.T) {
	t.Parallel()

	for _, from := range []keyForm{formTeam, formSandbox} {
		to, probe := otherFormAndProbe(from)

		t.Run(fmt.Sprintf("update from form %d", from), func(t *testing.T) {
			t.Parallel()

			client := redis_utils.SetupInstance(t)
			storage := newTestStorage(t, client)
			teamID := uuid.New()
			old := newRecord(teamID, "sbx-update-moved")
			oldKey := addRecord(t, storage, from, old)
			successor := newRecord(teamID, old.SandboxID)
			fired := landSuccessorOnProbe(t, client, storage, oldKey, probe, to, successor)

			updated, err := storage.Update(t.Context(), teamID, old.SandboxID, func(cur sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
				cur.TemplateID = "moved-and-updated"

				return cur, nil
			})

			require.NoError(t, err)
			require.True(t, fired.Load(), "the resume did not land on the probe")
			require.Equal(t, successor.ExecutionID, updated.ExecutionID)
			require.Equal(t, "moved-and-updated", updated.TemplateID)
			requireNoRecord(t, client, oldKey)
			requireRecord(t, client, recordKey(to, teamID.String(), old.SandboxID), mustJSON(t, updated))
			requireLocksFree(t, storage, teamID, old.SandboxID)
		})

		t.Run(fmt.Sprintf("kill from form %d", from), func(t *testing.T) {
			t.Parallel()

			client := redis_utils.SetupInstance(t)
			storage := newTestStorage(t, client)
			teamID := uuid.New()
			old := newRecord(teamID, "sbx-kill-moved")
			oldKey := addRecord(t, storage, from, old)
			successor := newRecord(teamID, old.SandboxID)
			fired := landSuccessorOnProbe(t, client, storage, oldKey, probe, to, successor)

			transition, done, finish, err := storage.StartRemoving(t.Context(), teamID, old.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionKill})

			require.NoError(t, err)
			require.True(t, fired.Load(), "the resume did not land on the probe")
			require.False(t, done)
			require.NotNil(t, finish)
			require.Equal(t, successor.ExecutionID, transition.Sandbox.ExecutionID)
			require.Equal(t, sandboxtypes.StateKilling, transition.Sandbox.State)
			requireNoRecord(t, client, oldKey)
			marker, err := client.Get(t.Context(), transitionKeyFor(to, teamID.String(), old.SandboxID)).Result()
			require.NoError(t, err)
			require.NotEmpty(t, marker, "the transition started under the form the record moved to")
			requireLocksFree(t, storage, teamID, old.SandboxID)
		})
	}
}

// A refused pause's restore, having resolved a form and lost that form's
// lock to a resume of the same ID writing the other form, finds its form
// empty and reports the successor as a mismatch, which its caller treats as
// superseded, rather than as a sandbox gone, which would have the resumed
// sandbox killed on its node by ID. Both directions of the move.
func TestRestoreJudgesSuccessorMovedToOtherForm(t *testing.T) {
	t.Parallel()

	for _, from := range []keyForm{formTeam, formSandbox} {
		to, probe := otherFormAndProbe(from)

		t.Run(fmt.Sprintf("restore from form %d", from), func(t *testing.T) {
			t.Parallel()

			client := redis_utils.SetupInstance(t)
			storage := newTestStorage(t, client)
			teamID := uuid.New()
			old := newRecord(teamID, "sbx-restore-moved")
			oldKey := addRecord(t, storage, from, old)
			transition, done, _, err := storage.StartRemoving(t.Context(), teamID, old.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
			require.NoError(t, err)
			require.False(t, done)
			successor := newRecord(teamID, old.SandboxID)
			successorKey := recordKey(to, teamID.String(), old.SandboxID)
			fired := landSuccessorOnProbe(t, client, storage, oldKey, probe, to, successor)

			_, err = storage.RestoreRunning(t.Context(), transition, sandboxtypes.RestoreOpts{})

			require.ErrorIs(t, err, sandboxtypes.ErrExecutionMismatch)
			require.True(t, fired.Load(), "the resume did not land on the probe")
			requireNoRecord(t, client, oldKey)
			requireRecord(t, client, successorKey, mustJSON(t, successor))
			requireLocksFree(t, storage, teamID, old.SandboxID)
		})
	}
}
