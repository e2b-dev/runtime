package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// Get reads a record in either form, the sandbox form first, and counts a
// read the team form served after the sandbox form missed; a sandbox in
// neither form is not found.
func TestGetReadsBothForms(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage, reader := newMeteredTestStorage(t, client)
	teamID := uuid.New()
	fallbacks := func() int64 { return counterTotal(t, reader, string(telemetry.ApiRedisStorageReadsFallback)) }

	sandboxForm := newRecord(teamID, "sbx-sandbox")
	addRecord(t, storage, formSandbox, sandboxForm)
	got, err := storage.Get(t.Context(), teamID, sandboxForm.SandboxID)
	require.NoError(t, err)
	require.Equal(t, mustJSON(t, sandboxForm), mustJSON(t, got))
	require.Zero(t, fallbacks(), "a sandbox-form hit is not a fallback")

	teamForm := newRecord(teamID, "sbx-team")
	addRecord(t, storage, formTeam, teamForm)
	got, err = storage.Get(t.Context(), teamID, teamForm.SandboxID)
	require.NoError(t, err)
	require.Equal(t, mustJSON(t, teamForm), mustJSON(t, got))
	require.Equal(t, int64(1), fallbacks(), "a team-form hit after the sandbox-form miss is a fallback")

	_, err = storage.Get(t.Context(), teamID, "sbx-none")
	require.ErrorIs(t, err, sandboxtypes.ErrNotFound)
	require.Equal(t, int64(1), fallbacks(), "a miss in both forms is not a fallback")
}

// Update reads only the form it resolved and locked. A team-form record
// that appears after the resolution, as a replica of the release that still
// writes that form resumes the sandbox under both form locks before Update
// took the sandbox form's: Update finds its form empty, releases that lock,
// resolves once more and updates the record under the team form's lock,
// never inside the lock it first held.
func TestUpdateFollowsALateRecordUnderItsOwnLock(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()
	sbx := newRecord(teamID, "sbx-late")
	key := recordKey(formTeam, teamID.String(), sbx.SandboxID)
	teamLock, sandboxLock := lockKeys(teamID, sbx.SandboxID)
	storage.SetFaultHook(func(p faultPoint) error {
		if p == faultUpdateAfterResolve {
			writeRecord(t, client, key, "", sbx)
		}

		return nil
	})

	runs := 0
	updated, err := storage.Update(t.Context(), teamID, sbx.SandboxID, func(cur sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
		runs++
		require.True(t, lockHeld(t, storage.locker, teamLock), "the closure ran without the record's lock")
		require.False(t, lockHeld(t, storage.locker, sandboxLock), "the closure ran inside the lock of the empty form")
		cur.TemplateID = "late"

		return cur, nil
	})

	require.NoError(t, err)
	require.Equal(t, 1, runs)
	require.Equal(t, "late", updated.TemplateID)
	requireRecord(t, client, key, mustJSON(t, updated))
	requireLocksFree(t, storage, teamID, sbx.SandboxID)
}

// A record in either form is updated under the lock of the form it is in,
// that form's only, in place and with its member rescored.
func TestUpdateLocksRecordForm(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	teamID := uuid.New()

	for _, form := range []keyForm{formTeam, formSandbox} {
		sbx := newRecord(teamID, fmt.Sprintf("sbx-%d", form))
		key := addRecord(t, storage, form, sbx)
		otherKey := recordKey(form.other(), teamID.String(), sbx.SandboxID)

		updated, err := storage.Update(t.Context(), teamID, sbx.SandboxID, func(cur sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
			require.True(t, lockHeld(t, storage.locker, redis_utils.GetLockKey(key)), "the record form's lock is not held")
			require.False(t, lockHeld(t, storage.locker, redis_utils.GetLockKey(otherKey)), "the other form's lock is held")
			cur.EndTime = cur.EndTime.Add(time.Hour)

			return cur, nil
		})
		require.NoError(t, err)

		requireRecord(t, client, key, mustJSON(t, updated))
		requireNoRecord(t, client, otherKey)
		requireMemberScore(t, client, sandboxExpirationMember(updated), float64(updated.EndTime.UnixMilli()))
		require.False(t, lockHeld(t, storage.locker, redis_utils.GetLockKey(key)))
	}
}
