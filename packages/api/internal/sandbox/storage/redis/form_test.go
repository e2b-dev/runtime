package redis

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// sandboxKeys returns every per-sandbox key of one form: the record, its
// lock, the transition marker, a transition result and its refusal class.
func sandboxKeys(form keyForm, teamID, sandboxID, transitionID string) []string {
	record := recordKey(form, teamID, sandboxID)
	result := transitionResultKeyFor(form, teamID, sandboxID, transitionID)

	return []string{
		record,
		redis_utils.GetLockKey(record),
		transitionKeyFor(form, teamID, sandboxID),
		result,
		refusalClassKey(result),
	}
}

// Every key of both forms carries exactly one hash tag. A sandbox's
// sandbox-form keys share a slot of their own, so the scripts and the MGET
// that span them stay single-slot; its team-form keys share the team's slot
// with the team index and with every other sandbox of the team.
func TestRecordKeyFormsHaveOneTagEach(t *testing.T) {
	t.Parallel()

	const team, sandboxA, sandboxB, transition = "team-a", "sbx-a", "sbx-b", "tx-1"

	for _, form := range []keyForm{formTeam, formSandbox} {
		for _, key := range sandboxKeys(form, team, sandboxA, transition) {
			open, closing := strings.Index(key, "{"), strings.Index(key, "}")
			assert.Equal(t, 1, strings.Count(key, "{"), key)
			assert.Equal(t, 1, strings.Count(key, "}"), key)
			assert.Less(t, open+1, closing, "empty or reversed tag in %q", key)
		}
	}

	sandboxForm := sandboxKeys(formSandbox, team, sandboxA, transition)
	assert.True(t, redis_utils.AllSameSlot(sandboxForm...), "sandbox-form keys span slots: %q", sandboxForm)
	assert.NotEqual(t,
		redis_utils.HashSlot(recordKey(formSandbox, team, sandboxA)),
		redis_utils.HashSlot(recordKey(formSandbox, team, sandboxB)),
		"two sandboxes' sandbox-form records share a slot")

	teamForm := append(sandboxKeys(formTeam, team, sandboxA, transition), GetSandboxStorageTeamIndexKey(team))
	assert.True(t, redis_utils.AllSameSlot(teamForm...), "team-form keys span slots: %q", teamForm)
	assert.Equal(t,
		redis_utils.HashSlot(recordKey(formTeam, team, sandboxA)),
		redis_utils.HashSlot(recordKey(formTeam, team, sandboxB)),
		"two sandboxes' team-form records are in different slots")
}

// An empty ID would make Redis hash the whole key, and a brace would move or
// end the tag; either splits a sandbox's keys across slots, so the builders
// refuse such IDs instead of producing a key.
func TestBuildersRejectBadIDs(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{"", "a{b", "a}b"} {
		assert.Panics(t, func() { sandboxPrefix(bad) }, "sandboxPrefix(%q)", bad)

		for _, form := range []keyForm{formTeam, formSandbox} {
			assert.Panics(t, func() { recordKey(form, bad, "sbx") }, "recordKey(%d, team %q)", form, bad)
			assert.Panics(t, func() { recordKey(form, "team", bad) }, "recordKey(%d, sandbox %q)", form, bad)
			assert.Panics(t, func() { transitionKeyFor(form, bad, "sbx") }, "transitionKeyFor(%d, team %q)", form, bad)
			assert.Panics(t, func() { transitionKeyFor(form, "team", bad) }, "transitionKeyFor(%d, sandbox %q)", form, bad)
			assert.Panics(t, func() { transitionResultKeyFor(form, bad, "sbx", "tx") }, "transitionResultKeyFor(%d, team %q)", form, bad)
			assert.Panics(t, func() { transitionResultKeyFor(form, "team", bad, "tx") }, "transitionResultKeyFor(%d, sandbox %q)", form, bad)
		}
	}
}

// The form of a sandbox is the form of the record that exists, the sandbox
// form when both do; only a sandbox with no record takes the sandbox form,
// which new records are written in.
func TestResolveForm(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	ctx := t.Context()
	teamID := uuid.New()
	team := teamID.String()

	// existing is the form formOfExisting reports, 0 for no record.
	records := []struct {
		name     string
		written  []keyForm
		existing keyForm
	}{
		{"none", nil, 0},
		{"team", []keyForm{formTeam}, formTeam},
		{"sandbox", []keyForm{formSandbox}, formSandbox},
		{"both", []keyForm{formTeam, formSandbox}, formSandbox},
	}
	for _, tc := range records {
		sandboxID := "sbx-" + tc.name
		for _, form := range tc.written {
			require.NoError(t, client.Set(ctx, recordKey(form, team, sandboxID), mustJSON(t, newRecord(teamID, sandboxID)), 0).Err())
		}
		existing, exists, err := storage.formOfExisting(ctx, team, sandboxID)
		require.NoError(t, err)
		assert.Equal(t, tc.existing != 0, exists, tc.name)
		assert.Equal(t, tc.existing, existing, tc.name)

		want := tc.existing
		if want == 0 {
			want = formSandbox
		}
		resolved, err := storage.resolveForm(ctx, team, sandboxID)
		require.NoError(t, err)
		assert.Equal(t, want, resolved, tc.name)
	}
}

// A sandbox-form record of another team is not found by the probe, so no
// caller takes the owner's lock for it: while the owner's lock is held, Get,
// Update, StartRemoving and Remove by another team answer as for an absent
// sandbox instead of waiting for the lock, run no closure, start no
// transition, and touch neither the record, the owner's index entry and
// expiration member, nor the caller's index. The owner reads the record. A
// team-form record under the caller's key that names another team is not
// found either.
func TestOtherTeamIsRefused(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	storage := newTestStorage(t, client)
	owner, caller := uuid.New(), uuid.New()
	sbx := newRecord(owner, "sbx-held")
	key := addRecord(t, storage, formSandbox, sbx)
	callerTeamLock, _ := lockKeys(caller, sbx.SandboxID)

	_, exists, err := storage.formOfExisting(t.Context(), caller.String(), sbx.SandboxID)
	require.ErrorIs(t, err, sandboxtypes.ErrNotFound)
	assert.False(t, exists)
	_, err = storage.resolveForm(t.Context(), caller.String(), sbx.SandboxID)
	require.ErrorIs(t, err, sandboxtypes.ErrNotFound)

	// The held lock is the sandbox-form one, which carries no team: a call
	// that reached it would wait out the context and fail with another
	// error than the guard's.
	held, err := storage.locker.Obtain(t.Context(), redis_utils.GetLockKey(key), time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, held.Release(context.WithoutCancel(t.Context()))) })
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	for _, tc := range []struct {
		name string
		call func() error
		want error
	}{
		{"Get", func() error {
			_, err := storage.Get(ctx, caller, sbx.SandboxID)

			return err
		}, sandboxtypes.ErrNotFound},
		{"Update", func() error {
			_, err := storage.Update(ctx, caller, sbx.SandboxID, func(cur sandboxtypes.Sandbox) (sandboxtypes.Sandbox, error) {
				require.FailNow(t, "Update ran the closure on another team's record")

				return cur, nil
			})

			return err
		}, sandboxtypes.ErrNotFound},
		{"StartRemoving", func() error {
			_, alreadyDone, finish, err := storage.StartRemoving(ctx, caller, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
			assert.False(t, alreadyDone)
			assert.Nil(t, finish)

			return err
		}, sandboxtypes.ErrNotFound},
		{"Remove", func() error { return storage.Remove(ctx, caller, sbx.SandboxID, "") }, nil},
		{"Remove of the execution", func() error { return storage.Remove(ctx, caller, sbx.SandboxID, sbx.ExecutionID) }, nil},
	} {
		if tc.want == nil {
			require.NoError(t, tc.call(), tc.name)
		} else {
			require.ErrorIs(t, tc.call(), tc.want, tc.name)
		}
		requireRecord(t, client, key, mustJSON(t, sbx))
		requireIndexed(t, client, GetSandboxStorageTeamIndexKey(owner.String()), sbx.SandboxID, true)
		requireIndexed(t, client, GetSandboxStorageTeamIndexKey(caller.String()), sbx.SandboxID, false)
		requireMemberScore(t, client, sandboxExpirationMember(sbx), float64(sbx.EndTime.UnixMilli()))
		requireNoRecord(t, client, transitionKeyFor(formSandbox, caller.String(), sbx.SandboxID))
		requireNoRecord(t, client, transitionKeyFor(formTeam, caller.String(), sbx.SandboxID))
		require.False(t, lockHeld(t, storage.locker, callerTeamLock), tc.name)
	}

	got, err := storage.Get(t.Context(), owner, sbx.SandboxID)
	require.NoError(t, err)
	require.Equal(t, mustJSON(t, sbx), mustJSON(t, got))

	teamForm := newRecord(owner, "sbx-team-form")
	writeRecord(t, client, recordKey(formTeam, caller.String(), teamForm.SandboxID), "", teamForm)
	_, err = storage.Get(t.Context(), caller, teamForm.SandboxID)
	require.ErrorIs(t, err, sandboxtypes.ErrNotFound)
}
