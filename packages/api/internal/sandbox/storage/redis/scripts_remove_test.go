package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// removeReply is a remove script's reply as parseRemoveReply reads it.
type removeReply struct {
	deleted bool
	json    string
	absent  bool
}

func removed(data string) removeReply { return removeReply{deleted: true, json: data} }
func refused(data string) removeReply { return removeReply{json: data} }

var absentReply = removeReply{absent: true}

func readRemoveReply(cmd *redis.Cmd) (removeReply, error) {
	deleted, data, isAbsent, err := parseRemoveReply(cmd)

	return removeReply{deleted: deleted, json: data, absent: isAbsent}, err
}

func requireRemoveReply(t *testing.T, cmd *redis.Cmd, want removeReply) {
	t.Helper()

	got, err := readRemoveReply(cmd)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

// writeRecord stores sbx under key as the store writes it, and its member in
// index unless index is empty. It returns the JSON written.
func writeRecord(t *testing.T, client redis.UniversalClient, key, index string, sbx sandboxtypes.Sandbox) string {
	t.Helper()

	data, err := json.Marshal(sbx)
	require.NoError(t, err)
	require.NoError(t, client.Set(t.Context(), key, data, 0).Err())
	if index != "" {
		require.NoError(t, client.SAdd(t.Context(), index, sbx.SandboxID).Err())
	}

	return string(data)
}

func requireRecord(t *testing.T, client redis.UniversalClient, key, want string) {
	t.Helper()

	got, err := client.Get(t.Context(), key).Result()
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func requireNoRecord(t *testing.T, client redis.UniversalClient, key string) {
	t.Helper()

	require.ErrorIs(t, client.Get(t.Context(), key).Err(), redis.Nil)
}

func requireIndexed(t *testing.T, client redis.UniversalClient, index, sandboxID string, want bool) {
	t.Helper()

	got, err := client.SIsMember(t.Context(), index, sandboxID).Result()
	require.NoError(t, err)
	assert.Equal(t, want, got, "index entry of %s", sandboxID)
}

func newRecord(teamID uuid.UUID, sandboxID string) sandboxtypes.Sandbox {
	return makeIndexedSandbox(teamID, sandboxID, uuid.NewString(), time.Now(), time.Now().Add(time.Hour))
}

// testLockValue is the lock value lockRecord writes, which a remove script
// run through removeScripts presents.
const testLockValue = "test-lock"

// lockRecord holds key's lock with testLockValue, as a caller of the remove
// scripts holds it.
func lockRecord(t *testing.T, client redis.UniversalClient, key string) {
	t.Helper()

	require.NoError(t, client.Set(t.Context(), redis_utils.GetLockKey(key), testLockValue, 0).Err())
}

// removeScript is the remove script, run on a sandbox of the team under the
// lock lockRecord holds, as the hold holdID when one is named.
type removeScript struct {
	run func(ctx context.Context, sandboxID, expected string) *redis.Cmd
	as  func(ctx context.Context, sandboxID, expected, holdID, lockValue string) *redis.Cmd
}

// newRemoveScript returns the remove script's call: it removes the record
// alone, its index entry living in another slot.
func newRemoveScript(client redis.UniversalClient) removeScript {
	as := func(ctx context.Context, sandboxID, expected, holdID, lockValue string) *redis.Cmd {
		key := recordKey(sandboxID)

		return RunScript(ctx, client, "remove_record", removeSandboxScript, []string{key, goneKeyFor(key), redis_utils.GetLockKey(key)}, sandboxID, expected, 60, holdID, lockValue)
	}

	return removeScript{
		run: func(ctx context.Context, sandboxID, expected string) *redis.Cmd {
			return as(ctx, sandboxID, expected, "", testLockValue)
		},
		as: as,
	}
}

// The remove script removes the record for the execution the caller names,
// or for any execution when it names none, and returns what it removed. For
// another execution it returns the record it found and writes nothing. An
// absent record is nil, and nothing is written.
func TestRemoveScriptGuards(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	teamID := uuid.New()
	script := newRemoveScript(client)
	write := func(name string) (sbx sandboxtypes.Sandbox, key, data string) {
		t.Helper()

		sbx = newRecord(teamID, "sbx-"+name)
		key = recordKey(sbx.SandboxID)
		lockRecord(t, client, key)

		return sbx, key, writeRecord(t, client, key, "", sbx)
	}

	sbx, key, data := write("match")
	requireRemoveReply(t, script.run(t.Context(), sbx.SandboxID, sbx.ExecutionID), removed(data))
	requireNoRecord(t, client, key)

	sbx, key, data = write("other")
	requireRemoveReply(t, script.run(t.Context(), sbx.SandboxID, uuid.NewString()), refused(data))
	requireRecord(t, client, key, data)

	sbx, key, data = write("unguarded")
	requireRemoveReply(t, script.run(t.Context(), sbx.SandboxID, ""), removed(data))
	requireNoRecord(t, client, key)

	requireRemoveReply(t, script.run(t.Context(), "sbx-absent", ""), absentReply)
	requireRemoveReply(t, script.run(t.Context(), "sbx-absent", uuid.NewString()), absentReply)
}

// The remove script re-executed after a lost reply finds its record gone,
// or finds a successor of the same sandbox ID and leaves it whole.
func TestRemoveScriptReexecutionIsNoop(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	teamID := uuid.New()
	script := newRemoveScript(client)
	sandboxID := "sbx-reexecuted"
	key := recordKey(sandboxID)

	lockRecord(t, client, key)
	sbx := newRecord(teamID, sandboxID)
	data := writeRecord(t, client, key, "", sbx)
	requireRemoveReply(t, script.run(t.Context(), sandboxID, sbx.ExecutionID), removed(data))
	requireRemoveReply(t, script.run(t.Context(), sandboxID, sbx.ExecutionID), absentReply)

	// A successor: refused, and left whole.
	successor := newRecord(teamID, sandboxID)
	successorData := writeRecord(t, client, key, "", successor)
	requireRemoveReply(t, script.run(t.Context(), sandboxID, sbx.ExecutionID), refused(successorData))
	requireRecord(t, client, key, successorData)
	requireRemoveReply(t, script.run(t.Context(), sandboxID, successor.ExecutionID), removed(successorData))
}

// The remove script writes nothing once the record's lock no longer holds
// the caller's value, and, run as a hold, removes only a record under that
// live hold: one whose hold lapsed, or that nobody holds, is left whole, as
// is one under another operation's hold.
func TestRemoveScriptRequiresTheLockAndTheLiveHold(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	teamID := uuid.New()
	script := newRemoveScript(client)
	write := func(name, holdID string, until time.Time) (sbx sandboxtypes.Sandbox, key, data string) {
		t.Helper()

		sbx = newRecord(teamID, "sbx-"+name)
		sbx.LifecycleHold, sbx.LifecycleHoldUntil = holdID, until.UnixMilli()
		key = recordKey(sbx.SandboxID)
		lockRecord(t, client, key)

		return sbx, key, writeRecord(t, client, key, "", sbx)
	}

	sbx, key, data := write("lock-lost", "", time.Time{})
	_, err := readRemoveReply(script.as(t.Context(), sbx.SandboxID, sbx.ExecutionID, "", "another-lock"))
	require.ErrorIs(t, err, sandboxtypes.ErrLockLost)
	requireRecord(t, client, key, data)

	sbx, key, data = write("expired-hold", "op", time.Now().Add(-time.Second))
	requireRemoveReply(t, script.as(t.Context(), sbx.SandboxID, sbx.ExecutionID, "op", testLockValue), refused(data))
	requireRecord(t, client, key, data)

	sbx, key, data = write("unheld", "", time.Time{})
	requireRemoveReply(t, script.as(t.Context(), sbx.SandboxID, sbx.ExecutionID, "op", testLockValue), refused(data))
	requireRecord(t, client, key, data)

	sbx, key, data = write("other-hold", "other", time.Now().Add(time.Minute))
	requireRemoveReply(t, script.as(t.Context(), sbx.SandboxID, sbx.ExecutionID, "op", testLockValue), refused(data))
	requireRemoveReply(t, script.as(t.Context(), sbx.SandboxID, sbx.ExecutionID, "", testLockValue), refused(data))
	requireRecord(t, client, key, data)

	sbx, key, data = write("held", "op", time.Now().Add(time.Minute))
	requireRemoveReply(t, script.as(t.Context(), sbx.SandboxID, sbx.ExecutionID, "op", testLockValue), removed(data))
	requireNoRecord(t, client, key)
}

// parseRemoveReply distinguishes the replies and names any other shape in
// its error.
func TestParseRemoveReply(t *testing.T) {
	t.Parallel()

	requireRemoveReply(t, redis.NewCmdResult(nil, redis.Nil), absentReply)
	requireRemoveReply(t, redis.NewCmdResult([]any{int64(1), "{}"}, nil), removed("{}"))
	requireRemoveReply(t, redis.NewCmdResult([]any{int64(0), "{}"}, nil), refused("{}"))
	lost, err := readRemoveReply(redis.NewCmdResult([]any{int64(2), "{}"}, nil))
	require.ErrorIs(t, err, sandboxtypes.ErrLockLost)
	assert.Equal(t, refused("{}"), lost)

	failed := errors.New("connection reset")
	got, err := readRemoveReply(redis.NewCmdResult(nil, failed))
	require.ErrorIs(t, err, failed)
	assert.Equal(t, removeReply{}, got)

	for _, reply := range []any{"{}", int64(1), []any{int64(1)}, []any{"1", "{}"}, []any{int64(1), int64(2)}} {
		got, err := readRemoveReply(redis.NewCmdResult(reply, nil))
		require.ErrorContains(t, err, fmt.Sprintf("%#v", reply))
		assert.Equal(t, removeReply{}, got)
	}
}
