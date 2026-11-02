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

// removeScript is one of the remove scripts, run on a sandbox of the team
// in the form whose record it guards.
type removeScript struct {
	name string
	form keyForm
	run  func(ctx context.Context, sandboxID, expected string) *redis.Cmd
}

// removeScripts returns the remove script's two calls: the team form's,
// which removes the record with its index entry, and the sandbox form's,
// which removes the record alone, its index entry living in another slot.
func removeScripts(client redis.UniversalClient, teamID uuid.UUID, index string) []removeScript {
	return []removeScript{
		{"remove_sandbox", formTeam, func(ctx context.Context, sandboxID, expected string) *redis.Cmd {
			key := recordKey(formTeam, teamID.String(), sandboxID)

			return RunScript(ctx, client, "remove_sandbox", removeSandboxScript, []string{key, index}, sandboxID, expected)
		}},
		{"remove_record", formSandbox, func(ctx context.Context, sandboxID, expected string) *redis.Cmd {
			key := recordKey(formSandbox, teamID.String(), sandboxID)

			return RunScript(ctx, client, "remove_record", removeSandboxScript, []string{key}, sandboxID, expected)
		}},
	}
}

// A remove script removes the record for the execution the caller names, or
// for any execution when it names none, and returns what it removed. For
// another execution it returns the record it found and writes nothing. An
// absent record is nil, and nothing is written. The team form's script
// takes the index entry with the record and leaves it with a refused or an
// absent one: the entry may be a successor's, held in the other form.
func TestRemoveScriptGuards(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())

	for _, script := range removeScripts(client, teamID, index) {
		t.Run(script.name, func(t *testing.T) {
			t.Parallel()

			// Only the team form's script can touch the index.
			requireEntry := func(sandboxID string, want bool) {
				t.Helper()

				if script.form == formTeam {
					requireIndexed(t, client, index, sandboxID, want)
				}
			}
			write := func(name string) (sbx sandboxtypes.Sandbox, key, data string) {
				t.Helper()

				sbx = newRecord(teamID, fmt.Sprintf("sbx-%s-%s", script.name, name))
				key = recordKey(script.form, teamID.String(), sbx.SandboxID)

				return sbx, key, writeRecord(t, client, key, index, sbx)
			}

			sbx, key, data := write("match")
			requireRemoveReply(t, script.run(t.Context(), sbx.SandboxID, sbx.ExecutionID), removed(data))
			requireNoRecord(t, client, key)
			requireEntry(sbx.SandboxID, false)

			sbx, key, data = write("other")
			requireRemoveReply(t, script.run(t.Context(), sbx.SandboxID, uuid.NewString()), refused(data))
			requireRecord(t, client, key, data)
			requireEntry(sbx.SandboxID, true)

			sbx, key, data = write("unguarded")
			requireRemoveReply(t, script.run(t.Context(), sbx.SandboxID, ""), removed(data))
			requireNoRecord(t, client, key)
			requireEntry(sbx.SandboxID, false)

			absent := "sbx-" + script.name + "-absent"
			require.NoError(t, client.SAdd(t.Context(), index, absent).Err())
			requireRemoveReply(t, script.run(t.Context(), absent, ""), absentReply)
			requireRemoveReply(t, script.run(t.Context(), absent, uuid.NewString()), absentReply)
			requireEntry(absent, true)
		})
	}
}

// A remove script re-executed after a lost reply finds its record gone, or
// finds a successor of the same sandbox ID and leaves it whole: the
// successor's record and index entry survive, whichever form it was written
// in.
func TestRemoveScriptReexecutionIsNoop(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	teamID := uuid.New()
	index := GetSandboxStorageTeamIndexKey(teamID.String())

	for _, script := range removeScripts(client, teamID, index) {
		t.Run(script.name, func(t *testing.T) {
			t.Parallel()

			sandboxID := "sbx-" + script.name
			key := recordKey(script.form, teamID.String(), sandboxID)

			sbx := newRecord(teamID, sandboxID)
			data := writeRecord(t, client, key, index, sbx)
			requireRemoveReply(t, script.run(t.Context(), sandboxID, sbx.ExecutionID), removed(data))
			requireRemoveReply(t, script.run(t.Context(), sandboxID, sbx.ExecutionID), absentReply)

			// A successor in the same form: refused, and left whole.
			successor := newRecord(teamID, sandboxID)
			successorData := writeRecord(t, client, key, index, successor)
			requireRemoveReply(t, script.run(t.Context(), sandboxID, sbx.ExecutionID), refused(successorData))
			requireRecord(t, client, key, successorData)
			// Only the team form's script can touch the index.
			if script.form == formTeam {
				requireIndexed(t, client, index, sandboxID, true)
			}
			requireRemoveReply(t, script.run(t.Context(), sandboxID, successor.ExecutionID), removed(successorData))

			// A successor in the other form shares the index entry: absent
			// here, and the entry stays.
			otherKey := recordKey(script.form.other(), teamID.String(), sandboxID)
			successor = newRecord(teamID, sandboxID)
			successorData = writeRecord(t, client, otherKey, index, successor)
			requireRemoveReply(t, script.run(t.Context(), sandboxID, sbx.ExecutionID), absentReply)
			requireRecord(t, client, otherKey, successorData)
			if script.form == formTeam {
				requireIndexed(t, client, index, sandboxID, true)
			}
		})
	}
}

// parseRemoveReply distinguishes the three replies and names any other
// shape in its error.
func TestParseRemoveReply(t *testing.T) {
	t.Parallel()

	requireRemoveReply(t, redis.NewCmdResult(nil, redis.Nil), absentReply)
	requireRemoveReply(t, redis.NewCmdResult([]any{int64(1), "{}"}, nil), removed("{}"))
	requireRemoveReply(t, redis.NewCmdResult([]any{int64(0), "{}"}, nil), refused("{}"))

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
