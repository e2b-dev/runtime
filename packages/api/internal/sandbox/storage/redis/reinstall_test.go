package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// A Replace reinstalling an execution writes its member before its record.
// ExpiredItems and Remove remove a member after reading its record absent or
// another execution's, and a Replace landing between that read and the
// removal loses its member to it: both read the record again after the
// removal and put the member back, due now, and a Replace landing after that
// read asserts its member again after its record's write, due now as well;
// the next sweep then scores the member by the record. These tests drive
// each interleaving through a client hook, with the hook's writes landing
// inside the operation under test.

// commandHook routes single commands and pipelines to the functions set; a
// nil function passes its commands through.
type commandHook struct {
	single   func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error
	pipeline func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error
}

func (h commandHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h commandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	if h.single == nil {
		return next
	}

	return func(ctx context.Context, cmd redis.Cmder) error { return h.single(ctx, cmd, next) }
}

func (h commandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	if h.pipeline == nil {
		return next
	}

	return func(ctx context.Context, cmds []redis.Cmder) error { return h.pipeline(ctx, cmds, next) }
}

// hookedStorage builds a storage over a client with hook installed before
// the storage starts using the client. The healer the storage starts makes
// its first pass a heal interval away, beyond any test here.
func hookedStorage(t *testing.T, hook redis.Hook) (*Storage, redis.UniversalClient) {
	t.Helper()

	client := redis_utils.SetupInstance(t)
	client.AddHook(hook)
	storage := newTestStorage(t, client)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	return storage, client
}

// isAddScript reports whether cmd runs the add script on the record key
// (the test client prefixes keys), the script loaded beforehand so it runs
// as EVALSHA on a fresh server too.
func isAddScript(cmd redis.Cmder, key string) bool {
	args := cmd.Args()
	if cmd.Name() != "evalsha" || args[1] != addSandboxScript.Hash() || len(args) < 4 {
		return false
	}
	recordKey, ok := args[3].(string)

	return ok && strings.HasSuffix(recordKey, key)
}

// reinstallBeforeAssert performs a Replace's writes up to its record's write:
// the member, then the record, without the assertion that follows the write.
func reinstallBeforeAssert(ctx context.Context, client redis.UniversalClient, sbx sandboxtypes.Sandbox) error {
	member := redis.Z{Score: float64(sbx.EndTime.UnixMilli()), Member: sandboxExpirationMember(sbx)}
	if err := client.ZAdd(ctx, globalExpirationSet, member).Err(); err != nil {
		return err
	}

	data, err := json.Marshal(sbx)
	if err != nil {
		return err
	}

	key := recordKey(formSandbox, sbx.TeamID.String(), sbx.SandboxID)
	keys := []string{key, goneKeyFor(key)}

	return RunScript(ctx, client, "add_sandbox", addSandboxScript, keys, data, sbx.SandboxID, sbx.ExecutionID, "1", int(goneExecutionsTTL.Seconds())).Err()
}

// reinstalls are the two ways a Replace can have landed when a removal of
// its member runs: complete, its assertion included, or up to its record's
// write.
var reinstalls = []struct {
	name string
	land func(ctx context.Context, storage *Storage, client redis.UniversalClient, sbx sandboxtypes.Sandbox) error
}{
	{"complete", func(ctx context.Context, storage *Storage, _ redis.UniversalClient, sbx sandboxtypes.Sandbox) error {
		return storage.Replace(ctx, sbx)
	}},
	{"before its assertion", func(ctx context.Context, _ *Storage, client redis.UniversalClient, sbx sandboxtypes.Sandbox) error {
		return reinstallBeforeAssert(ctx, client, sbx)
	}},
}

// requireMemberDue asserts the member is in the sweep's window, put there
// no earlier than notBefore.
func requireMemberDue(t *testing.T, client redis.UniversalClient, member string, notBefore time.Time) {
	t.Helper()

	score, err := client.ZScore(t.Context(), globalExpirationSet, member).Result()
	require.NoError(t, err)
	require.GreaterOrEqual(t, score, float64(notBefore.UnixMilli()))
	require.LessOrEqual(t, score, float64(time.Now().UnixMilli()))
}

// The sweep read the member's record absent (a refused create's leftover) or
// another execution's; the Replace lands before the sweep's removal, which
// takes the member. The sweep reads the record again, puts the member back
// due now, and returns the sandbox, expired, on its next pass.
func TestExpiredItems_PutsBackAMemberReinstalledDuringItsRemoval(t *testing.T) {
	t.Parallel()

	for _, stored := range []struct {
		name    string
		install func(ctx context.Context, storage *Storage, sbx sandboxtypes.Sandbox) error
	}{
		{"no record", func(context.Context, *Storage, sandboxtypes.Sandbox) error { return nil }},
		{"another execution's record", func(ctx context.Context, storage *Storage, sbx sandboxtypes.Sandbox) error {
			other := sbx
			other.ExecutionID = uuid.NewString()
			other.EndTime = time.Now().Add(time.Hour)

			return storage.Add(ctx, other)
		}},
	} {
		for _, reinstall := range reinstalls {
			t.Run(stored.name+", the Replace "+reinstall.name, func(t *testing.T) {
				t.Parallel()

				ctx := t.Context()
				teamID := uuid.New()
				sbx := makeIndexedSandbox(teamID, "sbx-reinstalled-in-sweep", uuid.NewString(), time.Now().Add(-2*time.Minute), time.Now().Add(-time.Minute))
				member := sandboxExpirationMember(sbx)

				var storage *Storage
				var client redis.UniversalClient
				var reinstalled atomic.Bool
				storage, client = hookedStorage(t, commandHook{single: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
					if cmd.Name() == "zrem" && reinstalled.CompareAndSwap(false, true) {
						if err := reinstall.land(ctx, storage, client, sbx); err != nil {
							return err
						}
					}

					return next(ctx, cmd)
				}})

				require.NoError(t, stored.install(ctx, storage, sbx))
				// The member of the execution with no record of its own, expired.
				require.NoError(t, client.ZAdd(ctx, globalExpirationSet, redis.Z{Score: float64(sbx.EndTime.UnixMilli()), Member: member}).Err())

				before := time.Now()
				items, err := storage.ExpiredItems(ctx)
				require.NoError(t, err)
				require.Empty(t, items, "the sweep judged the member stale before the Replace landed")
				require.True(t, reinstalled.Load())

				got, err := storage.Get(ctx, teamID, sbx.SandboxID)
				require.NoError(t, err)
				require.Equal(t, sbx.ExecutionID, got.ExecutionID)
				requireMemberDue(t, client, member, before)

				items, err = storage.ExpiredItems(ctx)
				require.NoError(t, err)
				require.Len(t, items, 1)
				require.Equal(t, sbx.ExecutionID, items[0].ExecutionID)
			})
		}
	}
}

// Remove deleted the record; a Replace reinstalls the execution before
// Remove's removal of its member, which takes the member. Remove reads the
// record again and puts the member back due now; the next sweep scores it by
// the record. A complete Replace takes both form locks and waits for this
// Remove, so the one that can land inside it was written past an expired
// lock: the writes up to the record, without the assertion.
func TestRemove_PutsBackAMemberReinstalledDuringItsRemoval(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	teamID := uuid.New()
	sbx := makeIndexedSandbox(teamID, "sbx-reinstalled-in-remove", uuid.NewString(), time.Now(), time.Now().Add(time.Hour))
	member := sandboxExpirationMember(sbx)

	var client redis.UniversalClient
	var reinstalled atomic.Bool
	storage, client := hookedStorage(t, commandHook{single: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if cmd.Name() == "zrem" && reinstalled.CompareAndSwap(false, true) {
			if err := reinstallBeforeAssert(ctx, client, sbx); err != nil {
				return err
			}
		}

		return next(ctx, cmd)
	}})

	require.NoError(t, storage.Add(ctx, sbx))
	before := time.Now()
	require.NoError(t, storage.Remove(ctx, teamID, sbx.SandboxID, ""))
	require.True(t, reinstalled.Load())

	got, err := storage.Get(ctx, teamID, sbx.SandboxID)
	require.NoError(t, err, "the reinstalled record stays")
	require.Equal(t, sbx.ExecutionID, got.ExecutionID)
	requireMemberDue(t, client, member, before)

	items, err := storage.ExpiredItems(ctx)
	require.NoError(t, err)
	require.Empty(t, items)
	requireMemberScore(t, client, member, float64(sbx.EndTime.UnixMilli()))
}

// A removal lands inside the write: a whole sweep between the member's write
// and the record's (the sweep reads the record absent, removes the member,
// reads the record absent again and returns), or a removal right after the
// record's write. The write asserts its member after the record's write,
// due now, and the next sweep scores it by the record or returns the
// sandbox. The same holds for a create's write.
func TestAdd_AssertsItsMemberAfterItsRecordWrite(t *testing.T) {
	t.Parallel()

	type hole struct {
		name    string
		endTime time.Duration // from now
		land    func(ctx context.Context, storage *Storage, client redis.UniversalClient, member string) error
		before  bool // land before the record's write, else right after it
	}
	holes := []hole{
		{"a sweep between the member's write and the record's", -time.Minute, func(ctx context.Context, storage *Storage, _ redis.UniversalClient, _ string) error {
			_, err := storage.ExpiredItems(ctx)

			return err
		}, true},
		{"a removal right after the record's write", time.Hour, func(ctx context.Context, _ *Storage, client redis.UniversalClient, member string) error {
			return client.ZRem(ctx, globalExpirationSet, member).Err()
		}, false},
	}

	for _, write := range []struct {
		name  string
		write func(storage *Storage, ctx context.Context, sbx sandboxtypes.Sandbox) error
	}{
		{"replace", (*Storage).Replace},
		{"create", (*Storage).Add},
	} {
		for _, hole := range holes {
			t.Run(write.name+", "+hole.name, func(t *testing.T) {
				t.Parallel()

				ctx := t.Context()
				teamID := uuid.New()
				sbx := makeIndexedSandbox(teamID, "sbx-asserted", uuid.NewString(), time.Now().Add(-2*time.Minute), time.Now().Add(hole.endTime))
				member := sandboxExpirationMember(sbx)

				var storage *Storage
				var client redis.UniversalClient
				var landed atomic.Bool
				storage, client = hookedStorage(t, commandHook{single: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
					if !isAddScript(cmd, recordKey(formSandbox, teamID.String(), sbx.SandboxID)) || !landed.CompareAndSwap(false, true) {
						return next(ctx, cmd)
					}
					if hole.before {
						if err := hole.land(ctx, storage, client, member); err != nil {
							return err
						}

						return next(ctx, cmd)
					}
					if err := next(ctx, cmd); err != nil {
						return err
					}

					return hole.land(ctx, storage, client, member)
				}})

				require.NoError(t, addSandboxScript.Load(ctx, client).Err())
				before := time.Now()
				require.NoError(t, write.write(storage, ctx, sbx))
				require.True(t, landed.Load())
				requireMemberDue(t, client, member, before)

				items, err := storage.ExpiredItems(ctx)
				require.NoError(t, err)
				if sbx.IsExpired(time.Now()) {
					require.Len(t, items, 1)
					require.Equal(t, sbx.ExecutionID, items[0].ExecutionID)
				} else {
					require.Empty(t, items)
					requireMemberScore(t, client, member, float64(sbx.EndTime.UnixMilli()))
				}
			})
		}
	}
}

// The assertion's round trip fails after a sweep took the member between
// the member's write and the record's: the write still returns, with the
// record stored and unindexed, and the healer's pass re-indexes it.
func TestAdd_AFailedAssertionIsLeftToTheHealer(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	teamID := uuid.New()
	sbx := makeIndexedSandbox(teamID, "sbx-unasserted", uuid.NewString(), time.Now().Add(-2*healGracePeriod), time.Now().Add(-time.Minute))
	member := sandboxExpirationMember(sbx)

	var storage *Storage
	var swept, failed atomic.Bool
	storage, client := hookedStorage(t, commandHook{
		single: func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
			if isAddScript(cmd, recordKey(formSandbox, teamID.String(), sbx.SandboxID)) && swept.CompareAndSwap(false, true) {
				if _, err := storage.ExpiredItems(ctx); err != nil {
					return err
				}
			}

			return next(ctx, cmd)
		},
		pipeline: func(ctx context.Context, cmds []redis.Cmder, next redis.ProcessPipelineHook) error {
			for _, cmd := range cmds {
				if cmd.Name() == "zadd" && cmd.Args()[len(cmd.Args())-1] == member && failed.CompareAndSwap(false, true) {
					return errors.New("the assertion's round trip failed")
				}
			}

			return next(ctx, cmds)
		},
	})

	// The healer walks the global teams index; the team is in it already.
	sibling := makeIndexedSandbox(teamID, "sbx-sibling", uuid.NewString(), time.Now(), time.Now().Add(time.Hour))
	require.NoError(t, storage.Add(ctx, sibling))
	require.NoError(t, addSandboxScript.Load(ctx, client).Err())

	require.NoError(t, storage.Replace(ctx, sbx))
	require.True(t, swept.Load())
	require.True(t, failed.Load())

	_, err := storage.Get(ctx, teamID, sbx.SandboxID)
	require.NoError(t, err, "the record is stored")
	requireMemberAbsent(t, client, member)
	items, err := storage.ExpiredItems(ctx)
	require.NoError(t, err)
	require.Empty(t, items, "unindexed, the expired sandbox is invisible to the sweep")

	healed, _, err := storage.healExpirationIndex(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, healed)
	requireMemberScore(t, client, member, float64(sbx.EndTime.UnixMilli()))
	items, err = storage.ExpiredItems(ctx)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, sbx.ExecutionID, items[0].ExecutionID)
}
