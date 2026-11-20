package state_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

type env struct {
	team           uuid.UUID
	other          uuid.UUID
	sandboxID      string
	executionID    string
	otherExecution string
	start          time.Time
}

func newEnv(start time.Time) env {
	return env{
		team:           uuid.New(),
		other:          uuid.New(),
		sandboxID:      newSandboxID(),
		executionID:    uuid.NewString(),
		otherExecution: uuid.NewString(),
		start:          start,
	}
}

func (e env) record() sandboxtypes.Sandbox {
	return sandboxtypes.Sandbox{
		SandboxID:   e.sandboxID,
		TeamID:      e.team,
		ExecutionID: e.executionID,
		StartTime:   e.start,
		EndTime:     e.start.Add(time.Hour),
		NodeID:      "node-a",
		State:       sandboxtypes.StateRunning,
	}
}

func (e env) hold(id string) sandboxtypes.Hold {
	return sandboxtypes.Hold{TeamID: e.team, SandboxID: e.sandboxID, ExecutionID: e.executionID, SourceNodeID: "node-a", ID: id}
}

func (e env) until() time.Time {
	return e.start.Add(time.Hour)
}

func (e env) normalize(t *testing.T, value any) string {
	t.Helper()

	data, err := json.Marshal(value)
	require.NoError(t, err)
	text := string(data)
	for from, to := range map[string]string{
		e.team.String():  "TEAM",
		e.other.String(): "OTHER-TEAM",
		e.sandboxID:      "SANDBOX",
		e.executionID:    "EXECUTION",
		e.otherExecution: "OTHER-EXECUTION",
	} {
		text = strings.ReplaceAll(text, from, to)
	}

	return text
}

var storeSentinels = []error{
	sandboxtypes.ErrNotFound,
	sandboxtypes.ErrAlreadyExists,
	sandboxtypes.ErrEvictionInProgress,
	sandboxtypes.ErrEvictionNotNeeded,
	sandboxtypes.ErrTransitionRestored,
	sandboxtypes.ErrSkipRestore,
	sandboxtypes.ErrDraining,
	sandboxtypes.ErrExecutionMismatch,
	sandboxtypes.ErrLifecycleHeld,
	sandboxtypes.ErrHoldConflict,
	sandboxtypes.ErrLockLost,
	sandboxtypes.ErrInvalidUpdateToken,
	sandboxtypes.ErrNotHeld,
	sandboxtypes.ErrSandboxKilled,
	sandboxtypes.ErrRestoreConflict,
	sandboxtypes.PauseQueueExhaustedError{},
	sandboxtypes.PauseUnsupportedError{},
	context.Canceled,
	context.DeadlineExceeded,
}

func requireSameTyped[E error](t *testing.T, direct, remote error, de, re env) {
	t.Helper()

	d, directOK := errors.AsType[E](direct)
	r, remoteOK := errors.AsType[E](remote)
	require.Equal(t, directOK, remoteOK, "both or neither are %T: direct %v, remote %v", d, direct, remote)
	if directOK {
		assert.Equal(t, de.normalize(t, d), re.normalize(t, r), "the fields of %T", d)
	}
}

func requireSameError(t *testing.T, direct, remote error, de, re env) {
	t.Helper()

	require.Equal(t, direct == nil, remote == nil, "both or neither fail: direct %v, remote %v", direct, remote)
	for _, sentinel := range storeSentinels {
		assert.Equal(t, errors.Is(direct, sentinel), errors.Is(remote, sentinel), "errors.Is %v: direct %v, remote %v", sentinel, direct, remote)
	}
	requireSameTyped[*sandboxtypes.InvalidStateTransitionError](t, direct, remote, de, re)
	requireSameTyped[*sandboxtypes.NotRunningError](t, direct, remote, de, re)
	requireSameTyped[*sandboxtypes.TransitionFailedError](t, direct, remote, de, re)
	requireSameTyped[*sandboxtypes.LimitExceededError](t, direct, remote, de, re)
	requireSameTyped[*sandboxtypes.StartResultMissingError](t, direct, remote, de, re)
	directRestored, directOK := errors.AsType[*sandboxtypes.RestoredTransitionError](direct)
	remoteRestored, remoteOK := errors.AsType[*sandboxtypes.RestoredTransitionError](remote)
	require.Equal(t, directOK, remoteOK)
	if directOK {
		assert.Equal(t, directRestored.Refusal, remoteRestored.Refusal)
	}
	if direct != nil {
		t.Logf("direct error: %s", de.normalize(t, direct.Error()))
		t.Logf("remote error: %s", re.normalize(t, remote.Error()))
	}
}

type pick func(s redisSetup) sandboxtypes.Storage

type storeScenario func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error)

func direct(s redisSetup) sandboxtypes.Storage { return s.records }

func viaRemote(s redisSetup) sandboxtypes.Storage { return s.remote }

func (s redisSetup) storeRecord(ctx context.Context, t *testing.T, sbx sandboxtypes.Sandbox) {
	t.Helper()

	require.NoError(t, s.records.Add(ctx, sbx))
}

func (s redisSetup) storedOrAbsent(ctx context.Context, t *testing.T, teamID uuid.UUID, sandboxID string) any {
	t.Helper()

	sbx, err := s.records.Get(ctx, teamID, sandboxID)
	if errors.Is(err, sandboxtypes.ErrNotFound) {
		return "absent"
	}
	require.NoError(t, err)

	return sbx
}

func TestDirectVersusRemote(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	scenarios := map[string]storeScenario{
		"Add new": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			err := store(s).Add(ctx, e.record())

			return s.storedOrAbsent(ctx, t, e.team, e.sandboxID), err
		},
		"Add duplicate execution": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			s.storeRecord(ctx, t, e.record())
			late := e.record()
			late.TemplateID = "late"
			err := store(s).Add(ctx, late)

			return s.storedOrAbsent(ctx, t, e.team, e.sandboxID), err
		},
		"Add held": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			s.storeRecord(ctx, t, e.record())
			require.NoError(t, s.records.HoldExecution(ctx, e.hold("hold"), e.until()))
			replacement := e.record()
			replacement.ExecutionID = e.otherExecution
			err := store(s).Add(ctx, replacement)

			return s.storedOrAbsent(ctx, t, e.team, e.sandboxID), err
		},
		"Get stored": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			s.storeRecord(ctx, t, e.record())

			return store(s).Get(ctx, e.team, e.sandboxID)
		},
		"Get absent": func(ctx context.Context, _ *testing.T, s redisSetup, store pick, e env) (any, error) {
			return store(s).Get(ctx, e.team, e.sandboxID)
		},
		"Get another team": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			s.storeRecord(ctx, t, e.record())

			return store(s).Get(ctx, e.other, e.sandboxID)
		},
		"Remove stored": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			s.storeRecord(ctx, t, e.record())
			err := store(s).Remove(ctx, e.team, e.sandboxID, e.executionID)

			return s.storedOrAbsent(ctx, t, e.team, e.sandboxID), err
		},
		"Remove absent": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			err := store(s).Remove(ctx, e.team, e.sandboxID, "")

			return s.storedOrAbsent(ctx, t, e.team, e.sandboxID), err
		},
		"Remove execution mismatch": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			s.storeRecord(ctx, t, e.record())
			err := store(s).Remove(ctx, e.team, e.sandboxID, e.otherExecution)

			return s.storedOrAbsent(ctx, t, e.team, e.sandboxID), err
		},
		"Remove held": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			s.storeRecord(ctx, t, e.record())
			require.NoError(t, s.records.HoldExecution(ctx, e.hold("hold"), e.until()))
			err := store(s).Remove(ctx, e.team, e.sandboxID, "")

			return s.storedOrAbsent(ctx, t, e.team, e.sandboxID), err
		},
		"TeamItems with and without a state filter": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			for _, suffix := range []string{"-a", "-b", "-c"} {
				sbx := e.record()
				sbx.SandboxID += suffix
				sbx.ExecutionID = uuid.NewString()
				if suffix == "-c" {
					sbx.State = sandboxtypes.StatePausing
				}
				s.storeRecord(ctx, t, sbx)
			}
			s.storeRecord(ctx, t, sandboxtypes.Sandbox{
				SandboxID: e.sandboxID + "-other", TeamID: e.other, ExecutionID: uuid.NewString(), StartTime: e.start, EndTime: e.start.Add(time.Hour), State: sandboxtypes.StateRunning,
			})
			sorted := func(items []sandboxtypes.Sandbox) []string {
				var ids []string
				for _, sbx := range items {
					ids = append(ids, sbx.SandboxID+" "+string(sbx.State))
				}
				slices.Sort(ids)

				return ids
			}
			all, err := store(s).TeamItems(ctx, e.team, nil)
			if err != nil {
				return nil, err
			}
			running, err := store(s).TeamItems(ctx, e.team, []sandboxtypes.State{sandboxtypes.StateRunning})
			if err != nil {
				return nil, err
			}
			pausing, err := store(s).TeamItems(ctx, e.team, []sandboxtypes.State{sandboxtypes.StatePausing, sandboxtypes.StateKilling})

			return []any{sorted(all), sorted(running), sorted(pausing), len(all)}, err
		},
		"TeamItems of an empty team": func(ctx context.Context, _ *testing.T, s redisSetup, store pick, e env) (any, error) {
			items, err := store(s).TeamItems(ctx, e.team, nil)

			return []any{items, items == nil}, err
		},
		"ExpiredItems": func(ctx context.Context, t *testing.T, fresh redisSetup, store pick, e env) (any, error) {
			t.Helper()
			empty, err := store(fresh).ExpiredItems(ctx)
			require.NoError(t, err)

			expired := e.record()
			expired.EndTime = e.start.Add(-time.Hour)
			fresh.storeRecord(ctx, t, expired)
			live := e.record()
			live.SandboxID += "-live"
			fresh.storeRecord(ctx, t, live)
			items, err := store(fresh).ExpiredItems(ctx)

			return []any{empty, empty == nil, items}, err
		},
		"TeamsWithSandboxCount": func(ctx context.Context, t *testing.T, fresh redisSetup, store pick, e env) (any, error) {
			t.Helper()
			empty, err := store(fresh).TeamsWithSandboxCount(ctx)
			require.NoError(t, err)

			for _, suffix := range []string{"-a", "-b"} {
				sbx := e.record()
				sbx.SandboxID += suffix
				fresh.storeRecord(ctx, t, sbx)
			}
			other := e.record()
			other.TeamID = e.other
			fresh.storeRecord(ctx, t, other)
			counts, err := store(fresh).TeamsWithSandboxCount(ctx)

			return []any{empty, empty == nil, counts[e.team], counts[e.other], len(counts)}, err
		},
		"Reconcile": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			s.storeRecord(ctx, t, e.record())
			old := e.start.Add(-time.Hour)
			node := func(suffix string, start time.Time) sandboxtypes.NodeSandbox {
				return sandboxtypes.NodeSandbox{
					SandboxID: e.sandboxID + suffix, ExecutionID: e.executionID, TeamID: e.team, NodeID: "node-a", ClusterID: e.other,
					StartTime: start, VCpu: 2, RamMB: 512, HugePages: true, GuestEvent: "event", DiskKept: true,
				}
			}
			orphans := store(s).Reconcile(ctx, []sandboxtypes.NodeSandbox{
				node("", old), node("-absent-old", old), node("-absent-young", time.Now()),
			}, "node-a")

			return orphans, nil
		},
		"Reconcile of nothing": func(ctx context.Context, _ *testing.T, s redisSetup, store pick, _ env) (any, error) {
			orphans := store(s).Reconcile(ctx, nil, "node-a")

			return []any{orphans, orphans == nil}, nil
		},
		"HoldExecution": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			s.storeRecord(ctx, t, e.record())
			err := store(s).HoldExecution(ctx, e.hold("hold"), e.until())

			return s.storedOrAbsent(ctx, t, e.team, e.sandboxID), err
		},
		"HoldExecution refused": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			s.storeRecord(ctx, t, e.record())
			require.NoError(t, s.records.HoldExecution(ctx, e.hold("hold"), e.until()))
			err := store(s).HoldExecution(ctx, e.hold("other"), e.until())

			return s.storedOrAbsent(ctx, t, e.team, e.sandboxID), err
		},
		"MoveHeldExecution": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			s.storeRecord(ctx, t, e.record())
			require.NoError(t, s.records.HoldExecution(ctx, e.hold("hold"), e.until()))
			moved, err := store(s).MoveHeldExecution(ctx, e.hold("hold"), "node-b")
			require.NoError(t, err)
			again, err := store(s).MoveHeldExecution(ctx, e.hold("hold"), "node-b")

			return []any{moved, again, s.storedOrAbsent(ctx, t, e.team, e.sandboxID)}, err
		},
		"MoveHeldExecution refused": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			s.storeRecord(ctx, t, e.record())
			require.NoError(t, s.records.HoldExecution(ctx, e.hold("hold"), e.until()))
			moved, err := store(s).MoveHeldExecution(ctx, e.hold("other"), "node-b")

			return []any{moved, s.storedOrAbsent(ctx, t, e.team, e.sandboxID)}, err
		},
		"ReleaseHeldExecution": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			s.storeRecord(ctx, t, e.record())
			require.NoError(t, s.records.HoldExecution(ctx, e.hold("hold"), e.until()))
			err := store(s).ReleaseHeldExecution(ctx, e.hold("hold"))

			return s.storedOrAbsent(ctx, t, e.team, e.sandboxID), err
		},
		"ReleaseHeldExecution refused": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			s.storeRecord(ctx, t, e.record())
			require.NoError(t, s.records.HoldExecution(ctx, e.hold("hold"), e.until()))
			mismatch := e.hold("hold")
			mismatch.ExecutionID = e.otherExecution
			err := store(s).ReleaseHeldExecution(ctx, mismatch)

			return s.storedOrAbsent(ctx, t, e.team, e.sandboxID), err
		},
		"RemoveHeldExecution": func(ctx context.Context, t *testing.T, s redisSetup, store pick, e env) (any, error) {
			t.Helper()
			s.storeRecord(ctx, t, e.record())
			require.NoError(t, s.records.HoldExecution(ctx, e.hold("hold"), e.until()))
			removed, err := store(s).RemoveHeldExecution(ctx, e.hold("hold"))

			return []any{removed, s.storedOrAbsent(ctx, t, e.team, e.sandboxID)}, err
		},
		"RemoveHeldExecution refused": func(ctx context.Context, _ *testing.T, s redisSetup, store pick, e env) (any, error) {
			return store(s).RemoveHeldExecution(ctx, e.hold("hold"))
		},
	}
	global := map[string]bool{"ExpiredItems": true, "TeamsWithSandboxCount": true}
	for name, run := range scenarios {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			directSetup, remoteSetup := s, s
			if global[name] {
				directSetup, remoteSetup = setupRedis(t), setupRedis(t)
			}
			ctx := callContext(t)
			start := time.Now().UTC().Truncate(time.Second)
			de, re := newEnv(start), newEnv(start)
			directValue, directErr := run(ctx, t, directSetup, direct, de)
			remoteValue, remoteErr := run(ctx, t, remoteSetup, viaRemote, re)

			requireSameError(t, directErr, remoteErr, de, re)
			assert.Equal(t, de.normalize(t, directValue), re.normalize(t, remoteValue), "the remote store returns what the Redis store returns")
		})
	}
}

func TestRemoteRecordFidelity(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	ctx := callContext(t)
	original := fullRecord(uuid.New(), newSandboxID())
	requireNoZeroField(t, original)

	require.NoError(t, s.remote.Add(ctx, original))
	got, err := s.remote.Get(ctx, original.TeamID, original.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, original, got, "the record survives the remote store's Add and Get")
	stored, err := s.records.Get(ctx, original.TeamID, original.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, stored, got, "the remote store returns what the Redis store returns")
}
