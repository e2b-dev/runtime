package evictor

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	redisreservations "github.com/e2b-dev/infra/packages/api/internal/sandbox/reservations/redis"
	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// A sandbox whose guest powered off gets its timeout action at once: a kill,
// or a pause that is filesystem-only whatever the sandbox asked for, pinned
// to the execution the node listed.
func TestEvictPoweredOff(t *testing.T) {
	t.Parallel()

	redisClient := redis_utils.SetupInstance(t)
	storage, err := sandboxredis.NewStorage(redisClient, noop.NewMeterProvider())
	require.NoError(t, err)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })
	store := sandbox.NewStore(storage, redisreservations.NewReservationStorage(redisClient, storage.Notifier()), sandbox.Callbacks{})

	flags, err := featureflags.NewClientWithLogLevel("", "", "", ldlog.Error)
	require.NoError(t, err)
	counter, err := telemetry.GetCounter(noop.NewMeterProvider().Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator/evictor"), telemetry.ApiEvictorFsOnlyAutoPause)
	require.NoError(t, err)

	stored := func(t *testing.T, autoPause, autoPauseFilesystemOnly bool) sandbox.NodeSandbox {
		t.Helper()

		sbx := sandbox.Sandbox{
			SandboxID:               "sbx-" + uuid.NewString()[:8],
			TeamID:                  uuid.New(),
			ExecutionID:             uuid.NewString(),
			State:                   sandbox.StateRunning,
			AutoPause:               autoPause,
			AutoPauseFilesystemOnly: autoPauseFilesystemOnly,
			StartTime:               time.Now(),
			EndTime:                 time.Now().Add(time.Hour),
		}
		require.NoError(t, store.Add(t.Context(), sbx, nil))

		node := sbx.ToNodeSandbox()
		node.DiskKept = true

		return node
	}

	evict := func(t *testing.T, node sandbox.NodeSandbox, prepare func(e *Evictor)) []sandbox.RemoveOpts {
		t.Helper()

		limiter, err := utils.NewAdjustableSemaphore(1)
		require.NoError(t, err)

		var got []sandbox.RemoveOpts
		e := &Evictor{
			store:                  store,
			featureFlags:           flags,
			fsOnlyAutoPauseCounter: counter,
			degradedCounter:        counter,
			concurrencyLimiter:     limiter,
			removeSandbox: func(_ context.Context, _ uuid.UUID, _ string, opts sandbox.RemoveOpts) error {
				got = append(got, opts)

				return nil
			},
		}
		if prepare != nil {
			prepare(e)
		}

		e.EvictPoweredOff(t.Context(), node)

		return got
	}

	// A refused pause's retry window, set by any API replica, holds here too.
	t.Run("a sandbox inside its refusal retry window is left alone", func(t *testing.T) {
		t.Parallel()

		sbx := sandbox.Sandbox{
			SandboxID:    "sbx-" + uuid.NewString()[:8],
			TeamID:       uuid.New(),
			ExecutionID:  uuid.NewString(),
			State:        sandbox.StateRunning,
			AutoPause:    true,
			StartTime:    time.Now(),
			EndTime:      time.Now().Add(time.Hour),
			RefusedUntil: time.Now().Add(time.Minute),
		}
		require.NoError(t, store.Add(t.Context(), sbx, nil))
		node := sbx.ToNodeSandbox()
		node.DiskKept = true

		assert.Empty(t, evict(t, node, nil))
	})

	t.Run("a listing from another node is left alone", func(t *testing.T) {
		t.Parallel()

		node := stored(t, false, false)
		node.NodeID = "other-node"

		assert.Empty(t, evict(t, node, nil))
	})

	// The node refusing the disk-only capture as unsupported must not turn
	// it into a memory pause of a guest that has no memory left.
	t.Run("an unsupported refusal is not retried as a memory pause", func(t *testing.T) {
		t.Parallel()

		node := stored(t, true, false)
		var got []sandbox.RemoveOpts
		evict(t, node, func(e *Evictor) {
			e.removeSandbox = func(_ context.Context, _ uuid.UUID, _ string, opts sandbox.RemoveOpts) error {
				got = append(got, opts)

				return sandbox.PauseUnsupportedError{}
			}
		})

		require.Len(t, got, 1)
		assert.True(t, got[0].FilesystemOnly)
	})

	// The node takes the disk-only capture of a powered-off guest whatever
	// its profile, so a refusal the running guest met earlier, under either
	// policy, does not turn the sync into a kill.
	t.Run("an earlier unsupported refusal keeps the disk-only capture", func(t *testing.T) {
		t.Parallel()

		for _, fsOnlyPolicy := range []bool{false, true} {
			node := stored(t, true, fsOnlyPolicy)
			got := evict(t, node, func(e *Evictor) {
				e.unsupportedFilesystemOnly.Store(node.SandboxID, unsupportedRefusal{at: time.Now(), executionID: node.ExecutionID})
			})

			require.Len(t, got, 1)
			assert.Equal(t, sandbox.StateActionPause, got[0].Action, "policy filesystem-only=%v", fsOnlyPolicy)
			assert.True(t, got[0].FilesystemOnly)
		}
	})

	t.Run("kill when the sandbox does not pause on timeout", func(t *testing.T) {
		t.Parallel()

		node := stored(t, false, false)
		got := evict(t, node, nil)

		require.Len(t, got, 1)
		assert.Equal(t, sandbox.StateActionKill, got[0].Action)
		assert.Equal(t, sandbox.KillReasonGuestPowerOff, got[0].Reason)
		assert.Equal(t, node.ExecutionID, got[0].ExpectExecutionID)
		assert.True(t, got[0].Eviction)
		assert.True(t, got[0].BeforeEndTime)
	})

	t.Run("filesystem-only pause when the sandbox pauses with memory", func(t *testing.T) {
		t.Parallel()

		node := stored(t, true, false)
		got := evict(t, node, nil)

		require.Len(t, got, 1)
		assert.Equal(t, sandbox.StateActionPause, got[0].Action)
		assert.True(t, got[0].FilesystemOnly)
		assert.Equal(t, node.ExecutionID, got[0].ExpectExecutionID)
		assert.True(t, got[0].BeforeEndTime)
	})

	t.Run("filesystem-only pause when the sandbox pauses filesystem-only", func(t *testing.T) {
		t.Parallel()

		got := evict(t, stored(t, true, true), nil)

		require.Len(t, got, 1)
		assert.Equal(t, sandbox.StateActionPause, got[0].Action)
		assert.True(t, got[0].FilesystemOnly)
	})

	t.Run("another execution is left alone", func(t *testing.T) {
		t.Parallel()

		node := stored(t, false, false)
		node.ExecutionID = uuid.NewString()

		assert.Empty(t, evict(t, node, nil))
	})

	t.Run("a sandbox not in the store is left alone", func(t *testing.T) {
		t.Parallel()

		node := stored(t, false, false)
		node.SandboxID = "sbx-missing"

		assert.Empty(t, evict(t, node, nil))
	})

	// The timeout sweep and the sync can find the same sandbox; only one of
	// them acts.
	t.Run("an eviction in flight is not repeated", func(t *testing.T) {
		t.Parallel()

		node := stored(t, false, false)
		got := evict(t, node, func(e *Evictor) {
			e.activeEvictions.Store(node.SandboxID, struct{}{})
		})

		assert.Empty(t, got)
	})
}
