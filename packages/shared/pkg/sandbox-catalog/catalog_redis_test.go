package sandbox_catalog

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func testSandboxInfo(executionID, orchestratorID string) *SandboxInfo {
	return &SandboxInfo{
		OrchestratorID:   orchestratorID,
		OrchestratorIP:   "10.0.0.1",
		ExecutionID:      executionID,
		StartedAt:        time.Unix(0, 0).UTC(),
		MaxLengthInHours: 1,
	}
}

func TestRedisSandboxCatalog(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	catalog := NewRedisSandboxCatalog(client)
	ctx := t.Context()

	t.Run("store then get round-trips the info", func(t *testing.T) {
		t.Parallel()

		id := "sbx-roundtrip"
		want := testSandboxInfo("exec-1", "orch-A")
		require.NoError(t, catalog.StoreSandbox(ctx, id, want, time.Minute))

		got, err := catalog.GetSandbox(ctx, id)
		require.NoError(t, err)
		require.Equal(t, want.ExecutionID, got.ExecutionID)
		require.Equal(t, want.OrchestratorID, got.OrchestratorID)
		require.Nil(t, got.NetworkPlacement)
	})

	t.Run("a routed record round-trips its placement", func(t *testing.T) {
		t.Parallel()

		id := "sbx-routed"
		want := testSandboxInfo("exec-1", "orch-A")
		want.LifecycleID = "lifecycle-1"
		want.NetworkPlacement = &NetworkPlacement{
			NetworkMode: NetworkModeFullRouter,
			RouterID:    "router-1",
			PortableIP:  "10.4.0.7",
		}
		require.NoError(t, catalog.StoreSandbox(ctx, id, want, time.Minute))

		got, err := catalog.GetSandbox(ctx, id)
		require.NoError(t, err)
		require.Equal(t, want, got)
		require.NoError(t, catalog.DeleteSandbox(ctx, id, "exec-1"))
	})

	t.Run("get on an absent key returns ErrSandboxNotFound", func(t *testing.T) {
		t.Parallel()

		_, err := catalog.GetSandbox(ctx, "sbx-absent")
		require.ErrorIs(t, err, ErrSandboxNotFound)
	})

	t.Run("delete removes the entry when the execution matches", func(t *testing.T) {
		t.Parallel()

		id := "sbx-delete-match"
		require.NoError(t, catalog.StoreSandbox(ctx, id, testSandboxInfo("exec-1", "orch-A"), time.Minute))
		require.NoError(t, catalog.DeleteSandbox(ctx, id, "exec-1"))

		_, err := catalog.GetSandbox(ctx, id)
		require.ErrorIs(t, err, ErrSandboxNotFound)
	})

	t.Run("delete keeps an entry owned by a different execution", func(t *testing.T) {
		t.Parallel()

		// The compare-and-delete guard: a stale teardown for exec-1 must not remove
		// the routing entry a newer exec-2 stored under the same id.
		id := "sbx-delete-mismatch"
		require.NoError(t, catalog.StoreSandbox(ctx, id, testSandboxInfo("exec-2", "orch-B"), time.Minute))
		require.NoError(t, catalog.DeleteSandbox(ctx, id, "exec-1"))

		got, err := catalog.GetSandbox(ctx, id)
		require.NoError(t, err)
		require.Equal(t, "exec-2", got.ExecutionID)
		require.Equal(t, "orch-B", got.OrchestratorID)
	})

	t.Run("delete on an absent key is a no-op", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, catalog.DeleteSandbox(ctx, "sbx-never-stored", "exec-1"))
	})

	t.Run("keys live under the routing prefix", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, "sandbox:routing:sbx-1", catalog.getCatalogKey("sbx-1"))
	})
}

func TestDeleteSandboxReturnsRedisError(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	require.NoError(t, client.Close())
	broken := NewRedisSandboxCatalog(client)
	ctx := t.Context()

	require.Error(t, broken.DeleteSandbox(ctx, "sbx-closed", "exec-1"))
}

func TestDeleteIfSameExecutionOutcomes(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	catalog := NewRedisSandboxCatalog(client)
	ctx := t.Context()

	run := func(t *testing.T, id, executionID string) int {
		t.Helper()
		outcome, err := deleteIfSameExecution.Run(ctx, client, []string{catalog.getCatalogKey(id)}, executionID).Int()
		require.NoError(t, err)

		return outcome
	}

	t.Run("absent key", func(t *testing.T) {
		t.Parallel()

		require.Equal(t, catalogDeleteAbsent, run(t, "sbx-outcome-absent", "exec-1"))
	})

	t.Run("matching execution deletes", func(t *testing.T) {
		t.Parallel()

		id := "sbx-outcome-match"
		require.NoError(t, catalog.StoreSandbox(ctx, id, testSandboxInfo("exec-1", "orch-A"), time.Minute))

		require.Equal(t, catalogDeleteDeleted, run(t, id, "exec-1"))
		_, err := catalog.GetSandbox(ctx, id)
		require.ErrorIs(t, err, ErrSandboxNotFound)
	})

	t.Run("different execution is a mismatch and keeps the entry", func(t *testing.T) {
		t.Parallel()

		id := "sbx-outcome-mismatch"
		require.NoError(t, catalog.StoreSandbox(ctx, id, testSandboxInfo("exec-2", "orch-B"), time.Minute))

		require.Equal(t, catalogDeleteMismatch, run(t, id, "exec-1"))
		got, err := catalog.GetSandbox(ctx, id)
		require.NoError(t, err)
		require.Equal(t, "exec-2", got.ExecutionID)
	})

	t.Run("unreadable value is reported and kept", func(t *testing.T) {
		t.Parallel()

		id := "sbx-outcome-unreadable"
		require.NoError(t, client.Set(ctx, catalog.getCatalogKey(id), "not-json", time.Minute).Err())

		require.Equal(t, catalogDeleteUnreadable, run(t, id, "exec-1"))
		require.EqualValues(t, 1, client.Exists(ctx, catalog.getCatalogKey(id)).Val())
	})

	t.Run("valid JSON without an execution_id is unreadable, not a mismatch", func(t *testing.T) {
		t.Parallel()

		// A table that decodes but has no execution_id must not be mislabeled as an
		// execution-mismatch (which would read as "a newer execution owns it").
		id := "sbx-outcome-no-execid"
		require.NoError(t, client.Set(ctx, catalog.getCatalogKey(id), `{"orchestrator_id":"orch-A"}`, time.Minute).Err())

		require.Equal(t, catalogDeleteUnreadable, run(t, id, "exec-1"))
		require.EqualValues(t, 1, client.Exists(ctx, catalog.getCatalogKey(id)).Val())
	})
}
