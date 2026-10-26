package sandbox_catalog

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestDeleteSandboxIfOwnerMatchesAndIsIdempotent(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	catalog := NewRedisSandboxCatalog(client)
	ctx := t.Context()
	id := "sbx-owner-delete"
	owner := SandboxOwner{ExecutionID: "exec-1", LifecycleID: "life-1", OrchestratorID: "orch-A"}

	outcome, err := catalog.DeleteSandboxIfOwner(ctx, id, owner)
	require.NoError(t, err)
	require.Equal(t, DeleteOutcomeAbsent, outcome)

	info := testSandboxInfo(owner.ExecutionID, owner.OrchestratorID)
	info.LifecycleID = owner.LifecycleID
	require.NoError(t, catalog.StoreSandbox(ctx, id, info, time.Minute))

	outcome, err = catalog.DeleteSandboxIfOwner(ctx, id, owner)
	require.NoError(t, err)
	require.Equal(t, DeleteOutcomeDeleted, outcome)
	_, err = catalog.GetSandbox(ctx, id)
	require.ErrorIs(t, err, ErrSandboxNotFound)

	outcome, err = catalog.DeleteSandboxIfOwner(ctx, id, owner)
	require.NoError(t, err)
	require.Equal(t, DeleteOutcomeAbsent, outcome)
}

func TestDeleteSandboxIfOwnerKeepsDifferentOwner(t *testing.T) {
	t.Parallel()

	for name, owner := range map[string]SandboxOwner{
		"different execution":    {ExecutionID: "exec-old", LifecycleID: "life-1", OrchestratorID: "orch-A"},
		"different lifecycle":    {ExecutionID: "exec-1", LifecycleID: "life-old", OrchestratorID: "orch-A"},
		"different orchestrator": {ExecutionID: "exec-1", LifecycleID: "life-1", OrchestratorID: "orch-old"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			client := redis_utils.SetupInstance(t)
			catalog := NewRedisSandboxCatalog(client)
			ctx := t.Context()
			id := "sbx-owner-mismatch"
			key := catalog.getCatalogKey(id)
			value := `{"execution_id":"exec-1","lifecycle_id":"life-1","orchestrator_id":"orch-A","unknown_field":"preserve me"}`
			require.NoError(t, client.Set(ctx, key, value, time.Minute).Err())
			expiresAt, err := client.PExpireTime(ctx, key).Result()
			require.NoError(t, err)

			outcome, err := catalog.DeleteSandboxIfOwner(ctx, id, owner)
			require.NoError(t, err)
			require.Equal(t, DeleteOutcomeSuperseded, outcome)

			got, err := client.Get(ctx, key).Result()
			require.NoError(t, err)
			require.Equal(t, value, got)
			gotExpiresAt, err := client.PExpireTime(ctx, key).Result()
			require.NoError(t, err)
			require.Equal(t, expiresAt, gotExpiresAt)
		})
	}
}

func TestDeleteSandboxIfOwnerKeepsReplacementLifecycle(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	catalog := NewRedisSandboxCatalog(client)
	ctx := t.Context()
	id := "sbx-same-execution-aba"
	key := catalog.getCatalogKey(id)
	oldOwner := SandboxOwner{ExecutionID: "exec-1", LifecycleID: "life-1", OrchestratorID: "orch-A"}
	oldInfo := testSandboxInfo(oldOwner.ExecutionID, oldOwner.OrchestratorID)
	oldInfo.LifecycleID = oldOwner.LifecycleID
	require.NoError(t, catalog.StoreSandbox(ctx, id, oldInfo, time.Minute))

	// A resume-fresh checkpoint replaces the VM while retaining execution and node.
	newOwner := SandboxOwner{ExecutionID: "exec-1", LifecycleID: "life-2", OrchestratorID: "orch-A"}
	newInfo := testSandboxInfo(newOwner.ExecutionID, newOwner.OrchestratorID)
	newInfo.LifecycleID = newOwner.LifecycleID
	require.NoError(t, catalog.StoreSandbox(ctx, id, newInfo, 2*time.Minute))
	value, err := client.Get(ctx, key).Result()
	require.NoError(t, err)
	expiresAt, err := client.PExpireTime(ctx, key).Result()
	require.NoError(t, err)

	// Delayed cleanup from the original VM must not remove the replacement.
	outcome, err := catalog.DeleteSandboxIfOwner(ctx, id, oldOwner)
	require.NoError(t, err)
	require.Equal(t, DeleteOutcomeSuperseded, outcome)
	got, err := client.Get(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, value, got)
	gotExpiresAt, err := client.PExpireTime(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, expiresAt, gotExpiresAt)

	outcome, err = catalog.DeleteSandboxIfOwner(ctx, id, newOwner)
	require.NoError(t, err)
	require.Equal(t, DeleteOutcomeDeleted, outcome)
	_, err = catalog.GetSandbox(ctx, id)
	require.ErrorIs(t, err, ErrSandboxNotFound)
}

func TestDeleteSandboxIfOwnerKeepsUnverifiableRecord(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"legacy record":  `{"execution_id":"exec-1","orchestrator_id":"orch-A"}`,
		"malformed JSON": `{"execution_id":"exec-1"`,
		"JSON null":      `null`,
		"JSON array":     `[]`,
		"JSON string":    `"exec-1"`,
		"JSON number":    `1`,
		"JSON boolean":   `true`,
	}
	for _, field := range []string{"execution_id", "lifecycle_id", "orchestrator_id"} {
		for name, invalidValue := range map[string]any{
			"missing": nil,
			"empty":   "",
			"null":    nil,
			"number":  1,
			"boolean": true,
			"array":   []string{"value"},
			"object":  map[string]string{"id": "value"},
		} {
			info := map[string]any{"execution_id": "exec-1", "lifecycle_id": "life-1", "orchestrator_id": "orch-A"}
			if name == "missing" {
				delete(info, field)
			} else {
				info[field] = invalidValue
			}
			value, err := json.Marshal(info)
			require.NoError(t, err)
			cases[field+" "+name] = string(value)
		}
	}

	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			client := redis_utils.SetupInstance(t)
			catalog := NewRedisSandboxCatalog(client)
			ctx := t.Context()
			id := "sbx-unverifiable-owner"
			key := catalog.getCatalogKey(id)
			require.NoError(t, client.Set(ctx, key, value, time.Minute).Err())
			expiresAt, err := client.PExpireTime(ctx, key).Result()
			require.NoError(t, err)

			outcome, err := catalog.DeleteSandboxIfOwner(ctx, id, SandboxOwner{
				ExecutionID: "exec-1", LifecycleID: "life-1", OrchestratorID: "orch-A",
			})
			require.NoError(t, err)
			require.Equal(t, DeleteOutcomeUnverifiable, outcome)

			got, err := client.Get(ctx, key).Result()
			require.NoError(t, err)
			require.Equal(t, value, got)
			gotExpiresAt, err := client.PExpireTime(ctx, key).Result()
			require.NoError(t, err)
			require.Equal(t, expiresAt, gotExpiresAt)
		})
	}
}

func TestDeleteSandboxIfOwnerRejectsIncompleteExpectedOwner(t *testing.T) {
	t.Parallel()

	for name, owner := range map[string]SandboxOwner{
		"missing execution":    {LifecycleID: "life-1", OrchestratorID: "orch-A"},
		"missing lifecycle":    {ExecutionID: "exec-1", OrchestratorID: "orch-A"},
		"missing orchestrator": {ExecutionID: "exec-1", LifecycleID: "life-1"},
		"zero owner":           {},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			client := redis_utils.SetupInstance(t)
			catalog := NewRedisSandboxCatalog(client)
			ctx := t.Context()
			id := "sbx-incomplete-expected-owner"
			key := catalog.getCatalogKey(id)
			value := `{"execution_id":"exec-1","lifecycle_id":"life-1","orchestrator_id":"orch-A"}`
			require.NoError(t, client.Set(ctx, key, value, time.Minute).Err())
			expiresAt, err := client.PExpireTime(ctx, key).Result()
			require.NoError(t, err)

			outcome, err := catalog.DeleteSandboxIfOwner(ctx, id, owner)
			require.ErrorIs(t, err, ErrInvalidSandboxOwner)
			require.Empty(t, outcome)

			got, err := client.Get(ctx, key).Result()
			require.NoError(t, err)
			require.Equal(t, value, got)
			gotExpiresAt, err := client.PExpireTime(ctx, key).Result()
			require.NoError(t, err)
			require.Equal(t, expiresAt, gotExpiresAt)

			outcome, err = catalog.DeleteSandboxIfOwner(ctx, "sbx-absent", owner)
			require.ErrorIs(t, err, ErrInvalidSandboxOwner)
			require.Empty(t, outcome)
		})
	}
}

func TestDeleteSandboxIfOwnerKeepsLegacyRecordReadableAndLegacyDeletable(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	catalog := NewRedisSandboxCatalog(client)
	ctx := t.Context()
	id := "sbx-legacy-compatibility"
	key := catalog.getCatalogKey(id)
	value := `{"orchestrator_id":"orch-A","orchestrator_ip":"10.0.0.1","execution_id":"exec-1"}`
	require.NoError(t, client.Set(ctx, key, value, time.Minute).Err())

	outcome, err := catalog.DeleteSandboxIfOwner(ctx, id, SandboxOwner{
		ExecutionID: "exec-1", LifecycleID: "life-1", OrchestratorID: "orch-A",
	})
	require.NoError(t, err)
	require.Equal(t, DeleteOutcomeUnverifiable, outcome)
	info, err := catalog.GetSandbox(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "exec-1", info.ExecutionID)
	require.Equal(t, "orch-A", info.OrchestratorID)
	require.Empty(t, info.LifecycleID)
	require.Nil(t, info.NetworkPlacement)

	require.NoError(t, catalog.DeleteSandbox(ctx, id, "exec-1"))
	_, err = catalog.GetSandbox(ctx, id)
	require.ErrorIs(t, err, ErrSandboxNotFound)
}

func TestDeleteSandboxIfOwnerReturnsRedisError(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	require.NoError(t, client.Close())
	catalog := NewRedisSandboxCatalog(client)

	outcome, err := catalog.DeleteSandboxIfOwner(t.Context(), "sbx-closed", SandboxOwner{
		ExecutionID: "exec-1", LifecycleID: "life-1", OrchestratorID: "orch-A",
	})
	require.Error(t, err)
	require.Empty(t, outcome)
}
