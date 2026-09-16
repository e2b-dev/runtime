package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func TestCathedralLifecycleDispatchSurvivesCallerDisconnect(t *testing.T) {
	t.Parallel()

	parent, cancelParent := context.WithCancel(context.Background())
	dispatch, cancelDispatch := cathedralLifecycleDispatchContext(parent)
	t.Cleanup(cancelDispatch)
	cancelParent()

	select {
	case <-dispatch.Done():
		t.Fatalf("detached lifecycle dispatch inherited caller cancellation: %v", dispatch.Err())
	default:
	}
}

func TestHashCathedralLifecycleRequestBindsResourceExecutionAndIntent(t *testing.T) {
	t.Parallel()

	base := api.CathedralLifecycleOperationRequest{
		Operation:   api.CathedralLifecycleOperationRequestOperationDelete,
		ExecutionId: "exec-1",
	}
	first, err := hashCathedralLifecycleRequest("sbx-1", base)
	require.NoError(t, err)
	second, err := hashCathedralLifecycleRequest("sbx-1", base)
	require.NoError(t, err)
	differentExecution, err := hashCathedralLifecycleRequest("sbx-1", api.CathedralLifecycleOperationRequest{
		Operation:   api.CathedralLifecycleOperationRequestOperationDelete,
		ExecutionId: "exec-2",
	})
	require.NoError(t, err)
	differentSandbox, err := hashCathedralLifecycleRequest("sbx-2", base)
	require.NoError(t, err)
	explicitFalse := false
	equivalentDefault, err := hashCathedralLifecycleRequest("sbx-1", api.CathedralLifecycleOperationRequest{
		Operation:      api.CathedralLifecycleOperationRequestOperationDelete,
		ExecutionId:    "exec-1",
		FilesystemOnly: &explicitFalse,
	})
	require.NoError(t, err)
	explicitTrue := true
	differentFilesystemMode, err := hashCathedralLifecycleRequest("sbx-1", api.CathedralLifecycleOperationRequest{
		Operation:      api.CathedralLifecycleOperationRequestOperationDelete,
		ExecutionId:    "exec-1",
		FilesystemOnly: &explicitTrue,
	})
	require.NoError(t, err)

	assert.Len(t, first, 64)
	assert.Equal(t, first, second)
	assert.Equal(t, first, equivalentDefault)
	assert.NotEqual(t, first, differentExecution)
	assert.NotEqual(t, first, differentSandbox)
	assert.NotEqual(t, first, differentFilesystemMode)
}

func TestLifecycleOperationToAPIPreservesEvidenceAndCleanupDebt(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	errorCode := int32(503)
	errorMessage := "storage cleanup unconfirmed"
	remaining := int64(45_000)
	buildID := "build-1"
	got := lifecycleOperationToAPI(queries.CathedralSandboxLifecycleOperation{
		OperationKey: "lifecycle-1", OperationKind: "pause", SandboxID: "sbx-1",
		ExecutionID: "exec-1", State: "unknown", CleanupState: "failed",
		ExecutionRemovedAt: &now, SnapshotBuildID: &buildID,
		SnapshotCompletedAt: &now, RemainingLifetimeMs: &remaining,
		ErrorCode: &errorCode, ErrorMessage: &errorMessage,
	})

	assert.Equal(t, api.CathedralLifecycleOperationStateUnknown, got.State)
	assert.Equal(t, api.CathedralLifecycleOperationCleanupStateFailed, got.CleanupState)
	assert.Equal(t, "exec-1", got.ExecutionId)
	assert.Equal(t, &remaining, got.RemainingLifetimeMs)
	require.NotNil(t, got.ErrorCode)
	assert.Equal(t, 503, *got.ErrorCode)
}

func TestFrozenLifetimeMillisecondsMatchesSnapshotRounding(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int64(0), frozenLifetimeMilliseconds(0))
	assert.Equal(t, int64(0), frozenLifetimeMilliseconds(-time.Second))
	assert.Equal(t, int64(1000), frozenLifetimeMilliseconds(time.Millisecond))
	assert.Equal(t, int64(1000), frozenLifetimeMilliseconds(time.Second))
	assert.Equal(t, int64(2000), frozenLifetimeMilliseconds(time.Second+time.Nanosecond))
}
