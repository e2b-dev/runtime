package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

var killOpts = sandbox.RemoveOpts{Action: sandbox.StateActionKill, Reason: sandbox.KillReasonRequest}

func TestKillAndClaim_RunningSandboxIsKilledAndClaimed(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)

	claimID, killed, err := f.o.KillAndClaim(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, killOpts)
	require.NoError(t, err)
	assert.True(t, killed)
	assert.NotEmpty(t, claimID)

	_, _, err = f.o.sandboxStore.Reserve(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, -1)
	require.ErrorIs(t, err, sandbox.ErrSandboxKilled, "a resume must not start while the snapshot is deleted")
}

func TestKillAndClaim_KillsTheSandboxAnInFlightResumeStarts(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)
	require.NoError(t, f.o.RemoveSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, killOpts))
	finishStart, _, err := f.o.sandboxStore.Reserve(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, -1)
	require.NoError(t, err)
	require.NotNil(t, finishStart)

	go func() {
		time.Sleep(200 * time.Millisecond)
		resumed := f.sbx
		resumed.ExecutionID = uuid.NewString()
		assert.NoError(t, f.o.sandboxStore.Add(context.WithoutCancel(t.Context()), resumed, nil))
		finishStart(resumed, nil)
	}()

	claimID, killed, err := f.o.KillAndClaim(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, killOpts)
	require.NoError(t, err)
	assert.True(t, killed, "the sandbox the resume started must be killed")
	assert.NotEmpty(t, claimID)

	_, err = f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.ErrorIs(t, err, sandbox.ErrNotFound)
}

func TestKillAndClaim_ResumeThatNeverFinishes(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, nil)
	require.NoError(t, f.o.RemoveSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, killOpts))
	_, _, err := f.o.sandboxStore.Reserve(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, -1)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	claimID, killed, err := f.o.KillAndClaim(ctx, f.sbx.TeamID, f.sbx.SandboxID, killOpts)
	require.ErrorIs(t, err, ErrSandboxStillResuming)
	assert.False(t, killed)
	assert.Empty(t, claimID)
}
