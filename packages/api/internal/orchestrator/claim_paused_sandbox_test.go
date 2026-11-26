package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	sandboxredis "github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/storage/redis"
)

func TestClaimPausedSandbox(t *testing.T) {
	t.Parallel()

	t.Run("a running sandbox is refused and left running", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, true, consts.LocalClusterID, nil)

		claimID, claimed, err := f.o.ClaimPausedSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, err)
		assert.False(t, claimed)
		assert.Empty(t, claimID)

		stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, err)
		assert.Equal(t, sandbox.StateRunning, stored.State)
	})

	t.Run("a paused sandbox is claimed and its resume blocked", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, true, consts.LocalClusterID, nil)
		require.NoError(t, f.o.RemoveSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, killOpts))

		claimID, claimed, err := f.o.ClaimPausedSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, err)
		assert.True(t, claimed)
		assert.NotEmpty(t, claimID)

		_, _, err = f.o.sandboxStore.Reserve(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, -1)
		require.ErrorIs(t, err, sandbox.ErrSandboxKilled)
	})

	t.Run("a resuming sandbox is refused", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, true, consts.LocalClusterID, nil)
		require.NoError(t, f.o.RemoveSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, killOpts))
		_, _, err := f.o.sandboxStore.Reserve(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, -1)
		require.NoError(t, err)

		_, claimed, err := f.o.ClaimPausedSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, err)
		assert.False(t, claimed)
	})

	t.Run("a record missing from the team index is refused and the claim released", func(t *testing.T) {
		t.Parallel()

		f := newRefusalFixture(t, true, consts.LocalClusterID, nil)
		require.NoError(t, f.client.SRem(t.Context(), sandboxredis.GetSandboxStorageTeamIndexKey(f.sbx.TeamID.String()), f.sbx.SandboxID).Err())

		_, claimed, err := f.o.ClaimPausedSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, err)
		assert.False(t, claimed)

		require.NoError(t, f.o.RemoveSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, killOpts))
		_, _, err = f.o.sandboxStore.Reserve(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, -1)
		require.NoError(t, err, "a refused claim must not block the next resume")
	})
}
