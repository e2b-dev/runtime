package orchestrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

// The node's unsupported refusal (a filesystem-only pause of a guest without a filesystem quiesce)
// leaves the record present and Running, kills nothing, and answers the
// caller with the refusal, with the restore flag off: the flag gates the
// retryable restore's rollout, not a refusal that can never be admitted.
func TestRemoveSandbox_UnsupportedRefusalRestoresWithoutTheFlag(t *testing.T) {
	t.Parallel()

	for _, flag := range []bool{false, true} {
		f := newRefusalFixture(t, flag, consts.LocalClusterID, unsupportedPauseErr())

		err := f.o.RemoveSandbox(t.Context(), f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause, FilesystemOnly: true})
		require.ErrorIs(t, err, PauseUnsupportedError{}, "flag %t", flag)

		stored, err := f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
		require.NoError(t, err, "the record must survive an unsupported refusal (flag %t)", flag)
		assert.Equal(t, sandbox.StateRunning, stored.State)
		assert.WithinDuration(t, f.sbx.EndTime, stored.EndTime, time.Second, "the restore reinstates the expiry")
		assert.True(t, stored.RefusedSince.IsZero(), "a refusal that says nothing about the node opens no overstay episode")
		assert.WithinDuration(t, time.Now().Add(refusalRetryAfter), stored.RefusedUntil, time.Second, "the sweep is still held for the retry window")

		time.Sleep(300 * time.Millisecond)
		assert.Zero(t, f.recorder.stoppedCount(), "a restored sandbox must not emit stopped analytics")
		assert.Equal(t, map[string]int64{"restored/request": 1}, f.restoreOutcomes(t))
	}
}

// A FailedPrecondition without the refusal detail is handled as before: the
// record goes and the caller gets the generic failure.
func TestRemoveSandbox_PlainFailedPreconditionRemovesToday(t *testing.T) {
	t.Parallel()

	f := newRefusalFixture(t, true, consts.LocalClusterID, status.Error(codes.FailedPrecondition, "envd too old"))

	err := f.removePause(t)
	require.ErrorIs(t, err, ErrSandboxOperationFailed)

	_, err = f.o.sandboxStore.Get(t.Context(), f.sbx.TeamID, f.sbx.SandboxID)
	require.ErrorIs(t, err, sandbox.ErrNotFound, "a plain failure removes the record as before")
}
