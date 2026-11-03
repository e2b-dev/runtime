package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

// A user's filesystem-only pause that joins one the node refused as unsupported
// gets the permanent refusal from RemoveSandbox, the answer the handler turns
// into its 400, never the generic failure; a memory pause that joined may retry.
func TestRemoveSandbox_JoinerOfAnUnsupportedRefusal(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name           string
		filesystemOnly bool
		want           error
	}{
		{name: "filesystem-only joiner gets the permanent refusal", filesystemOnly: true, want: PauseUnsupportedError{}},
		{name: "memory joiner may retry", filesystemOnly: false, want: PauseQueueExhaustedError{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			read := make(chan struct{})
			f := newRefusalFixture(t, true, consts.LocalClusterID, unsupportedPauseErr(), &supersededWaiterHook{ready: read})
			ctx := t.Context()

			transition, _, finish, err := f.o.sandboxStore.StartRemoving(ctx, f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause, FilesystemOnly: true})
			require.NoError(t, err)

			joiner := make(chan error, 1)
			go func() {
				joinerCtx := context.WithValue(ctx, supersededWaiterContextKey{}, true)
				joiner <- f.o.RemoveSandbox(joinerCtx, f.sbx.TeamID, f.sbx.SandboxID, sandbox.RemoveOpts{Action: sandbox.StateActionPause, FilesystemOnly: tc.filesystemOnly})
			}()
			// Restore only once the joiner has read the in-flight transition, so it joins it.
			select {
			case <-read:
			case <-time.After(5 * time.Second):
				t.Fatal("the joiner never read the in-flight transition")
			}

			_, err = f.o.sandboxStore.RestoreRunning(ctx, transition, sandbox.RestoreOpts{})
			require.NoError(t, err)
			finish(ctx, &sandbox.RestoredTransitionError{Refusal: sandbox.PauseUnsupportedError{}})

			select {
			case jerr := <-joiner:
				require.ErrorIs(t, jerr, tc.want)
				require.NotErrorIs(t, jerr, ErrSandboxOperationFailed, "a joiner is never answered with the generic failure")
			case <-time.After(5 * time.Second):
				t.Fatal("joiner did not wake")
			}

			stored, err := f.o.sandboxStore.Get(ctx, f.sbx.TeamID, f.sbx.SandboxID)
			require.NoError(t, err)
			require.Equal(t, sandbox.StateRunning, stored.State)
		})
	}
}
