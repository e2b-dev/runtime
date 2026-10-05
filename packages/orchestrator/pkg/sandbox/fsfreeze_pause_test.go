//go:build linux

package sandbox

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// TestGuestPrepareFsForPause_FreezeFailureRollsBackThaw verifies the pause
// rollback contract: when POST /fsfreeze fails during a filesystem-only pause,
// the cleanup that guestPrepareFsForPause registers invokes POST /fsthaw, so a
// rootfs the kernel may have already frozen is thawed instead of leaving the
// sandbox deadlocked.
//
// This pins the orchestrator wiring only. That the ioctls behind those endpoints
// really freeze and thaw a filesystem is covered on envd's side, by
// TestFreezeBlocksWritesAndThawReleasesThem in
// packages/envd/internal/services/fsfreeze.
func TestGuestPrepareFsForPause_FreezeFailureRollsBackThaw(t *testing.T) {
	t.Parallel()

	var freezeCalls, thawCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fsfreeze":
			freezeCalls.Add(1)
			// Freeze fails from the orchestrator's perspective (e.g. a timeout
			// after the kernel already began/finished freezing the rootfs).
			http.Error(w, "FIFREEZE /: simulated failure", http.StatusInternalServerError)
		case "/fsthaw":
			thawCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected path "+r.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	s := newFsFreezeSandbox(t, srv.URL)
	cleanup := NewCleanup()

	// During the pause: freeze fails, so guestPrepareFsForPause aborts the pause.
	frozen, err := s.guestPrepareFsForPause(t.Context(), cleanup, nil)
	require.Error(t, err, "a failed freeze must abort the filesystem-only pause")
	require.False(t, frozen, "a failed freeze must not report the rootfs as frozen")
	require.Equal(t, int32(1), freezeCalls.Load(), "freeze should have been attempted once")
	require.Zero(t, thawCalls.Load(), "thaw must not run before the pause actually aborts")

	// Pause's deferred error handler runs the cleanup chain on abort; that is
	// what fires the rollback thaw.
	require.NoError(t, cleanup.Run(t.Context()))
	require.Equal(t, int32(1), thawCalls.Load(), "an aborted pause must thaw the rootfs exactly once")
}

// TestGuestPrepareFsForPause_SuccessDoesNotThaw guards the other half of the
// contract: on a successful freeze the cleanup is NOT run (pause succeeds), so
// /fsthaw is never called — the frozen state is intended to be discarded by the
// reboot on resume, not thawed in place.
func TestGuestPrepareFsForPause_SuccessDoesNotThaw(t *testing.T) {
	t.Parallel()

	var freezeCalls, thawCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/fsfreeze":
			freezeCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case "/fsthaw":
			thawCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected path "+r.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	s := newFsFreezeSandbox(t, srv.URL)
	cleanup := NewCleanup()

	frozen, err := s.guestPrepareFsForPause(t.Context(), cleanup, nil)
	require.NoError(t, err)
	require.True(t, frozen, "a successful native freeze must report the rootfs as frozen")
	require.Equal(t, int32(1), freezeCalls.Load(), "freeze should have run once")
	require.Zero(t, thawCalls.Load(), "a successful freeze must not thaw (no abort)")
}

func newFsFreezeSandbox(t *testing.T, envdURL string) *Sandbox {
	t.Helper()

	ff, err := featureflags.NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	token := "test-token"
	s := &Sandbox{Metadata: &Metadata{
		Config: &Config{
			RamMB: 1024,
			Envd: EnvdMetadata{
				Version:     utils.MinEnvdVersionForFsFreeze, // gate: fsfreeze supported
				AccessToken: &token,
			},
		},
		Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "test-sandbox"},
	}}
	s.featureFlags = ff
	s.internalConfig.envdServerURLOverride = envdURL

	return s
}

// thawRootfs retries a failed /fsthaw twice with back-off and succeeds as soon
// as one attempt does.
func TestThawRootfs_RetriesThenSucceeds(t *testing.T) {
	t.Parallel()

	var thawCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fsthaw" {
			http.Error(w, "unexpected path "+r.URL.Path, http.StatusNotFound)

			return
		}
		if thawCalls.Add(1) < 3 {
			http.Error(w, "FITHAW /: simulated failure", http.StatusInternalServerError)

			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	s := newFsFreezeSandbox(t, srv.URL)
	require.NoError(t, s.thawRootfs(t.Context()))
	require.Equal(t, int32(3), thawCalls.Load(), "two failures then a success: three attempts")
}

// After the last retry fails the error is returned, so the caller can treat the
// sandbox as lost; it is never left silently frozen.
func TestThawRootfs_GivesUpAfterTheRetries(t *testing.T) {
	t.Parallel()

	var thawCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		thawCalls.Add(1)
		http.Error(w, "FITHAW /: simulated failure", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	s := newFsFreezeSandbox(t, srv.URL)
	require.Error(t, s.thawRootfs(t.Context()))
	require.Equal(t, int32(fsthawAttempts), thawCalls.Load(), "one call and two retries, then give up")
}

// Giving up tears the sandbox down as lost, tagged as a thaw failure so the
// kill is not booked as a failed resume.
func TestThawRootfsOrLose_TagsTheThawFailure(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "FITHAW /: simulated failure", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	s := newFsFreezeSandbox(t, srv.URL)
	s.cleanup = NewCleanup()

	err := s.thawRootfsOrLose(t.Context())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSandboxLost)
	require.ErrorIs(t, err, ErrRootfsThawFailed)
	require.Equal(t, StopReasonKilled, s.GetStopReason())
}
