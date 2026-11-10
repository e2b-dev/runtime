//go:build linux

package server

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
)

// A Delete that takes the original while a late power-off gives it back keeps
// its kill: the checkpointing stop reason is cleared before the entry is
// published, so the kill is recorded and the Stop that follows releases the
// power-off hold instead of leaving the disk, the slot and the template pin
// held outside the live map.
func TestCheckpointResumeFresh_DeleteDuringThePowerOffRestoreKeepsItsKill(t *testing.T) {
	t.Parallel()

	s, sbx, steps, _ := slowResumeServer(t)
	defer steps.releaseAll()
	// The Firecracker exit already stopped the sandbox; Delete's Stop only
	// ends the hold.
	sbx.MarkStoppedForTest()
	sbx.MarkExitedForTest(nil)
	sbx.SetChecks(sandbox.NewChecks(sbx))
	var hold <-chan struct{}
	steps.onSnapshot = func() { hold = sbx.HoldPoweredOff() }
	steps.snapshotErr = sandbox.ErrPoweredOffHasNoMemory

	// The restoration is held between publishing the entry and returning,
	// and the Delete between recording its kill and its Stop (its health
	// check), so the two overlap as the race needs.
	inserted, releaseInsert := make(chan struct{}), make(chan struct{})
	s.sandboxFactory.Sandboxes.Subscribe(insertProbe{started: inserted, release: releaseInsert})
	deleteAtHealth, releaseHealth := make(chan struct{}), make(chan struct{})
	var once sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		once.Do(func() { close(deleteAtHealth) })
		<-releaseHealth
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	sbx.SetEnvdServerURLForTest(srv.URL)

	done := startSlowCheckpoint(t.Context(), s, steps, sbx)
	select {
	case <-inserted:
	case <-time.After(5 * time.Second):
		t.Fatal("the checkpoint never published the restored original")
	}
	deleted := make(chan error, 1)
	go func() {
		_, err := s.Delete(t.Context(), deleteRequest(sbx, "test-kill"))
		deleted <- err
	}()
	select {
	case <-deleteAtHealth:
	case <-time.After(5 * time.Second):
		t.Fatal("the Delete never recorded its kill")
	}

	close(releaseInsert)
	assert.Equal(t, codes.FailedPrecondition, status.Code(awaitCheckpoint(t, done)))
	assert.Equal(t, sandbox.StopReasonKilled, sbx.GetStopReason(), "the restoration keeps the concurrent Delete's reason")
	close(releaseHealth)
	require.NoError(t, <-deleted)

	// The same idempotent Stop the Delete runs, joined here.
	require.NoError(t, sbx.Stop(t.Context()))
	select {
	case <-hold:
	default:
		t.Error("the Delete succeeded but the power-off hold is still blocked")
	}
	_, live := s.sandboxFactory.Sandboxes.Get(sbx.Runtime.SandboxID)
	assert.False(t, live, "the killed sandbox left the live map")
}
