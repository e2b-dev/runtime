package steps

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/builderrors"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
)

type fakeGuest struct {
	exitAfter time.Duration
	exitInfo  *fc.ExitInfo
	exited    chan struct{}
	waited    bool
}

func newFakeGuest(exitAfter time.Duration, info *fc.ExitInfo) *fakeGuest {
	g := &fakeGuest{exitAfter: exitAfter, exitInfo: info, exited: make(chan struct{})}
	if exitAfter >= 0 {
		time.AfterFunc(exitAfter, func() { close(g.exited) })
	}

	return g
}

func (g *fakeGuest) Exited() bool {
	select {
	case <-g.exited:
		return true
	default:
		return false
	}
}

func (g *fakeGuest) Wait(ctx context.Context) error {
	g.waited = true
	select {
	case <-g.exited:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *fakeGuest) FirecrackerExit() *fc.ExitInfo { return g.exitInfo }

var (
	resetExit  = &fc.ExitInfo{Exited: true, MetricsRead: true, MetricsAwaited: true, GuestResets: 1}
	cleanExit  = &fc.ExitInfo{Exited: true}
	killedExit = &fc.ExitInfo{Signaled: true, Signal: syscall.SIGKILL}
	crashExit  = &fc.ExitInfo{Exited: true, MetricsRead: true, MetricsAwaited: true, GuestResets: 1, GuestCrashes: 1, GuestCrashCode: 0x7e}
)

func unavailable() error {
	return fmt.Errorf("command failed: %w", connect.NewError(connect.CodeUnavailable, errors.New("the connection to sandbox ended before the stream completed")))
}

func TestGuestEndedRestartAfterGrace(t *testing.T) {
	t.Parallel()

	guest := newFakeGuest(50*time.Millisecond, resetExit)
	err := guestEnded(t.Context(), guest, unavailable())

	require.ErrorIs(t, err, ErrStepEndedGuest)
	assert.True(t, guest.waited, "the lost stream waits for the exit")

	asUser := phases.NewPhaseBuildError(phases.PhaseMeta{}, err)
	assert.True(t, builderrors.IsUserError(asUser))
	assert.Contains(t, builderrors.UnwrapUserError(asUser).GetMessage(), "a restart or shutdown inside a step is not supported")
}

func TestGuestEndedCommandFailureIsNotWaitedFor(t *testing.T) {
	t.Parallel()

	guest := newFakeGuest(-1, nil)
	exitStatus := errors.New("exit status 1")

	assert.Same(t, exitStatus, guestEnded(t.Context(), guest, exitStatus))
	assert.False(t, guest.waited)
}

func TestGuestEndedRunningGuestKeepsTheError(t *testing.T) {
	t.Parallel()

	// Ends the wait before guestEndGrace does.
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	guest := newFakeGuest(-1, nil)
	err := unavailable()

	assert.Same(t, err, guestEnded(ctx, guest, err))
	assert.True(t, guest.waited)
}

func TestGuestEndedHostKillIsNotTheStep(t *testing.T) {
	t.Parallel()

	guest := newFakeGuest(0, killedExit)
	require.Eventually(t, guest.Exited, time.Second, time.Millisecond)

	err := unavailable()
	assert.Same(t, err, guestEnded(t.Context(), guest, err))
}

func TestGuestEndedUncountedCleanExitIsTheStep(t *testing.T) {
	t.Parallel()

	guest := newFakeGuest(0, cleanExit)
	require.Eventually(t, guest.Exited, time.Second, time.Millisecond)

	require.ErrorIs(t, guestEnded(t.Context(), guest, unavailable()), ErrStepEndedGuest)
}

// A guest that crashed and reset during the step did not do what the step asked:
// the error names the crash and is not the step's.
func TestGuestEndedCrashIsNotTheStep(t *testing.T) {
	t.Parallel()

	guest := newFakeGuest(0, crashExit)
	require.Eventually(t, guest.Exited, time.Second, time.Millisecond)

	err := guestEnded(t.Context(), guest, unavailable())
	require.NotErrorIs(t, err, ErrStepEndedGuest)
	assert.Contains(t, err.Error(), "crash code 0x7e")
}
