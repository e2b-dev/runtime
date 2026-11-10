package sandbox

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
)

// resettingProfile classifies a clean exit before envd as a guest reset.
type resettingProfile struct{ guest.Profile }

func (resettingProfile) Name() string  { return "resetting" }
func (resettingProfile) Label() string { return "resetting" }
func (resettingProfile) Reset() guest.Reset {
	return guest.Reset{CleanExitBeforeEnvdIsReset: true}
}

func TestIsGuestResetBeforeEnvdFollowsTheProfile(t *testing.T) {
	t.Parallel()

	exited := errors.Join(errors.New("wait for envd"), ErrFcProcessExited)
	clean := &fc.ExitInfo{ExitCode: 0, Exited: true}
	crashed := &fc.ExitInfo{ExitCode: 1, Exited: true}

	resetting := resettingProfile{guest.Default()}
	assert.True(t, isGuestResetBeforeEnvd(resetting, exited, clean))
	assert.True(t, isGuestResetBeforeEnvd(resetting, exited, &fc.ExitInfo{Exited: true, MetricsRead: true, GuestResets: 1}), "a counted reset")
	assert.True(t, isGuestResetBeforeEnvd(resetting, exited, &fc.ExitInfo{Exited: true, MetricsRead: true, GuestResets: 1, GuestCrashes: 1}), "a reset after a crash the guest reported")
	assert.False(t, isGuestResetBeforeEnvd(resetting, exited, &fc.ExitInfo{Exited: true, MetricsRead: true, GuestPowerOffs: 1}), "a power-off is not a reset")
	assert.False(t, isGuestResetBeforeEnvd(resetting, exited, crashed), "a crash is not a reset")
	assert.False(t, isGuestResetBeforeEnvd(resetting, errors.New("timeout"), clean), "the process must be gone")
	assert.False(t, isGuestResetBeforeEnvd(guest.Default(), exited, clean), "the default profile keeps a clean exit a failure")
	assert.True(t, isGuestResetBeforeEnvd(guest.Default(), exited, &fc.ExitInfo{Exited: true, MetricsRead: true, GuestResets: 1}), "a counted reset is a reset whatever the clean-exit rule says")
	assert.False(t, isGuestResetBeforeEnvd(guest.Default(), exited, &fc.ExitInfo{Exited: true}), "the clean-exit rule alone decides an exit with no counters")
}

func TestRebootEnvdWait(t *testing.T) {
	t.Parallel()

	linux := NewConfig(Config{})
	assert.Equal(t, rebootEnvdTimeoutFor(linux), rebootEnvdWait(linux, 0), "no override keeps the guest's cold-boot budget")
	assert.Equal(t, 2*rebootEnvdTimeout, rebootEnvdWait(linux, 2*rebootEnvdTimeout))
}

// A reset whose close failed is not marked as one, so a caller does not boot
// the same disk again: the failed close may have left it unflushed.
func TestEnvdWaitErrorMarksOnlyACleanlyClosedReset(t *testing.T) {
	t.Parallel()

	wait := errors.Join(errors.New("wait for envd"), ErrFcProcessExited)
	closeFailed := errors.New("rootfs flush failed")

	require.ErrorIs(t, envdWaitError(wait, true, nil), ErrGuestResetBeforeEnvd, "a reset whose close succeeded")

	err := envdWaitError(wait, true, closeFailed)
	require.NotErrorIs(t, err, ErrGuestResetBeforeEnvd, "a reset whose close failed")
	require.ErrorIs(t, err, closeFailed, "the close failure is returned")

	require.NotErrorIs(t, envdWaitError(wait, false, nil), ErrGuestResetBeforeEnvd, "not a reset")
	require.ErrorIs(t, envdWaitError(wait, false, closeFailed), closeFailed)
}
