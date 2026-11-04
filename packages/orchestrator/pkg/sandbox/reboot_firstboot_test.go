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

func TestFirstBootRetryOnlyOnAGuestResetWithinTheBudget(t *testing.T) {
	t.Parallel()

	reset := errors.Join(ErrGuestResetBeforeEnvd, ErrFcProcessExited)

	assert.True(t, firstBootRetry(reset, 0, 3))
	assert.True(t, firstBootRetry(reset, 2, 3))
	assert.False(t, firstBootRetry(reset, 3, 3), "the budget is spent")
	assert.False(t, firstBootRetry(ErrFcProcessExited, 0, 3), "an exit that is not a reset")
	assert.False(t, firstBootRetry(nil, 0, 3), "a boot that succeeded")
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

// A reset attempt whose close failed is not booted again: the next attempt
// reopens the same disk, which the failed close may have left unflushed.
func TestEnvdWaitErrorRetriesOnlyACleanlyClosedReset(t *testing.T) {
	t.Parallel()

	wait := errors.Join(errors.New("wait for envd"), ErrFcProcessExited)
	closeFailed := errors.New("rootfs flush failed")

	assert.True(t, firstBootRetry(envdWaitError(wait, true, nil), 0, 3), "a reset whose close succeeded")

	err := envdWaitError(wait, true, closeFailed)
	assert.False(t, firstBootRetry(err, 0, 3), "a reset whose close failed")
	require.ErrorIs(t, err, closeFailed, "the close failure is returned")

	assert.False(t, firstBootRetry(envdWaitError(wait, false, nil), 0, 3), "not a reset")
	require.ErrorIs(t, envdWaitError(wait, false, closeFailed), closeFailed)
}
