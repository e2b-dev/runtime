package utils

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestContextExtendedTo_CallerThatStaysKeepsItsOwnEnd(t *testing.T) {
	t.Parallel()

	ctx, cancel := ContextExtendedTo(t.Context(), time.Now().Add(50*time.Millisecond))
	defer cancel()

	time.Sleep(150 * time.Millisecond)
	require.NoError(t, ctx.Err(), "ended at the instant although the caller stayed")
}

// On synctest's clock the end is observed a millisecond before the instant
// and then on it, so a slow runner cannot make a correct end look early.
func TestContextExtendedTo_CallerThatLeavesEarlyIsExtendedToTheInstant(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		caller, leave := context.WithCancel(t.Context())
		until := time.Now().Add(time.Second)
		ctx, cancel := ContextExtendedTo(caller, until)
		defer cancel()

		leave()
		time.Sleep(time.Second - time.Millisecond)
		synctest.Wait()
		require.NoError(t, ctx.Err(), "ended with the caller before the instant")

		<-ctx.Done()
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		require.ErrorIs(t, context.Cause(ctx), context.DeadlineExceeded)
		require.Equal(t, until, time.Now(), "ended before or after the instant")
	})
}

func TestContextExtendedTo_CallerThatLeavesAfterTheInstantEndsAtOnce(t *testing.T) {
	t.Parallel()

	cause := errors.New("caller left")
	caller, leave := context.WithCancelCause(t.Context())
	ctx, cancel := ContextExtendedTo(caller, time.Now().Add(-time.Second))
	defer cancel()

	leave(cause)
	select {
	case <-ctx.Done():
		require.ErrorIs(t, context.Cause(ctx), cause)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "did not end with the caller")
	}
}

func TestContextExtendedTo_CancelEndsIt(t *testing.T) {
	t.Parallel()

	ctx, cancel := ContextExtendedTo(t.Context(), time.Now().Add(time.Hour))
	cancel()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}
