package orchestrator

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// unsupportedPauseErr is the orchestrator's refusal of a pause the guest
// cannot take, as the filesystem-only preflight answers it.
func unsupportedPauseErr() error {
	st, err := status.New(codes.FailedPrecondition, "the guest offers no filesystem quiesce: a filesystem-only pause is not supported").WithDetails(&errdetails.ErrorInfo{
		Reason: sandboxtypes.PauseRefusedUnsupportedReason,
		Domain: sandboxtypes.PauseRefusalDomain,
	})
	if err != nil {
		panic(err)
	}

	return st.Err()
}

// Only the detailed FailedPrecondition is the unsupported refusal; a plain
// one keeps today's handling, and ResourceExhausted stays the retryable one.
func TestPauseRefusal(t *testing.T) {
	t.Parallel()

	require.ErrorIs(t, pauseRefusal(unsupportedPauseErr()), PauseUnsupportedError{})
	require.ErrorIs(t, pauseRefusal(status.Error(codes.ResourceExhausted, "busy")), PauseQueueExhaustedError{})
	require.NoError(t, pauseRefusal(status.Error(codes.FailedPrecondition, "envd too old")), "a plain FailedPrecondition is not a refusal")

	other, err := status.New(codes.FailedPrecondition, "x").WithDetails(&errdetails.ErrorInfo{Reason: "SOMETHING_ELSE", Domain: sandboxtypes.PauseRefusalDomain})
	require.NoError(t, err)
	require.NoError(t, pauseRefusal(other.Err()), "another reason in the domain is not the refusal")
	require.NoError(t, pauseRefusal(status.Error(codes.Internal, "boom")))
	require.NoError(t, pauseRefusal(errors.New("not a status")))

	require.ErrorIs(t, pauseRefusalOf(fmt.Errorf("wrapped: %w", PauseUnsupportedError{})), PauseUnsupportedError{})
	require.ErrorIs(t, pauseRefusalOf(fmt.Errorf("wrapped: %w", PauseQueueExhaustedError{})), PauseQueueExhaustedError{})
	assert.NoError(t, pauseRefusalOf(errors.New("other")))
}
