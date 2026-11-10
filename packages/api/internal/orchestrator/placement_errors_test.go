package orchestrator

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// createRefusedStatus is a node's refusal with the typed detail the
// orchestrator attaches to a create no retry changes.
func createRefusedStatus(t *testing.T, code codes.Code, message string) error {
	t.Helper()

	return createRefusedStatusWith(t, code, message, http.StatusBadRequest)
}

func createRefusedStatusWith(t *testing.T, code codes.Code, message string, httpStatus uint32) error {
	t.Helper()

	st, err := status.New(code, message).WithDetails(&orchestrator.UserError{Code: orchestrator.UserErrorCode_SANDBOX_CREATE_REFUSED, Message: message, HttpStatus: httpStatus})
	require.NoError(t, err)

	return st.Err()
}

// autoPauseRefusedStatus is the node's refusal of a create whose auto-pause
// policy asks for a capture the guest cannot take: the pause refusal's
// detail beside the create-refused one.
func autoPauseRefusedStatus(t *testing.T, httpStatus uint32) error {
	t.Helper()

	message := "a timeout auto-pause that persists only the filesystem is not supported"
	st, err := status.New(codes.InvalidArgument, message).WithDetails(
		&errdetails.ErrorInfo{Reason: sandboxtypes.PauseRefusedUnsupportedReason, Domain: sandboxtypes.PauseRefusalDomain},
		&orchestrator.UserError{Code: orchestrator.UserErrorCode_SANDBOX_CREATE_REFUSED, Message: message, HttpStatus: httpStatus},
	)
	require.NoError(t, err)

	return st.Err()
}

func TestPlacementAPIError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		err           error
		wantCode      int
		wantErrorCode string
		wantMsg       string
		wantRetry     apierrors.Retryability
	}{
		{
			name:          "timeout",
			wantRetry:     apierrors.NoRetry,
			err:           placement.PlacementTimeoutError{Attempts: 2},
			wantCode:      http.StatusGatewayTimeout,
			wantErrorCode: "sandbox_placement_timeout",
			wantMsg:       "Failed to place sandbox: placement timed out after 2 attempt(s), please retry",
		},
		{
			name:          "zero-attempt timeout has no counter",
			err:           placement.PlacementTimeoutError{Attempts: 0},
			wantCode:      http.StatusGatewayTimeout,
			wantErrorCode: "sandbox_placement_timeout",
			wantMsg:       "Failed to place sandbox: placement timed out, please retry",
			wantRetry:     apierrors.CanRetry,
		},
		{
			name:          "no nodes available",
			err:           placement.NoNodesAvailableError{},
			wantCode:      http.StatusServiceUnavailable,
			wantErrorCode: "sandbox_capacity_unavailable",
			wantMsg:       "Failed to place sandbox: not enough capacity for the requested resources right now, please retry shortly",
			wantRetry:     apierrors.CanRetry,
		},
		{
			name:          "no eligible node",
			err:           placement.FailedToPlaceSandboxError{},
			wantCode:      http.StatusServiceUnavailable,
			wantErrorCode: "sandbox_no_compatible_node",
			wantMsg:       "Failed to place sandbox: no compatible node for this template's requirements",
			wantRetry:     apierrors.CanRetry,
		},
		{
			// Distinct from "no eligible node": the cluster is not busy, it is
			// too old, and the client must not be told to retry.
			name:          "feature unsupported by the cluster",
			wantRetry:     apierrors.NoRetry,
			err:           placement.UnsupportedFeatureError{Features: []string{"https-backend-ports"}, MinVersion: "0.11.0"},
			wantCode:      http.StatusServiceUnavailable,
			wantErrorCode: "sandbox_feature_unsupported",
			wantMsg:       "Failed to place sandbox: no available orchestrator at version 0.11.0 or above for a feature the request asked for",
		},
		{
			// A pause's files are still uploading when the resume is placed:
			// the node's answer is transient and keeps the retryable code.
			name:          "create failed forwards sandbox files not found",
			wantRetry:     apierrors.NoRetry,
			err:           placement.SandboxCreateError{Attempts: 3, LastErr: status.Error(codes.FailedPrecondition, "sandbox files for 'abc' not found")},
			wantCode:      http.StatusInternalServerError,
			wantErrorCode: "sandbox_create_failed",
			wantMsg:       "Failed to place sandbox: sandbox files for 'abc' not found",
		},
		{
			// A node that predates the typed detail: the message says the
			// refusal is for good.
			name:          "create refused by the vcpu bounds message",
			wantRetry:     apierrors.NoRetry,
			err:           placement.SandboxCreateError{Attempts: 3, LastErr: status.Error(codes.InvalidArgument, "vcpu 0 must be at least 1")},
			wantCode:      http.StatusInternalServerError,
			wantErrorCode: "sandbox_create_refused",
			wantMsg:       "Failed to place sandbox: vcpu 0 must be at least 1",
		},
		{
			// The typed detail decides on its own, whatever the message. Its
			// 400 is not the client's: no REST field reaches the refused
			// checks, so the answer is a server error.
			name:          "create refused by the typed detail",
			wantRetry:     apierrors.NoRetry,
			err:           placement.SandboxCreateError{Attempts: 3, LastErr: createRefusedStatus(t, codes.InvalidArgument, "invalid volume mount path")},
			wantCode:      http.StatusInternalServerError,
			wantErrorCode: "sandbox_create_refused",
			wantMsg:       "Failed to place sandbox: invalid volume mount path",
		},
		{
			// The auto-pause policy is a field of the request: the node's
			// 400 is the client's when the pause refusal's detail marks it.
			name:          "create refused for an auto-pause the guest cannot take is the client's",
			wantRetry:     apierrors.NoRetry,
			err:           placement.SandboxCreateError{Attempts: 1, LastErr: autoPauseRefusedStatus(t, http.StatusBadRequest)},
			wantCode:      http.StatusBadRequest,
			wantErrorCode: "sandbox_create_refused",
			wantMsg:       "Failed to place sandbox: a timeout auto-pause that persists only the filesystem is not supported",
		},
		{
			// The same detail with a server status keeps the server status:
			// only the node's 400 is trusted on it.
			name:          "create refused for an auto-pause the guest cannot take keeps a server status",
			wantRetry:     apierrors.NoRetry,
			err:           placement.SandboxCreateError{Attempts: 1, LastErr: autoPauseRefusedStatus(t, http.StatusInternalServerError)},
			wantCode:      http.StatusInternalServerError,
			wantErrorCode: "sandbox_create_refused",
			wantMsg:       "Failed to place sandbox: a timeout auto-pause that persists only the filesystem is not supported",
		},
		{
			// The orchestrator's createRefused helper answers every code
			// but InvalidArgument with a 500 detail; placement stops on
			// the detail whatever the code, so the API must report the
			// refusal rather than a failure the proxy retries for the
			// whole budget.
			name:          "create refused by the typed detail as Internal is final",
			wantRetry:     apierrors.NoRetry,
			err:           placement.SandboxCreateError{Attempts: 1, LastErr: createRefusedStatusWith(t, codes.Internal, "vcpu 0 must be at least 1", http.StatusInternalServerError)},
			wantCode:      http.StatusInternalServerError,
			wantErrorCode: "sandbox_create_refused",
			wantMsg:       "Failed to place sandbox: vcpu 0 must be at least 1",
		},
		{
			// The text-match shim stays behind the safe codes: without
			// the detail an Internal status is not read.
			name:          "create failed hides the vcpu bounds text on an unsafe code",
			wantRetry:     apierrors.NoRetry,
			err:           placement.SandboxCreateError{Attempts: 3, LastErr: status.Error(codes.Internal, "vcpu 0 must be at least 1")},
			wantCode:      http.StatusInternalServerError,
			wantErrorCode: "sandbox_create_failed",
			wantMsg:       "Failed to place sandbox: sandbox creation failed on 3 node(s), please retry; if the problem persists, contact us",
		},
		{
			name:          "create refused with a server status keeps it",
			wantRetry:     apierrors.NoRetry,
			err:           placement.SandboxCreateError{Attempts: 3, LastErr: createRefusedStatusWith(t, codes.FailedPrecondition, "node refuses this build for good", http.StatusNotImplemented)},
			wantCode:      http.StatusNotImplemented,
			wantErrorCode: "sandbox_create_refused",
			wantMsg:       "Failed to place sandbox: node refuses this build for good",
		},
		{
			name:          "create refused with a success status is a server error",
			wantRetry:     apierrors.NoRetry,
			err:           placement.SandboxCreateError{Attempts: 3, LastErr: createRefusedStatusWith(t, codes.FailedPrecondition, "node refuses this build for good", http.StatusOK)},
			wantCode:      http.StatusInternalServerError,
			wantErrorCode: "sandbox_create_refused",
			wantMsg:       "Failed to place sandbox: node refuses this build for good",
		},
		{
			name:          "create refused with a status beyond HTTP is a server error",
			wantRetry:     apierrors.NoRetry,
			err:           placement.SandboxCreateError{Attempts: 3, LastErr: createRefusedStatusWith(t, codes.FailedPrecondition, "node refuses this build for good", 1000)},
			wantCode:      http.StatusInternalServerError,
			wantErrorCode: "sandbox_create_refused",
			wantMsg:       "Failed to place sandbox: node refuses this build for good",
		},
		{
			// A node draining refuses with FailedPrecondition too; another
			// node takes the sandbox, so the code says to retry.
			name:          "create failed forwards a transient node refusal",
			wantRetry:     apierrors.NoRetry,
			err:           placement.SandboxCreateError{Attempts: 3, LastErr: status.Error(codes.FailedPrecondition, "node is draining")},
			wantCode:      http.StatusInternalServerError,
			wantErrorCode: "sandbox_create_failed",
			wantMsg:       "Failed to place sandbox: node is draining",
		},
		{
			name:          "create failed forwards a per-host vcpu refusal",
			wantRetry:     apierrors.NoRetry,
			err:           placement.SandboxCreateError{Attempts: 3, LastErr: status.Error(codes.InvalidArgument, "cannot cold-boot a VM with 8 vcpus on this host")},
			wantCode:      http.StatusInternalServerError,
			wantErrorCode: "sandbox_create_failed",
			wantMsg:       "Failed to place sandbox: cannot cold-boot a VM with 8 vcpus on this host",
		},
		{
			name:          "create failed forwards an unknown invalid-argument message",
			wantRetry:     apierrors.NoRetry,
			err:           placement.SandboxCreateError{Attempts: 3, LastErr: status.Error(codes.InvalidArgument, "invalid volume mount path")},
			wantCode:      http.StatusInternalServerError,
			wantErrorCode: "sandbox_create_failed",
			wantMsg:       "Failed to place sandbox: invalid volume mount path",
		},
		{
			name:          "create failed hides unsafe status message",
			wantRetry:     apierrors.NoRetry,
			err:           placement.SandboxCreateError{Attempts: 3, LastErr: status.Error(codes.Internal, "node ip-10-0-0-1: disk full")},
			wantCode:      http.StatusInternalServerError,
			wantErrorCode: "sandbox_create_failed",
			wantMsg:       "Failed to place sandbox: sandbox creation failed on 3 node(s), please retry; if the problem persists, contact us",
		},
		{
			name:          "create failed hides non-grpc error",
			wantRetry:     apierrors.NoRetry,
			err:           placement.SandboxCreateError{Attempts: 2, LastErr: errors.New("dial tcp 10.0.0.1: connection refused")},
			wantCode:      http.StatusInternalServerError,
			wantErrorCode: "sandbox_create_failed",
			wantMsg:       "Failed to place sandbox: sandbox creation failed on 2 node(s), please retry; if the problem persists, contact us",
		},
		{
			name:          "wrapped typed error still classified",
			err:           fmt.Errorf("failed to place sandbox: %w", placement.NoNodesAvailableError{}),
			wantCode:      http.StatusServiceUnavailable,
			wantErrorCode: "sandbox_capacity_unavailable",
			wantMsg:       "Failed to place sandbox: not enough capacity for the requested resources right now, please retry shortly",
			wantRetry:     apierrors.CanRetry,
		},
		{
			name:          "unknown error keeps generic message",
			wantRetry:     apierrors.NoRetry,
			err:           errors.New("boom"),
			wantCode:      http.StatusInternalServerError,
			wantErrorCode: "internal_server_error",
			wantMsg:       "Failed to place sandbox",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			apiErr := placementAPIError(tt.err)

			require.NotNil(t, apiErr)
			assert.Equal(t, tt.wantCode, apiErr.Code)
			assert.Equal(t, tt.wantErrorCode, apiErr.ErrorCode)
			assert.Equal(t, tt.wantMsg, apiErr.ClientMsg)
			assert.Equal(t, tt.wantRetry, apiErr.Retryable)
			assert.ErrorContains(t, apiErr.Err, tt.err.Error())
		})
	}
}
