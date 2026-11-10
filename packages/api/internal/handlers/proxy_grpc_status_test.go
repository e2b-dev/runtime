package handlers

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
)

// The resume RPC's status carries the API's semantic error code as an
// ErrorInfo detail under the shared domain, so the proxy can tell a
// placement refusal no retry helps from a capacity shortage: both are 503
// and both map to Unavailable. It also carries the HTTP status the API
// answered with, so the proxy's page keeps a 500 the gRPC code folds away.
func TestResumeStatusError_CarriesTheErrorCode(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		apiErr    *api.APIError
		code      codes.Code
		reason    string
		permanent bool
		retryable bool
	}{
		{"capacity", &api.APIError{Code: http.StatusServiceUnavailable, ErrorCode: proxygrpc.PlacementCapacityUnavailable, ClientMsg: "not enough capacity"}, codes.Unavailable, proxygrpc.PlacementCapacityUnavailable, false, true},
		{"no compatible node", &api.APIError{Code: http.StatusServiceUnavailable, ErrorCode: proxygrpc.PlacementNoCompatibleNode, ClientMsg: "no compatible node"}, codes.Unavailable, proxygrpc.PlacementNoCompatibleNode, false, true},
		{"feature unsupported", &api.APIError{Code: http.StatusServiceUnavailable, ErrorCode: proxygrpc.PlacementFeatureUnsupported, ClientMsg: "no orchestrator at version"}, codes.Unavailable, proxygrpc.PlacementFeatureUnsupported, true, false},
		{"placement timeout", &api.APIError{Code: http.StatusGatewayTimeout, ErrorCode: proxygrpc.PlacementTimeout, ClientMsg: "placement timed out, please retry"}, codes.DeadlineExceeded, proxygrpc.PlacementTimeout, false, true},
		{"create failed", &api.APIError{Code: http.StatusInternalServerError, ErrorCode: proxygrpc.PlacementCreateFailed, ClientMsg: "sandbox creation failed, please retry"}, codes.Internal, proxygrpc.PlacementCreateFailed, false, true},
		{"create refused", &api.APIError{Code: http.StatusBadRequest, ErrorCode: proxygrpc.PlacementCreateRefused, ClientMsg: "vcpu 0 must be at least 1"}, codes.InvalidArgument, proxygrpc.PlacementCreateRefused, true, false},
		{"create refused as a server error", &api.APIError{Code: http.StatusInternalServerError, ErrorCode: proxygrpc.PlacementCreateRefused, ClientMsg: "vcpu 0 must be at least 1"}, codes.Internal, proxygrpc.PlacementCreateRefused, true, false},
		{"snapshot guest agent unresponsive", &api.APIError{Code: http.StatusConflict, ErrorCode: proxygrpc.SnapshotGuestAgentUnresponsive, ClientMsg: "resume it without memory"}, codes.AlreadyExists, proxygrpc.SnapshotGuestAgentUnresponsive, true, false},
		{"internal", &api.APIError{Code: http.StatusInternalServerError, ErrorCode: "internal_server_error", ClientMsg: "Failed to place sandbox"}, codes.Internal, "internal_server_error", false, false},
		{"team limit", &api.APIError{Code: http.StatusTooManyRequests, ClientMsg: "limit reached"}, codes.ResourceExhausted, "", false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := resumeStatusError(t.Context(), tt.apiErr)
			st, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, tt.code, st.Code())
			require.Equal(t, tt.apiErr.ClientMsg, st.Message())

			var reason string
			for _, detail := range st.Details() {
				if info, isInfo := detail.(*errdetails.ErrorInfo); isInfo {
					require.Equal(t, proxygrpc.ResumeErrorDomain, info.GetDomain())
					require.Equal(t, strconv.Itoa(tt.apiErr.Code), info.GetMetadata()[proxygrpc.ResumeErrorHTTPStatusKey])
					reason = info.GetReason()
				}
			}
			require.Equal(t, tt.reason, reason)
			require.Equal(t, tt.permanent, proxygrpc.PermanentPlacementRefusal(reason))
			require.Equal(t, tt.retryable, proxygrpc.RetryablePlacementFailure(reason))
		})
	}
}
