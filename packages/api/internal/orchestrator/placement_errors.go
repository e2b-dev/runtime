package orchestrator

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// The "Failed to place sandbox" prefix is load-bearing (clients may match on it).
const placementMessagePrefix = "Failed to place sandbox"

// Semantic error codes emitted in APIError.ErrorCode for placement failures.
// error_code is an open string set on the wire (see the generated API type); these
// name the codes this package emits so the supported set has one source of truth.
// The placement codes live in the shared gRPC package so the API and the
// proxy name them from one place; the proxy refuses to retry only
// feature_unsupported and treats the others as failures that clear.
const (
	errCodePlacementTimeout    = proxygrpc.PlacementTimeout
	errCodeCapacityUnavailable = proxygrpc.PlacementCapacityUnavailable
	errCodeNoCompatibleNode    = proxygrpc.PlacementNoCompatibleNode
	errCodeFeatureUnsupported  = proxygrpc.PlacementFeatureUnsupported
	errCodeSandboxCreateFailed = proxygrpc.PlacementCreateFailed
	// A create the node refused for a reason no other node and no retry
	// changes (missing files, a vcpu request out of bounds), unlike
	// errCodeSandboxCreateFailed, which covers a node that could not take
	// it this time.
	errCodeSandboxCreateRefused = proxygrpc.PlacementCreateRefused
	errCodeInternalServer       = "internal_server_error"
)

// PlacementErrorCodes is every semantic error code placementAPIError can return.
var PlacementErrorCodes = []string{
	errCodePlacementTimeout,
	errCodeCapacityUnavailable,
	errCodeNoCompatibleNode,
	errCodeFeatureUnsupported,
	errCodeSandboxCreateFailed,
	errCodeSandboxCreateRefused,
	errCodeSandboxMemoryRecordMismatch,
	errCodeInternalServer,
}

// Codes whose status messages carry no internal detail and are safe to forward.
var safePlacementCreateCodes = map[codes.Code]struct{}{
	codes.FailedPrecondition: {},
	codes.InvalidArgument:    {},
}

// terminalCreateMessages are the node's refusals no retry changes, matched by
// text for nodes that predate the typed detail (createRefused): the vcpu
// bounds. Nodes refuse with the same codes for transient reasons too
// (draining, this host's vcpu limit, a pause's files still uploading), which
// stay retryable.
var terminalCreateMessages = []string{
	"must be at least 1",
	"a VM can have",
	"must be 1..max_vcpus",
	"requested vcpu exceeds snapshot capacity",
}

// createRefusal returns the typed detail of a create the node refused for
// good, or nil when the status carries none.
func createRefusal(st *status.Status) *orchestrator.UserError {
	for _, detail := range st.Details() {
		if userErr, ok := detail.(*orchestrator.UserError); ok && userErr.GetCode() == orchestrator.UserErrorCode_SANDBOX_CREATE_REFUSED {
			return userErr
		}
	}

	return nil
}

// refusalHTTPStatus is the status a refusal for good answers with: the
// node's own, trusted as a client error only for a field the request
// carried. The auto-pause policy is one, and the node marks its refusal
// with the pause refusal's detail; a memory refusal is decided before this
// (placementAPIError), by whether the caller named the size. A create's vcpu
// comes from the template build row and max_vcpus is never set from REST, so
// any other 4xx is a stored-data defect, answered as 500 like a status
// outside 400..599.
func refusalHTTPStatus(st *status.Status, refusal *orchestrator.UserError) int {
	code := int(refusal.GetHttpStatus())
	if code == http.StatusBadRequest && unsupportedPauseRefusal(st) {
		return code
	}
	if code < http.StatusInternalServerError || code > 599 {
		return http.StatusInternalServerError
	}

	return code
}

// The node's refusals of a memory_mb it cannot take, by the reason it names
// ("memory hotplug refused (<reason>): ..."), and the error code each answers with.
var memoryRefusalCodes = map[string]string{
	"memory hotplug refused (shrink)":          errCodeSandboxMemoryShrinkUnsupported,
	"memory hotplug refused (invalid_request)": errCodeSandboxMemoryOutOfRange,
}

// memoryRefusalCode is the sandbox_memory_* error code of a node's 400 refusal
// of the requested memory, or "" for any other refusal.
func memoryRefusalCode(refusal *orchestrator.UserError) string {
	if refusal.GetHttpStatus() != http.StatusBadRequest {
		return ""
	}
	for prefix, code := range memoryRefusalCodes {
		if strings.HasPrefix(refusal.GetMessage(), prefix) {
			return code
		}
	}

	return ""
}

// memoryRefusalMessage is the client message for a node's memory refusal: the
// node's own text names its internal sizes, so the API states the rule instead.
func memoryRefusalMessage(code string) string {
	if code == errCodeSandboxMemoryShrinkUnsupported {
		return "it is below the memory the sandbox's snapshot holds, and a sandbox's memory can only grow"
	}

	return "it is not the template's memoryMB plus whole 128 MiB slots within its maxMemoryMB"
}

// unsupportedPauseRefusal reports a status that carries the orchestrator's
// detail of a capture the guest cannot take: a filesystem-only pause or
// checkpoint, or a create whose auto-pause policy asks for one.
func unsupportedPauseRefusal(st *status.Status) bool {
	for _, d := range st.Details() {
		info, ok := d.(*errdetails.ErrorInfo)
		if ok && info.GetDomain() == sandboxtypes.PauseRefusalDomain && info.GetReason() == sandboxtypes.PauseRefusedUnsupportedReason {
			return true
		}
	}

	return false
}

// createRefusedByMessage reports a create a node that predates the typed
// detail refused for good, by one of its known refusal messages.
func createRefusedByMessage(st *status.Status) bool {
	for _, message := range terminalCreateMessages {
		if strings.Contains(st.Message(), message) {
			return true
		}
	}

	return false
}

// memoryRefusalCodeOf is memoryRefusalCode for a placement error: the code a
// node's refusal of memory_mb answers with, or "" for any other error.
func memoryRefusalCodeOf(err error) string {
	var createErr placement.SandboxCreateError
	if !errors.As(err, &createErr) {
		return ""
	}
	st, ok := status.FromError(createErr.LastErr)
	if !ok {
		return ""
	}
	refusal := createRefusal(st)
	if refusal == nil {
		return ""
	}

	return memoryRefusalCode(refusal)
}

// placementAPIError translates a typed PlaceSandbox error into the
// customer-facing HTTP status, semantic error code and message. memoryNamed
// is whether the caller's request named memoryMB: a node's refusal of the size
// is the caller's error only then; for a size the API sent from its own record
// it is a stored-data defect.
func placementAPIError(err error, memoryNamed bool) *api.APIError {
	apiErr := func(httpCode int, errorCode string, message string) *api.APIError {
		return &api.APIError{
			Code:      httpCode,
			ErrorCode: errorCode,
			ClientMsg: message,
			Err:       fmt.Errorf("failed to place sandbox: %w", err),
			Retryable: apierrors.NoRetry,
		}
	}

	var (
		timeoutErr     placement.PlacementTimeoutError
		noNodesErr     placement.NoNodesAvailableError
		noEligibleErr  placement.FailedToPlaceSandboxError
		unsupportedErr placement.UnsupportedFeatureError
		createErr      placement.SandboxCreateError
	)

	switch {
	case errors.As(err, &timeoutErr):
		if timeoutErr.Attempts == 0 {
			result := apiErr(http.StatusGatewayTimeout, errCodePlacementTimeout, placementMessagePrefix+": placement timed out, please retry")
			result.Retryable = apierrors.CanRetry

			return result
		}

		return apiErr(http.StatusGatewayTimeout, errCodePlacementTimeout, fmt.Sprintf("%s: placement timed out after %d attempt(s), please retry", placementMessagePrefix, timeoutErr.Attempts))
	case errors.As(err, &noNodesErr):
		result := apiErr(http.StatusServiceUnavailable, errCodeCapacityUnavailable, placementMessagePrefix+": not enough capacity for the requested resources right now, please retry shortly")
		result.Retryable = apierrors.CanRetry

		return result
	case errors.As(err, &unsupportedErr):
		// Same status as the sibling refusals; the error code carries the
		// distinction, and the message offers no retry because none helps.
		return apiErr(http.StatusServiceUnavailable, errCodeFeatureUnsupported, fmt.Sprintf("%s: no available orchestrator at version %s or above for a feature the request asked for", placementMessagePrefix, unsupportedErr.MinVersion))
	case errors.As(err, &noEligibleErr):
		result := apiErr(http.StatusServiceUnavailable, errCodeNoCompatibleNode, placementMessagePrefix+": no compatible node for this template's requirements")
		result.Retryable = apierrors.CanRetry

		return result
	case errors.As(err, &createErr):
		if st, ok := status.FromError(createErr.LastErr); ok {
			// The typed detail decides whatever the code, as placement
			// stops on it whatever the code: its message is the node's
			// own wording for the client. The text-match shim for nodes
			// that predate it stays behind the safe codes.
			if refusal := createRefusal(st); refusal != nil {
				if code := memoryRefusalCode(refusal); code != "" {
					if memoryNamed {
						return apiErr(http.StatusBadRequest, code, "memoryMB does not fit the sandbox: "+memoryRefusalMessage(code))
					}

					return apiErr(http.StatusInternalServerError, errCodeSandboxMemoryRecordMismatch,
						placementMessagePrefix+": the memory recorded for this sandbox does not match its snapshot; if the problem persists, contact us")
				}

				return apiErr(refusalHTTPStatus(st, refusal), errCodeSandboxCreateRefused, fmt.Sprintf("%s: %s", placementMessagePrefix, refusal.GetMessage()))
			}
			if _, safe := safePlacementCreateCodes[st.Code()]; safe {
				code := errCodeSandboxCreateFailed
				if createRefusedByMessage(st) {
					code = errCodeSandboxCreateRefused
				}

				return apiErr(http.StatusInternalServerError, code, fmt.Sprintf("%s: %s", placementMessagePrefix, st.Message()))
			}
		}

		return apiErr(http.StatusInternalServerError, errCodeSandboxCreateFailed, fmt.Sprintf("%s: sandbox creation failed on %d node(s), please retry; if the problem persists, contact us", placementMessagePrefix, createErr.Attempts))
	default:
		return apiErr(http.StatusInternalServerError, errCodeInternalServer, placementMessagePrefix)
	}
}
