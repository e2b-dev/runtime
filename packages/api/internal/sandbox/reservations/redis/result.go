package redis

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
)

// reservationResult is the serializable result stored in Redis for cross-instance communication.
type reservationResult struct {
	Sandbox sandboxtypes.Sandbox `json:"sandbox,omitzero"`
	Error   *reservationError    `json:"error,omitempty"`
	// LeaderLeft marks a result the leader published only because its own
	// context was cancelled (client disconnect / deadline), not a genuine
	// start failure. A waiter decodes it as ErrReservationLeaderLeft so it can
	// re-contend instead of adopting the leader's cancellation.
	LeaderLeft bool `json:"leader_left,omitempty"`
}

// reservationError preserves api.APIError fields for cross-instance error propagation.
type reservationError struct {
	Message   string `json:"message"`
	Code      int    `json:"code,omitempty"`
	ClientMsg string `json:"client_msg,omitempty"`
	ErrorCode string `json:"error_code,omitempty"`
}

func encodeResult(sbx sandboxtypes.Sandbox, err error) ([]byte, error) {
	result := reservationResult{
		Sandbox: sbx,
	}

	if err != nil {
		re := &reservationError{Message: err.Error()}
		if apiErr, ok := errors.AsType[*api.APIError](err); ok {
			re.Code = apiErr.Code
			re.ClientMsg = apiErr.ClientMsg
			re.ErrorCode = apiErr.ErrorCode
		}
		result.Error = re
		result.LeaderLeft = errors.Is(err, sandboxtypes.ErrReservationLeaderLeft)
	}

	return json.Marshal(result)
}

func decodeResult(data []byte) (sandboxtypes.Sandbox, error) {
	var result reservationResult
	if err := json.Unmarshal(data, &result); err != nil {
		return sandboxtypes.Sandbox{}, fmt.Errorf("failed to unmarshal reservation result: %w", err)
	}

	if result.LeaderLeft {
		// The leader gave up only because its own context was cancelled.
		// Surface the typed sentinel so a waiter with a live context can
		// re-contend; preserve the leader's message for the logs.
		msg := "leader left"
		if result.Error != nil {
			msg = result.Error.Message
		}

		return sandboxtypes.Sandbox{}, fmt.Errorf("%w: %s", sandboxtypes.ErrReservationLeaderLeft, msg)
	}

	if result.Error != nil {
		return sandboxtypes.Sandbox{}, reconstructError(result.Error)
	}

	return result.Sandbox, nil
}

// reconstructError rebuilds the appropriate error type from the serialized representation.
// If the error had an API code, it reconstructs an *api.APIError to preserve
// errors.As(err, &apiErr) behavior in create_instance.go.
func reconstructError(re *reservationError) error {
	if re.Code != 0 {
		return &api.APIError{
			Code:      re.Code,
			ClientMsg: re.ClientMsg,
			ErrorCode: re.ErrorCode,
			Err:       fmt.Errorf("%s", re.Message),
		}
	}

	return fmt.Errorf("%s", re.Message)
}
