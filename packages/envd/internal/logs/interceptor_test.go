package logs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"connectrpc.com/connect"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// levelOf runs getErrDebugLogEvent against err and returns the zerolog level
// the emitted event carried.
func levelOf(t *testing.T, err error) string {
	t.Helper()

	var buf bytesBuffer
	logger := zerolog.New(&buf)
	getErrDebugLogEvent(&logger, err).Msg("test")

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(buf.b, &parsed), "log line: %s", string(buf.b))

	lvl, _ := parsed["level"].(string)

	return lvl
}

// bytesBuffer is a tiny io.Writer so the test needs no extra deps beyond what
// the package already uses.
type bytesBuffer struct{ b []byte }

func (w *bytesBuffer) Write(p []byte) (int, error) {
	w.b = append(w.b, p...)

	return len(p), nil
}

func TestGetErrDebugLogEvent_Level(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil is debug", nil, "debug"},
		{"context canceled is info", context.Canceled, "info"},
		{"wrapped canceled is info", fmt.Errorf("stream canceled before start event: %w", context.Canceled), "info"},
		{"deadline is warn", context.DeadlineExceeded, "warn"},
		{"wrapped deadline is warn", fmt.Errorf("x: %w", context.DeadlineExceeded), "warn"},
		{"real failure is error", errors.New("boom"), "error"},
		{"connect internal is error", connect.NewError(connect.CodeInternal, errors.New("boom")), "error"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, levelOf(t, tc.err))
		})
	}
}

func TestCodeOf(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want connect.Code
	}{
		{"canceled -> Canceled (not Unknown)", context.Canceled, connect.CodeCanceled},
		{"wrapped canceled -> Canceled", fmt.Errorf("stream canceled: %w", context.Canceled), connect.CodeCanceled},
		{"deadline -> DeadlineExceeded", context.DeadlineExceeded, connect.CodeDeadlineExceeded},
		{"plain error -> Unknown", errors.New("boom"), connect.CodeUnknown},
		{"connect code preserved", connect.NewError(connect.CodeInvalidArgument, errors.New("x")), connect.CodeInvalidArgument},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, codeOf(tc.err))
		})
	}
}
