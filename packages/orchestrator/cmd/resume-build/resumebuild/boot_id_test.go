//go:build linux

package resumebuild

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process"
)

// fakeStream replays envd process events; it counts the events read.
type fakeStream struct {
	events []*process.StartResponse
	read   int
}

func (f *fakeStream) Receive() bool {
	if f.read == len(f.events) {
		return false
	}
	f.read++

	return true
}

func (f *fakeStream) Msg() *process.StartResponse { return f.events[f.read-1] }
func (f *fakeStream) Err() error                  { return nil }

func stdoutEvent(s string) *process.StartResponse {
	return &process.StartResponse{Event: &process.ProcessEvent{Event: &process.ProcessEvent_Data{Data: &process.ProcessEvent_DataEvent{Output: &process.ProcessEvent_DataEvent_Stdout{Stdout: []byte(s)}}}}}
}

func endEvent() *process.StartResponse {
	return &process.StartResponse{Event: &process.ProcessEvent{Event: &process.ProcessEvent_End{End: &process.ProcessEvent_EndEvent{Exited: true}}}}
}

// Many individually small frames cannot push more than an identifier into the host.
func TestBootIDOutputIsCapped(t *testing.T) {
	t.Parallel()

	frames := make([]*process.StartResponse, 0, 10001)
	for range 10000 {
		frames = append(frames, stdoutEvent(strings.Repeat("x", 1024)))
	}
	frames = append(frames, endEvent())
	stream := &fakeStream{events: frames}

	out := &cappedBuffer{limit: maxBootIDOutput}
	err := copyCommandOutput(stream, out)
	require.ErrorIs(t, err, errBootIDTooLong)
	assert.LessOrEqual(t, len(out.String()), maxBootIDOutput, "nothing past the limit is kept")
	assert.Less(t, stream.read, 10, "the copy stops at the first frame past the limit")

	out = &cappedBuffer{limit: maxBootIDOutput}
	require.NoError(t, copyCommandOutput(&fakeStream{events: []*process.StartResponse{stdoutEvent("6f1c-boot\n"), endEvent()}}, out))
	id, err := parseBootID(out.String())
	require.NoError(t, err)
	assert.Equal(t, "6f1c-boot", id)

	_, err = parseBootID(" \n")
	require.Error(t, err, "an empty identity is refused: two empty readings would compare equal")
}
