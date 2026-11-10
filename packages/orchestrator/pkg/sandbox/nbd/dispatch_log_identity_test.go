//go:build linux

package nbd

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// errProv fails every backend call, so one request is enough to reach the
// dispatcher's error log.
type errProv struct{}

var errBackend = errors.New("backend unavailable")

func (errProv) ReadAt(context.Context, []byte, int64) (int, error) { return 0, errBackend }

func (errProv) Size(context.Context) (int64, error) { return 0, nil }

func (errProv) WriteAt([]byte, int64) (int, error) { return 0, errBackend }

func (errProv) WriteZeroesAt(int64, int64) (int, error) { return 0, errBackend }

// A backend failure is the line an operator chases, so it has to name the
// sandbox it belongs to rather than an offset and a device index they then
// have to join back to one.
func TestDispatch_BackendFailureCarriesSandboxIdentity(t *testing.T) {
	t.Parallel()

	runtime := sandboxtypes.RuntimeMetadata{
		SandboxID:   "sbx-log-identity",
		TemplateID:  "tmpl-log-identity",
		TeamID:      "team-log-identity",
		BuildID:     "build-log-identity",
		ExecutionID: "exec-log-identity",
	}

	conn := &replyConn{reqCh: make(chan []byte, 1), replies: make(chan Response, 1)}
	d := NewDispatch(conn, errProv{}, true, runtime.Logger())

	done := make(chan error, 1)
	go func() { done <- d.Handle(t.Context()) }()

	conn.reqCh <- nbdRequest(NBDCmdRead, 1, 0, 4096)
	select {
	case resp := <-conn.replies:
		require.NotZero(t, resp.Error, "a failing backend read must reply with an NBD error")
	case <-time.After(5 * time.Second):
		t.Fatal("no reply for the failing read")
	}

	close(conn.reqCh) // Handle sees io.EOF and exits
	select {
	case err := <-done:
		require.ErrorIs(t, err, io.EOF)
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch loop did not exit")
	}

	var fields map[string]any
	for _, entry := range testLogObserver.FilterMessage("nbd backend read failed").All() {
		if entry.ContextMap()["sandbox.id"] == runtime.SandboxID {
			fields = entry.ContextMap()

			break
		}
	}

	require.NotNil(t, fields, "the backend read failure must be logged for this sandbox")
	require.Equal(t, runtime.TemplateID, fields["template.id"])
	require.Equal(t, runtime.TeamID, fields["team.id"])
	require.Equal(t, runtime.BuildID, fields["build.id"])
	require.Equal(t, runtime.ExecutionID, fields["execution.id"])
	require.Equal(t, "nbd.errProv", fields["nbd_provider"])
}
