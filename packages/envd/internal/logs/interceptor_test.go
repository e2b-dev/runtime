package logs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/logs"
	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/filesystem"
	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/filesystem/filesystemconnect"
	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/process/processconnect"
)

type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

// line returns the one log line whose message ends with suffix.
func (b *logBuffer) line(t *testing.T, suffix string) map[string]any {
	t.Helper()

	b.mu.Lock()
	defer b.mu.Unlock()

	var found []map[string]any
	for raw := range strings.SplitSeq(strings.TrimSpace(b.buf.String()), "\n") {
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw), &entry))

		if msg, _ := entry["message"].(string); strings.HasSuffix(msg, suffix) {
			found = append(found, entry)
		}
	}
	require.Len(t, found, 1, "log lines ending with %q", suffix)

	return found[0]
}

// testServer serves the handlers below over HTTP/1.1, as envd does, and signals
// done after a logging helper has written a call's final line.
type testServer struct {
	filesystemconnect.UnimplementedFilesystemHandler
	processconnect.UnimplementedProcessHandler

	logger *zerolog.Logger
	done   chan struct{}

	watch func(context.Context, *connect.ServerStream[filesystem.WatchDirResponse]) error
	input func(context.Context, *connect.ClientStream[process.StreamInputRequest]) error
	stat  func(context.Context) error

	opts []logs.Option
}

func (s *testServer) WatchDir(ctx context.Context, req *connect.Request[filesystem.WatchDirRequest], stream *connect.ServerStream[filesystem.WatchDirResponse]) error {
	defer func() { s.done <- struct{}{} }()

	return logs.LogServerStreamWithoutEvents(ctx, s.logger, req, stream,
		func(ctx context.Context, _ *connect.Request[filesystem.WatchDirRequest], stream *connect.ServerStream[filesystem.WatchDirResponse]) error {
			return s.watch(ctx, stream)
		}, s.opts...)
}

func (s *testServer) StreamInput(ctx context.Context, stream *connect.ClientStream[process.StreamInputRequest]) (*connect.Response[process.StreamInputResponse], error) {
	defer func() { s.done <- struct{}{} }()

	return logs.LogClientStreamWithoutEvents(ctx, s.logger, stream,
		func(ctx context.Context, stream *connect.ClientStream[process.StreamInputRequest]) (*connect.Response[process.StreamInputResponse], error) {
			if err := s.input(ctx, stream); err != nil {
				return nil, err
			}

			return connect.NewResponse(&process.StreamInputResponse{}), nil
		}, s.opts...)
}

func (s *testServer) Stat(ctx context.Context, _ *connect.Request[filesystem.StatRequest]) (*connect.Response[filesystem.StatResponse], error) {
	if err := s.stat(ctx); err != nil {
		return nil, err
	}

	return connect.NewResponse(&filesystem.StatResponse{}), nil
}

func newTestServer(t *testing.T, s *testServer) (*logBuffer, filesystemconnect.FilesystemClient, processconnect.ProcessClient) {
	t.Helper()

	buf := &logBuffer{}
	logger := zerolog.New(buf).Level(zerolog.DebugLevel)
	s.logger = &logger
	s.done = make(chan struct{}, 1)

	unaryDone := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			defer func() { s.done <- struct{}{} }()

			return next(ctx, req)
		}
	})
	interceptors := connect.WithInterceptors(unaryDone, logs.NewUnaryLogInterceptor(&logger, s.opts...))

	mux := http.NewServeMux()
	mux.Handle(filesystemconnect.NewFilesystemHandler(s, interceptors))
	mux.Handle(processconnect.NewProcessHandler(s, interceptors))

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	return buf,
		filesystemconnect.NewFilesystemClient(server.Client(), server.URL),
		processconnect.NewProcessClient(server.Client(), server.URL)
}

func (s *testServer) wait(t *testing.T) {
	t.Helper()

	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
		require.Fail(t, "handler did not return")
	}
}

// sendUntilError sends keepalives until the stream breaks, the way a handler
// fails when its client leaves between two sends.
func sendUntilError(stream *connect.ServerStream[filesystem.WatchDirResponse]) error {
	for {
		err := stream.Send(&filesystem.WatchDirResponse{
			Event: &filesystem.WatchDirResponse_Keepalive{Keepalive: &filesystem.WatchDirResponse_KeepAlive{}},
		})
		if err != nil {
			return connect.NewError(connect.CodeUnknown, fmt.Errorf("error sending keepalive: %w", err))
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func TestServerStreamEndLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		handler   func(context.Context, *connect.ServerStream[filesystem.WatchDirResponse]) error
		cancel    bool
		wantLevel string
	}{
		{
			name: "client cancels while the handler waits",
			handler: func(ctx context.Context, stream *connect.ServerStream[filesystem.WatchDirResponse]) error {
				if err := stream.Send(&filesystem.WatchDirResponse{}); err != nil {
					return err
				}
				<-ctx.Done()

				return ctx.Err()
			},
			cancel:    true,
			wantLevel: "debug",
		},
		{
			name: "client cancels while the handler sends",
			handler: func(_ context.Context, stream *connect.ServerStream[filesystem.WatchDirResponse]) error {
				return sendUntilError(stream)
			},
			cancel:    true,
			wantLevel: "debug",
		},
		{
			name: "handler fails",
			handler: func(_ context.Context, stream *connect.ServerStream[filesystem.WatchDirResponse]) error {
				if err := stream.Send(&filesystem.WatchDirResponse{}); err != nil {
					return err
				}

				return connect.NewError(connect.CodeInternal, errors.New("watcher error"))
			},
			wantLevel: "error",
		},
		{
			name: "handler ends",
			handler: func(_ context.Context, stream *connect.ServerStream[filesystem.WatchDirResponse]) error {
				return stream.Send(&filesystem.WatchDirResponse{})
			},
			wantLevel: "debug",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := &testServer{watch: tt.handler}
			buf, client, _ := newTestServer(t, s)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			stream, err := client.WatchDir(ctx, connect.NewRequest(&filesystem.WatchDirRequest{}))
			require.NoError(t, err)
			require.True(t, stream.Receive(), "first message: %v", stream.Err())

			if tt.cancel {
				cancel()
			}

			s.wait(t)

			end := buf.line(t, "(server stream end)")
			assert.Equal(t, tt.wantLevel, end["level"])
			if tt.wantLevel != "debug" || tt.cancel {
				assert.NotEmpty(t, end["error"], "the error is kept on the line")
			}
		})
	}
}

func TestClientStreamEndLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cancel    bool
		wantLevel string
	}{
		{name: "client cancels mid-stream", cancel: true, wantLevel: "debug"},
		{name: "client closes the stream", wantLevel: "debug"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			received := make(chan struct{}, 1)
			s := &testServer{
				input: func(_ context.Context, stream *connect.ClientStream[process.StreamInputRequest]) error {
					for stream.Receive() {
						select {
						case received <- struct{}{}:
						default:
						}
					}
					if err := stream.Err(); err != nil {
						return connect.NewError(connect.CodeUnknown, fmt.Errorf("error streaming input: %w", err))
					}

					return nil
				},
			}
			buf, _, client := newTestServer(t, s)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			stream := client.StreamInput(ctx)
			require.NoError(t, stream.Send(&process.StreamInputRequest{
				Event: &process.StreamInputRequest_Keepalive{Keepalive: &process.StreamInputRequest_KeepAlive{}},
			}))
			<-received

			if tt.cancel {
				cancel()
			} else {
				_, err := stream.CloseAndReceive()
				require.NoError(t, err)
			}

			s.wait(t)

			end := buf.line(t, "(client stream end)")
			assert.Equal(t, tt.wantLevel, end["level"])
			if tt.cancel {
				assert.NotEmpty(t, end["error"], "the error is kept on the line")
			}
		})
	}
}

func TestUnaryEndLevel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		handler   func(context.Context) error
		cancel    bool
		wantLevel string
	}{
		{
			name: "client cancels",
			handler: func(ctx context.Context) error {
				<-ctx.Done()

				return ctx.Err()
			},
			cancel:    true,
			wantLevel: "debug",
		},
		{
			name: "handler fails",
			handler: func(context.Context) error {
				return connect.NewError(connect.CodeNotFound, errors.New("process not found"))
			},
			wantLevel: "error",
		},
		{
			name:      "handler succeeds",
			handler:   func(context.Context) error { return nil },
			wantLevel: "info",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			entered := make(chan struct{})
			s := &testServer{stat: func(ctx context.Context) error {
				close(entered)

				return tt.handler(ctx)
			}}
			buf, client, _ := newTestServer(t, s)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			if tt.cancel {
				go func() {
					select {
					case <-entered:
						cancel()
					case <-ctx.Done():
					}
				}()
			}

			_, _ = client.Stat(ctx, connect.NewRequest(&filesystem.StatRequest{}))

			s.wait(t)

			assert.Equal(t, tt.wantLevel, buf.line(t, "Filesystem stat")["level"])
		})
	}
}

func TestWithRedactSetsLoggedMessages(t *testing.T) {
	t.Parallel()

	const redacted = "redacted"
	opts := []logs.Option{logs.WithRedact(func(any) any { return redacted })}

	tests := []struct {
		name   string
		server *testServer
		call   func(*testing.T, filesystemconnect.FilesystemClient, processconnect.ProcessClient)
		line   string
		fields []string
	}{
		{
			name:   "unary",
			server: &testServer{stat: func(context.Context) error { return nil }},
			call: func(t *testing.T, fs filesystemconnect.FilesystemClient, _ processconnect.ProcessClient) {
				t.Helper()

				_, err := fs.Stat(t.Context(), connect.NewRequest(&filesystem.StatRequest{}))
				require.NoError(t, err)
			},
			line:   "Filesystem stat",
			fields: []string{"request", "response"},
		},
		{
			name: "server stream",
			server: &testServer{watch: func(_ context.Context, stream *connect.ServerStream[filesystem.WatchDirResponse]) error {
				return stream.Send(&filesystem.WatchDirResponse{})
			}},
			call: func(t *testing.T, fs filesystemconnect.FilesystemClient, _ processconnect.ProcessClient) {
				t.Helper()

				stream, err := fs.WatchDir(t.Context(), connect.NewRequest(&filesystem.WatchDirRequest{}))
				require.NoError(t, err)
				require.True(t, stream.Receive(), "first message: %v", stream.Err())
			},
			line:   "(server stream start)",
			fields: []string{"request"},
		},
		{
			name: "client stream",
			server: &testServer{input: func(_ context.Context, stream *connect.ClientStream[process.StreamInputRequest]) error {
				for stream.Receive() {
				}

				return stream.Err()
			}},
			call: func(t *testing.T, _ filesystemconnect.FilesystemClient, proc processconnect.ProcessClient) {
				t.Helper()

				_, err := proc.StreamInput(t.Context()).CloseAndReceive()
				require.NoError(t, err)
			},
			line:   "(client stream end)",
			fields: []string{"response"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.server.opts = opts
			buf, fs, proc := newTestServer(t, tt.server)

			tt.call(t, fs, proc)
			tt.server.wait(t)

			logged := buf.line(t, tt.line)
			for _, field := range tt.fields {
				assert.Equal(t, redacted, logged[field], field)
			}
		})
	}
}
