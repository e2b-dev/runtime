package process

import (
	"bytes"
	"context"
	"net/http/httptest"
	"os/user"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	"github.com/e2b-dev/infra/packages/envd/internal/services/process/handler"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
	spec "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process/processconnect"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
)

const (
	testEnvName  = "DATABASE_URL"
	testEnvValue = "postgres://app:hunter2@db.internal/prod"
)

func withTestEnv(config *rpc.ProcessConfig) *rpc.ProcessConfig {
	config.Envs = map[string]string{testEnvName: testEnvValue}

	return config
}

func TestRedactKeepsEnvVarNamesOnly(t *testing.T) {
	t.Parallel()

	start := &rpc.StartRequest{Process: withTestEnv(echoCommand())}
	list := &rpc.ListResponse{Processes: []*rpc.ProcessInfo{{Config: withTestEnv(echoCommand()), Pid: 42}}}

	loggedStart, ok := redact(start).(*rpc.StartRequest)
	require.True(t, ok)
	loggedList, ok := redact(list).(*rpc.ListResponse)
	require.True(t, ok)
	require.Len(t, loggedList.GetProcesses(), 1)

	for _, logged := range []*rpc.ProcessConfig{loggedStart.GetProcess(), loggedList.GetProcesses()[0].GetConfig()} {
		assert.Equal(t, echoCommand().GetCmd(), logged.GetCmd())
		assert.Equal(t, echoCommand().GetArgs(), logged.GetArgs())
		assert.Contains(t, logged.GetEnvs(), testEnvName)
		assert.NotEqual(t, testEnvValue, logged.GetEnvs()[testEnvName])
	}

	// The messages themselves keep the values: the process is started from them.
	assert.Equal(t, testEnvValue, start.GetProcess().GetEnvs()[testEnvName])
	assert.Equal(t, testEnvValue, list.GetProcesses()[0].GetConfig().GetEnvs()[testEnvName])

	other := &rpc.SendSignalRequest{}
	assert.Same(t, other, redact(other))
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// Runs the service as Handle mounts it: fails if Start or List stops passing redact.
func TestServiceLogsOmitEnvVarValues(t *testing.T) {
	t.Parallel()

	u, err := user.Current()
	require.NoError(t, err)

	cwd := t.TempDir()
	out := &syncBuffer{}
	logger := zerolog.New(out).Level(zerolog.DebugLevel)

	mux := chi.NewRouter()
	svc := Handle(mux, &logger, &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    u.Username,
		Workdir: &cwd,
	}, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := spec.NewProcessClient(srv.Client(), srv.URL)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// A registered process gives List a config to return without spawning one.
	svc.processes.Store(42, &handler.Handler{Config: withTestEnv(echoCommand())})

	res, err := client.List(ctx, connect.NewRequest(&rpc.ListRequest{}))
	require.NoError(t, err)
	require.Len(t, res.Msg.GetProcesses(), 1)
	assert.Equal(t, testEnvValue, res.Msg.GetProcesses()[0].GetConfig().GetEnvs()[testEnvName])

	// Start may fail here without root; its request line is logged before that.
	stream, err := client.Start(ctx, connect.NewRequest(&rpc.StartRequest{Process: withTestEnv(echoCommand())}))
	require.NoError(t, err)

	for stream.Receive() {
	}

	_ = stream.Close()

	logged := out.String()
	assert.Contains(t, logged, "Process list")
	assert.Contains(t, logged, "Process start (server stream start)")
	assert.Contains(t, logged, testEnvName)
	assert.NotContains(t, logged, testEnvValue)
}
