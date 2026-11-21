package process

import (
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
	spec "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process/processconnect"
	"github.com/e2b-dev/infra/packages/envd/internal/services/streaming"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
)

// TestHandleDisablesProxyBufferingForConnect checks the interceptor is wired
// into the service Handle mounts: Connect responses must tell reverse proxies
// not to buffer, while unary responses keep the default.
func TestHandleDisablesProxyBufferingForConnect(t *testing.T) {
	t.Parallel()

	cwd := t.TempDir()
	logger := zerolog.Nop()
	mux := chi.NewRouter()
	Handle(mux, &logger, &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		Workdir: &cwd,
	}, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := spec.NewProcessClient(srv.Client(), srv.URL)

	// No such process, so the stream ends with NotFound; the header is set
	// before the handler runs and must be present regardless.
	stream, err := client.Connect(t.Context(), connect.NewRequest(&rpc.ConnectRequest{
		Process: &rpc.ProcessSelector{Selector: &rpc.ProcessSelector_Pid{Pid: 1 << 30}},
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.Close() })

	assert.False(t, stream.Receive())
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(stream.Err()))
	assert.Equal(t, []string{"no"}, stream.ResponseHeader().Values(streaming.AccelBufferingHeader))

	resp, err := client.List(t.Context(), connect.NewRequest(&rpc.ListRequest{}))
	require.NoError(t, err)
	assert.Empty(t, resp.Header().Values(streaming.AccelBufferingHeader))
}
