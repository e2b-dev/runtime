package filesystem

import (
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/filesystem"
	spec "github.com/e2b-dev/infra/packages/envd/internal/services/spec/filesystem/filesystemconnect"
	"github.com/e2b-dev/infra/packages/envd/internal/services/streaming"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
)

// TestHandleDisablesProxyBufferingForWatchDir checks the interceptor is wired
// into the service Handle mounts: WatchDir responses must tell reverse proxies
// not to buffer, while unary responses keep the default.
func TestHandleDisablesProxyBufferingForWatchDir(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	mux := chi.NewRouter()
	Handle(mux, &logger, &execcontext.Defaults{EnvVars: utils.NewEnvVars()})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := spec.NewFilesystemClient(srv.Client(), srv.URL)

	// No user is configured, so the stream ends with an error; the header is
	// set before the handler runs and must be present regardless.
	stream, err := client.WatchDir(t.Context(), connect.NewRequest(&rpc.WatchDirRequest{Path: t.TempDir()}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.Close() })

	assert.False(t, stream.Receive())
	assert.Equal(t, []string{"no"}, stream.ResponseHeader().Values(streaming.AccelBufferingHeader))

	_, err = client.ListDir(t.Context(), connect.NewRequest(&rpc.ListDirRequest{Path: t.TempDir()}))
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	assert.Empty(t, connectErr.Meta().Values(streaming.AccelBufferingHeader))
}
