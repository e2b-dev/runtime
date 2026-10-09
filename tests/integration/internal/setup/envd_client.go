package setup

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/filesystem/filesystemconnect"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process/processconnect"
	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/envd"
)

type EnvdClient struct {
	HTTPClient       *envd.ClientWithResponses
	FilesystemClient filesystemconnect.FilesystemClient
	ProcessClient    processconnect.ProcessClient
}

func GetEnvdClient(tb testing.TB, _ context.Context) *EnvdClient {
	tb.Helper()

	hc := http.Client{
		Timeout: envdTimeout,
	}

	httpC, err := envd.NewClientWithResponses(EnvdProxy, envd.WithHTTPClient(&hc))
	require.NoError(tb, err)

	fileC := filesystemconnect.NewFilesystemClient(&hc, EnvdProxy)
	processC := processconnect.NewProcessClient(&hc, EnvdProxy)

	return &EnvdClient{
		HTTPClient:       httpC,
		FilesystemClient: fileC,
		ProcessClient:    processC,
	}
}

func WithSandbox(tb testing.TB, sbx *api.Sandbox) func(context.Context, *http.Request) error {
	tb.Helper()

	return func(_ context.Context, req *http.Request) error {
		SetSandboxHeader(tb, req.Header, sbx)
		req.Host = req.Header.Get("Host")

		return nil
	}
}

func WithInsecureSandbox(tb testing.TB, sandboxID string) func(context.Context, *http.Request) error {
	tb.Helper()

	return func(_ context.Context, req *http.Request) error {
		SetInsecureSandboxHeader(tb, req.Header, sandboxID)
		req.Host = req.Header.Get("Host")

		return nil
	}
}

func WithEnvdAccessToken(tb testing.TB, accessToken string) func(ctx context.Context, req *http.Request) error {
	tb.Helper()

	return func(_ context.Context, req *http.Request) error {
		SetAccessTokenHeader(tb, req.Header, accessToken)

		return nil
	}
}

func SetSandboxHeader(tb testing.TB, header http.Header, sbx *api.Sandbox) {
	tb.Helper()
	SetInsecureSandboxHeader(tb, header, sbx.SandboxID)
	if sbx.EnvdAccessToken != nil {
		SetAccessTokenHeader(tb, header, *sbx.EnvdAccessToken)
	}
}

func SetInsecureSandboxHeader(tb testing.TB, header http.Header, sandboxID string) {
	tb.Helper()
	err := grpc.SetSandboxHeader(header, EnvdProxy, sandboxID)
	require.NoError(tb, err)
}

func SetAccessTokenHeader(tb testing.TB, header http.Header, accessToken string) {
	tb.Helper()
	header.Set("X-Access-Token", accessToken)
}

func SetUserHeader(tb testing.TB, header http.Header, user string) {
	tb.Helper()
	grpc.SetUserHeader(header, user)
}
