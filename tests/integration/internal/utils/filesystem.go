package utils

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/filesystem"
	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/envd"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
)

func UploadFile(tb testing.TB, ctx context.Context, sbx *api.Sandbox, envdClient *setup.EnvdClient, path string, content string) {
	tb.Helper()

	UploadFileAs(tb, ctx, sbx, envdClient, path, content, "user")
}

// UploadFileAs uploads content to path as username, for the paths the default
// user cannot write.
func UploadFileAs(tb testing.TB, ctx context.Context, sbx *api.Sandbox, envdClient *setup.EnvdClient, path, content, username string) {
	tb.Helper()

	buffer, contentType := CreateTextFile(tb, path, content)

	reqEditors := []envd.RequestEditorFn{setup.WithSandbox(tb, sbx.SandboxID)}
	if sbx.EnvdAccessToken != nil {
		reqEditors = append(reqEditors, setup.WithEnvdAccessToken(tb, *sbx.EnvdAccessToken))
	}

	writeRes, err := envdClient.HTTPClient.PostFilesWithBodyWithResponse(
		ctx,
		&envd.PostFilesParams{Path: &path, Username: &username},
		contentType,
		buffer,
		reqEditors...,
	)
	require.NoError(tb, err)
	require.Equal(tb, http.StatusOK, writeRes.StatusCode(), string(writeRes.Body))
}

func CreateTextFile(tb testing.TB, path string, content string) (*bytes.Buffer, string) {
	tb.Helper()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", path)
	if err != nil {
		tb.Fatal(err)
	}
	_, err = part.Write([]byte(content))
	if err != nil {
		tb.Fatal(err)
	}
	err = writer.Close()
	if err != nil {
		tb.Fatal(err)
	}

	return body, writer.FormDataContentType()
}

func CreateDir(tb testing.TB, sbx *api.Sandbox, path string) {
	tb.Helper()

	ctx, cancel := context.WithCancel(tb.Context())
	defer cancel()

	client := setup.GetEnvdClient(tb, ctx)
	req := connect.NewRequest(&filesystem.MakeDirRequest{
		Path: path,
	})
	setup.SetSandboxHeader(tb, req.Header(), sbx.SandboxID)
	setup.SetUserHeader(tb, req.Header(), "user")

	_, err := client.FilesystemClient.MakeDir(ctx, req)
	require.NoError(tb, err)
}
