//go:build linux

package sandboxtools

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const fileCopyTimeout = 10 * time.Minute

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/sandboxtools")

func CopyFile(
	ctx context.Context,
	proxy *proxy.SandboxProxy,
	sandboxID string,
	user string,
	sourcePath string,
	targetPath string,
) error {
	ctx, span := tracer.Start(ctx, "copy-file")
	defer span.End()

	return uploadFile(ctx, fmt.Sprintf("http://localhost%s", proxy.GetAddr()), sandboxID, user, sourcePath, targetPath)
}

// uploadFile posts sourcePath to the envd behind proxyHost.
func uploadFile(ctx context.Context, proxyHost, sandboxID, user, sourcePath, targetPath string) error {
	file, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer file.Close()

	// Pipe to stream data
	pr, pw := io.Pipe()
	// Unblocks the writer goroutine when a return comes before Do takes pr.
	defer pr.Close()
	writer := multipart.NewWriter(pw)

	errChan := make(chan error, 1)
	go func() {
		var err error
		defer func() {
			writer.Close()
			if err != nil {
				pw.CloseWithError(err)
			} else {
				pw.Close()
			}
			errChan <- err
		}()

		part, err := writer.CreateFormFile("file", filepath.Base(sourcePath))
		if err != nil {
			err = fmt.Errorf("failed to create form file: %w", err)

			return
		}

		if _, errCopy := io.Copy(part, file); errCopy != nil {
			err = fmt.Errorf("failed to copy file: %w", errCopy)

			return
		}
	}()

	// Prepare query parameters
	params := url.Values{}
	params.Add("path", targetPath)
	params.Add("username", user)

	telemetry.ReportEvent(ctx, "copy_file",
		attribute.String("source.path", sourcePath),
		attribute.String("target.path", targetPath),
		attribute.String("proxy.host", proxyHost),
		attribute.String("sandbox.id", sandboxID),
	)
	uploadURL := fmt.Sprintf("%s/files?%s", proxyHost, params.Encode())

	// Create HTTP request with streaming body
	req, err := http.NewRequestWithContext(sandbox.WithEnvdSandbox(ctx, sbxlogger.SandboxMetadata{SandboxID: sandboxID}), http.MethodPost, uploadURL, pr)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	err = grpc.SetSandboxHeader(req.Header, proxyHost, sandboxID)
	if err != nil {
		return fmt.Errorf("failed to set request header: %w", err)
	}
	req.Host = req.Header.Get("Host")

	// Send request
	resp, err := sandbox.NewEnvdHTTPClient(fileCopyTimeout).Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if uploadErr := <-errChan; uploadErr != nil {
		return fmt.Errorf("file upload failed: %w", uploadErr)
	}

	body, err := sandbox.ReadEnvdBody(resp.Body)
	telemetry.ReportEvent(ctx, "file_upload",
		attribute.Int("response.code", resp.StatusCode),
		attribute.String("response.body", string(body)),
	)
	if errors.Is(err, sandbox.ErrEnvdBodyTooLarge) {
		return fmt.Errorf("read upload response (%d): %w", resp.StatusCode, err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to upload file (%d): %s", resp.StatusCode, string(body))
	}

	return nil
}
