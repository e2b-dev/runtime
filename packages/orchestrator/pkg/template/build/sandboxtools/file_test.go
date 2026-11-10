//go:build linux

package sandboxtools

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
)

// The upload response is a status line from envd; a body past the cap is
// refused, not read into memory or the event.
func TestUploadFileRefusesAnOversizedResponseBody(t *testing.T) {
	t.Parallel()

	src := filepath.Join(t.TempDir(), "src")
	require.NoError(t, os.WriteFile(src, []byte("payload"), 0o600))

	for name, tc := range map[string]struct {
		status, size int
		tooLarge     bool
	}{
		"ok":              {status: http.StatusOK, size: 2},
		"ok at the cap":   {status: http.StatusOK, size: 64 << 10},
		"ok over the cap": {status: http.StatusOK, size: 64<<10 + 1, tooLarge: true},
		"ok far over":     {status: http.StatusOK, size: 32 << 20, tooLarge: true},
		"error":           {status: http.StatusInternalServerError, size: 16},
		"error far over":  {status: http.StatusInternalServerError, size: 32 << 20, tooLarge: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.WriteHeader(tc.status)
				_, _ = w.Write(bytes.Repeat([]byte("x"), tc.size))
			}))
			t.Cleanup(srv.Close)

			err := uploadFile(t.Context(), srv.URL, "sbx", "root", src, "/tmp/dst")

			switch {
			case tc.tooLarge:
				require.ErrorIs(t, err, sandbox.ErrEnvdBodyTooLarge)
				assert.Less(t, len(err.Error()), 1<<10)
			case tc.status == http.StatusOK:
				require.NoError(t, err)
			default:
				require.Error(t, err)
				assert.NotErrorIs(t, err, sandbox.ErrEnvdBodyTooLarge)
			}
		})
	}
}

// A gzip response is capped on its wire bytes, not its expanded ones.
func TestUploadFileCapsAGzipResponseOnTheWire(t *testing.T) {
	t.Parallel()

	src := filepath.Join(t.TempDir(), "src")
	require.NoError(t, os.WriteFile(src, []byte("payload"), 0o600))

	var member bytes.Buffer
	require.NoError(t, gzip.NewWriter(&member).Close())
	body := bytes.Repeat(member.Bytes(), (1<<20)/member.Len())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	err := uploadFile(t.Context(), srv.URL, "sbx", "root", src, "/tmp/dst")
	require.ErrorIs(t, err, sandbox.ErrEnvdBodyTooLarge)
}

// A return before the request is sent still ends the multipart writer;
// synctest.Test fails with a deadlock if the writer is left blocked.
func TestUploadFileEndsItsWriterOnAnEarlyReturn(t *testing.T) {
	t.Parallel()

	src := filepath.Join(t.TempDir(), "src")
	require.NoError(t, os.WriteFile(src, []byte("payload"), 0o600))

	synctest.Test(t, func(t *testing.T) {
		require.Error(t, uploadFile(t.Context(), "http://bad host", "sbx", "root", src, "/tmp/dst"))
	})
}
