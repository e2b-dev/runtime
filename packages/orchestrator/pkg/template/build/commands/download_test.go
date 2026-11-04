//go:build linux

package commands

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func blobWriting(t *testing.T, write func(ctx context.Context, dst io.Writer) (int64, error)) *storage.MockBlob {
	t.Helper()

	blob := storage.NewMockBlob(t)
	blob.EXPECT().WriteTo(mock.Anything, mock.Anything).RunAndReturn(write)

	return blob
}

// The blob reaches the file through a plain writer, and only a bare io.EOF counts as a complete stream.
func TestDownloadBlob(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		err     error
		wantErr bool
	}{
		{name: "complete"},
		{name: "a bare io.EOF ends the stream", err: io.EOF},
		{name: "a wrapped io.EOF is a failure", err: fmt.Errorf("read: %w", io.EOF), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f, err := os.CreateTemp(t.TempDir(), "layer-file-*.tar")
			require.NoError(t, err)
			t.Cleanup(func() { _ = f.Close() })

			err = downloadBlob(t.Context(), blobWriting(t, func(_ context.Context, dst io.Writer) (int64, error) {
				_, readerFrom := dst.(io.ReaderFrom)
				assert.False(t, readerFrom, "io.Copy into the writer keeps the backend's buffer")
				n, err := dst.Write([]byte("tar"))
				if err != nil {
					return int64(n), err
				}

				return int64(n), tc.err
			}), f)
			if tc.wantErr {
				require.ErrorIs(t, err, io.EOF)
			} else {
				require.NoError(t, err)
			}
			got, err := os.ReadFile(f.Name())
			require.NoError(t, err)
			assert.Equal(t, "tar", string(got))
		})
	}
}

// failingOnce fails its first write, then accepts writes.
type failingOnce struct {
	bytes.Buffer

	failed bool
}

var errDiskFull = errors.New("no space left on device")

func (f *failingOnce) Write(p []byte) (int, error) {
	if !f.failed {
		f.failed = true

		return 0, errDiskFull
	}

	return f.Buffer.Write(p)
}

// After a failed write the download is cancelled, and a backend that retries
// into the writer anyway, as the NFS cache falls back to its backend, appends nothing.
func TestDownloadBlobStopsAtTheFirstWriteError(t *testing.T) {
	t.Parallel()

	dst := &failingOnce{}
	err := downloadBlob(t.Context(), blobWriting(t, func(ctx context.Context, w io.Writer) (int64, error) {
		if _, err := w.Write([]byte("ta")); err == nil {
			return 0, errors.New("the first write should have failed")
		}
		require.ErrorIs(t, ctx.Err(), context.Canceled, "the backend is told to stop")
		n, _ := w.Write([]byte("tar"))

		return int64(n), nil
	}), dst)
	require.ErrorIs(t, err, errDiskFull, "the local write error, not the backend's result")
	assert.Empty(t, dst.String(), "nothing after the failure reaches the file")
}
