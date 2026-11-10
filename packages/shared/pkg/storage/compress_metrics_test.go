package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// heldMetricsReader installs a global meter provider once, so the storage
// package's instruments report to a reader the tests can collect.
var heldMetricsReader = sync.OnceValue(func() *sdkmetric.ManualReader {
	r := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(r)))

	return r
})

// collectUploadFramesHeld reads the counter for zstd, the codec these tests use.
func collectUploadFramesHeld(ctx context.Context) (int64, error) {
	var rm metricdata.ResourceMetrics
	if err := heldMetricsReader().Collect(ctx, &rm); err != nil {
		return 0, err
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "orchestrator.storage.upload.frames.held" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				return 0, fmt.Errorf("unexpected data type %T", m.Data)
			}
			for _, dp := range sum.DataPoints {
				if v, _ := dp.Attributes.Value(AttrCodec); v.AsString() == CompressionZstd.String() {
					return dp.Value, nil
				}
			}
		}
	}

	return 0, nil
}

func uploadFramesHeldValue(t *testing.T) int64 {
	t.Helper()

	v, err := collectUploadFramesHeld(t.Context())
	require.NoError(t, err)

	return v
}

// heldPartUploader holds its first UploadPart until release is closed or ctx
// is done, after reporting the capacity of the frames it was given on
// started. Every UploadPart returns ctx's error, else err. Complete records
// the counter, after every part is uploaded and before the stream returns.
type heldPartUploader struct {
	memPartUploader

	err     error
	once    sync.Once
	started chan int64
	release chan struct{}

	atComplete    int64
	atCompleteErr error
}

func (u *heldPartUploader) Complete(ctx context.Context) error {
	u.atComplete, u.atCompleteErr = collectUploadFramesHeld(ctx)

	return u.memPartUploader.Complete(ctx)
}

func (u *heldPartUploader) UploadPart(ctx context.Context, partIndex int, data ...[]byte) error {
	u.once.Do(func() {
		var capacity int64
		for _, d := range data {
			capacity += int64(cap(d))
		}
		u.started <- capacity
		select {
		case <-u.release:
		case <-ctx.Done():
		}
	})

	if err := ctx.Err(); err != nil {
		return err
	}
	if u.err != nil {
		return u.err
	}

	return u.memPartUploader.UploadPart(ctx, partIndex, data...)
}

// TestCompressStreamReportsHeldFrames checks that the frames of a part waiting
// on its upload are counted by capacity, and that the stream leaves nothing
// counted once it returns, whether the upload succeeds, fails or is cancelled.
//
//nolint:paralleltest // reads a process-wide counter that other streams would move
func TestCompressStreamReportsHeldFrames(t *testing.T) {
	errUpload := errors.New("upload failed")

	tests := []struct {
		name    string
		err     error
		cancel  bool
		wantErr error
	}{
		{name: "upload succeeds"},
		{name: "upload fails", err: errUpload, wantErr: errUpload},
		{name: "stream cancelled", cancel: true, wantErr: context.Canceled},
	}

	data := generateSemiRandomData(4 * megabyte)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { //nolint:paralleltest // see parent
			cfg := defaultCfg(CompressionZstd, 4, 256*1024)
			// One part for the whole input, closed at EOF.
			cfg.MinPartSizeMB = 1024

			base := uploadFramesHeldValue(t)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			up := &heldPartUploader{err: tc.err, started: make(chan int64, 1), release: make(chan struct{})}
			done := make(chan error, 1)
			go func() {
				_, _, err := compressStream(ctx, bytes.NewReader(data), cfg, up, 1, nil)
				done <- err
			}()

			var capacity int64
			select {
			case capacity = <-up.started:
			case err := <-done:
				t.Fatalf("stream finished before uploading: %v", err)
			}
			assert.Equal(t, base+capacity, uploadFramesHeldValue(t), "while the part uploads")

			if tc.cancel {
				cancel()
			} else {
				close(up.release)
			}
			err := <-done
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.NoError(t, up.atCompleteErr)
				assert.Equal(t, base, up.atComplete, "once every part is uploaded")
			}

			assert.Equal(t, base, uploadFramesHeldValue(t), "after the stream returns")
		})
	}
}

// failAfterCountedReader serves data, then fails once counted reports that
// the stream has counted frames, so they belong to a part that never reaches
// the upload.
type failAfterCountedReader struct {
	data    *bytes.Reader
	counted func() (bool, error)
	err     error
}

func (r *failAfterCountedReader) Read(p []byte) (int, error) {
	if r.data.Len() > 0 {
		return r.data.Read(p)
	}

	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		ok, err := r.counted()
		if err != nil {
			return 0, err
		}
		if ok {
			return 0, r.err
		}
	}

	return 0, errors.New("frames were never counted")
}

//nolint:paralleltest // reads a process-wide counter that other streams would move
func TestCompressStreamReleasesFramesNeverUploaded(t *testing.T) {
	errRead := errors.New("read failed")
	cfg := defaultCfg(CompressionZstd, 4, 256*1024)

	base := uploadFramesHeldValue(t)
	in := &failAfterCountedReader{
		data: bytes.NewReader(generateSemiRandomData(4 * 256 * 1024)),
		counted: func() (bool, error) {
			v, err := collectUploadFramesHeld(t.Context())

			return v > base, err
		},
		err: errRead,
	}

	_, _, err := compressStream(t.Context(), in, cfg, &memPartUploader{}, 1, nil)
	require.ErrorIs(t, err, errRead)

	assert.Equal(t, base, uploadFramesHeldValue(t))
}
