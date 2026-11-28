//go:build linux

package optimize

import (
	"errors"
	"golang.org/x/sync/errgroup"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
)

// TestFinalizeUploadsSettled gates the optimize phase's metadata re-upload on
// the finalize phase's async uploads (see Build). The build's UploadErrGroup
// carries the finalize upload goroutine; without the wait, its metadata upload
// and the optimize phase's updateMetadata race on the same metadata object
// for the build, and the later writer wins — clobbering the prefetch mapping.
func TestFinalizeUploadsSettled(t *testing.T) {
	t.Parallel()

	t.Run("settled group reports success", func(t *testing.T) {
		t.Parallel()

		eg := &errgroup.Group{}
		eg.Go(func() error { return nil })
		require.NoError(t, eg.Wait()) // settle it first

		pb := &OptimizeBuilder{
			BuildContext: buildcontext.BuildContext{UploadErrGroup: eg},
		}

		assert.NoError(t, pb.finalizeUploadsSettled())
	})

	t.Run("failed upload reports error", func(t *testing.T) {
		t.Parallel()

		uploadErr := errors.New("upload failed")
		eg := &errgroup.Group{}
		eg.Go(func() error { return uploadErr })
		require.Error(t, eg.Wait()) // settle it with the failure

		pb := &OptimizeBuilder{
			BuildContext: buildcontext.BuildContext{UploadErrGroup: eg},
		}

		err := pb.finalizeUploadsSettled()
		require.Error(t, err)
		assert.ErrorIs(t, err, uploadErr)
	})

	t.Run("wait blocks until the finalize upload finishes", func(t *testing.T) {
		t.Parallel()

		// The ordering invariant this phase depends on: finalizeUploadsSettled
		// must not return while the finalize upload goroutine is still
		// running, so updateMetadata (called only after the gate) can never
		// interleave with the finalize goroutine's metadata upload.
		eg := &errgroup.Group{}
		started := make(chan struct{})
		release := make(chan struct{})
		eg.Go(func() error {
			close(started)
			<-release
			return nil
		})
		<-started // the goroutine is in flight

		pb := &OptimizeBuilder{
			BuildContext: buildcontext.BuildContext{UploadErrGroup: eg},
		}

		settled := make(chan error, 1)
		go func() { settled <- pb.finalizeUploadsSettled() }()

		select {
		case err := <-settled:
			t.Fatalf("finalizeUploadsSettled returned %v while the finalize upload was still in flight", err)
		case <-time.After(50 * time.Millisecond):
			// still blocked — the gate is holding, as required.
		}

		close(release)
		select {
		case err := <-settled:
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("finalizeUploadsSettled did not return after the upload finished")
		}
	})

	t.Run("nil group is treated as settled", func(t *testing.T) {
		t.Parallel()

		// The BuildContext is constructed by the builder; a nil group would
		// otherwise panic on Wait. Treat it as settled (nothing to wait for)
		// — matches how a build without in-flight uploads behaves.
		pb := &OptimizeBuilder{
			BuildContext: buildcontext.BuildContext{},
		}

		assert.NoError(t, pb.finalizeUploadsSettled())
	})
}
