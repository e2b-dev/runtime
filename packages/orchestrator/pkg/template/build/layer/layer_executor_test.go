//go:build linux

package layer

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// A build layer's upload returns its pin either way. A failed one is
// abandoned, since its build fails and nothing will read it, so it does not
// count among the pause layers that did not land. Either way its waiters wake
// with the upload's error.
func TestEndLayerUpload(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		uploadErr error
		want      template.UploadOutcome
	}{
		{name: "landed", want: template.UploadLanded},
		{name: "failed", uploadErr: errors.New("storage down"), want: template.UploadAbandoned},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			uploads := sandbox.NewUploads(nil, nil, nil, nil, nil)
			t.Cleanup(uploads.Stop)

			var got []template.UploadOutcome
			snap := &sandbox.Snapshot{BuildID: uuid.New(), FilesystemSnapshot: true, RootfsBlockSize: 4096}
			upload, err := sandbox.NewUpload(t.Context(), uploads, snap, nil, storage.CompressConfig{}, nil, storage.UseCaseBuild, nil,
				func(o template.UploadOutcome) { got = append(got, o) })
			require.NoError(t, err)

			endLayerUpload(t.Context(), upload, tc.uploadErr)
			assert.Equal(t, []template.UploadOutcome{tc.want}, got)
			assert.ErrorIs(t, upload.Wait(t.Context()), tc.uploadErr, "a waiter wakes with the upload's outcome")
		})
	}
}
