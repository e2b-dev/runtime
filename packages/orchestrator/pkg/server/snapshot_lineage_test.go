//go:build linux

package server

import (
	"testing"

	"github.com/stretchr/testify/assert"

	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	templatemocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/mocks"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// A snapshot names the template its sandbox ran on as its predecessor, and
// abandons it unless the sandbox keeps running on it, as an in-place
// checkpoint does.
func TestSnapshotLineage(t *testing.T) {
	t.Parallel()

	files := storage.CachePaths{Paths: storage.Paths{BuildID: "predecessor-build"}}

	for _, tc := range []struct {
		name            string
		origin          storage.ObjectOrigin
		maintainSandbox bool
		want            sbxtemplate.SnapshotLineage
	}{
		{
			name:   "pause abandons its predecessor",
			origin: storage.ObjectOriginPause,
			want:   sbxtemplate.SnapshotLineage{Origin: storage.ObjectOriginPause, Predecessor: files.CacheKey(), AbandonsPredecessor: true},
		},
		{
			name:            "in-place checkpoint keeps its predecessor",
			origin:          storage.ObjectOriginSnapshotTemplate,
			maintainSandbox: true,
			want:            sbxtemplate.SnapshotLineage{Origin: storage.ObjectOriginSnapshotTemplate, Predecessor: files.CacheKey()},
		},
		{
			name:   "resume-fresh checkpoint abandons it under its own origin",
			origin: storage.ObjectOriginSnapshotTemplate,
			want:   sbxtemplate.SnapshotLineage{Origin: storage.ObjectOriginSnapshotTemplate, Predecessor: files.CacheKey(), AbandonsPredecessor: true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			predecessor := templatemocks.NewMockTemplate(t)
			predecessor.EXPECT().Files().Return(files)

			assert.Equal(t, tc.want, snapshotLineage(tc.origin, predecessor, tc.maintainSandbox))
		})
	}
}
