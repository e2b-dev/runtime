package orchestrator

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
)

// TestBuildUpsertSnapshotParams_PreservesIam verifies the sandbox workload
// identity configuration is written into the paused-sandbox config so it
// survives a pause/resume cycle through Postgres.
func TestBuildUpsertSnapshotParams_PreservesIam(t *testing.T) {
	t.Parallel()

	node := &nodemanager.Node{ID: "node-1"}

	configured := &types.SandboxIam{Tokens: map[string]types.SandboxIamToken{"aws": {Audience: "sts.amazonaws.com", TokenType: "JWT-SVID"}}}

	for _, in := range []*types.SandboxIam{configured, nil} {
		sbx := sandbox.Sandbox{
			SandboxID:      "sbx-1",
			BaseTemplateID: "tmpl",
			BuildID:        uuid.New(),
			Iam:            in,
		}

		params := buildUpsertSnapshotParams(sbx, node, false, false)

		assert.Equal(t, in, params.Config.Iam)
	}
}

// TestBuildUpsertSnapshotParams_FilesystemOnly verifies the snapshot kind is
// written into the paused-sandbox config: the resume and create paths read it
// to pick a cold boot over a memory restore.
func TestBuildUpsertSnapshotParams_FilesystemOnly(t *testing.T) {
	t.Parallel()

	node := &nodemanager.Node{ID: "node-1"}
	sbx := sandbox.Sandbox{SandboxID: "sbx-1", BaseTemplateID: "tmpl", BuildID: uuid.New()}

	for _, filesystemOnly := range []bool{true, false} {
		params := buildUpsertSnapshotParams(sbx, node, filesystemOnly, filesystemOnly)

		assert.Equal(t, filesystemOnly, params.Config.FilesystemOnly)
	}
}

// A pause records the memory a sandbox runs with on the new snapshot build, so
// it takes effect only once that build is ready, and boot RAM stays on ram_mb. A
// record without the size records none, so the next start sends none.
func TestBuildUpsertSnapshotParams_StartMemoryMB(t *testing.T) {
	t.Parallel()

	node := &nodemanager.Node{ID: "node-1"}
	for _, tt := range []struct {
		memoryMB int64
		want     *int64
	}{
		{memoryMB: 0, want: nil},
		{memoryMB: 1024, want: new(int64(1024))},
		{memoryMB: 3072, want: new(int64(3072))},
	} {
		sbx := sandbox.Sandbox{SandboxID: "sbx-1", BaseTemplateID: "tmpl", BuildID: uuid.New(), RamMB: 1024, MemoryMB: tt.memoryMB}

		params := buildUpsertSnapshotParams(sbx, node, false, false)

		assert.Equal(t, int64(1024), params.RamMb)
		assert.Equal(t, tt.want, params.StartMemoryMb)
	}
}
