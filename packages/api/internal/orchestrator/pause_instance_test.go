package orchestrator

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
)

// TestBuildUpsertSnapshotParams_PreservesIam verifies the sandbox workload
// identity configuration is written into the paused-sandbox config so the
// Postgres snapshot supplies it to a later resume or fork.
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

		params := buildUpsertSnapshotParams(sbx, node, false)

		assert.Equal(t, in, params.Config.Iam)
	}
}
