package orchestrator

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
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

		params := buildUpsertSnapshotParams(sbx, node, false)

		assert.Equal(t, in, params.Config.Iam)
	}
}

func TestBuildUpsertSnapshotParams_PreservesRemainingLifetime(t *testing.T) {
	t.Parallel()

	sbx := sandbox.Sandbox{
		SandboxID: "sbx-1", BaseTemplateID: "tmpl", BuildID: uuid.New(),
	}
	node := &nodemanager.Node{ID: "node-1"}
	params := buildUpsertSnapshotParams(sbx, node, false, 37*time.Minute)

	assert.Equal(t, uint64((37 * time.Minute).Seconds()), params.Config.RemainingLifetimeSeconds)

	subsecond := buildUpsertSnapshotParams(sbx, node, false, 500*time.Millisecond)
	assert.Equal(t, uint64(1), subsecond.Config.RemainingLifetimeSeconds)
}
