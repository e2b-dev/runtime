package orchestrator

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// shrinkRefusingSandboxClient is a node that refuses every create as a memory shrink.
type shrinkRefusingSandboxClient struct {
	orchestrator.SandboxServiceClient

	err error
}

func (c *shrinkRefusingSandboxClient) Create(context.Context, *orchestrator.SandboxCreateRequest, ...grpc.CallOption) (*orchestrator.SandboxCreateResponse, error) {
	return nil, c.err
}

// A node refusing a named size is the caller's error only when the size is below what
// the API records; at or above it, the API's record disagrees with the node's snapshot.
func TestCreateSandbox_NamedSizeRefusalBlamesTheRecordWhenItIsAtOrAboveIt(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		startMB   int64
		requested int32
		status    int
		code      string
	}{
		{name: "named size equal to the recorded size", startMB: 2048, requested: 2048, status: http.StatusInternalServerError, code: errCodeSandboxMemoryRecordMismatch},
		{name: "named size above the recorded size", startMB: 2048, requested: 3072, status: http.StatusInternalServerError, code: errCodeSandboxMemoryRecordMismatch},
		{name: "named size on a build without a recorded size", requested: 2048, status: http.StatusBadRequest, code: errCodeSandboxMemoryShrinkUnsupported},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := &shrinkRefusingSandboxClient{err: createRefusedStatusWith(t, codes.InvalidArgument,
				"memory hotplug refused (shrink): the requested memory does not fit the sandbox: a memory resume cannot shrink the sandbox", http.StatusBadRequest)}
			o := newCreateSandboxTestOrchestrator(t,
				nodemanager.WithOrchestratorVersion(clusters.SandboxMemoryResizeMinVersion.String()),
				func(node *nodemanager.TestNode) { node.SetSandboxClient(client) },
			)
			o.featureFlagsClient = sandboxCPUFlags(t, true)

			now := time.Now()
			_, apiErr := o.CreateSandbox(t.Context(), "sbx-refused-"+uuid.New().String()[:8], uuid.New().String(), memoryTeam(),
				func(_ context.Context) (SandboxMetadata, *api.APIError) {
					return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "base-tpl", Build: growableBuild(4096, tt.startMB)}, nil
				},
				now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{IsResume: true}, WithMemoryMB(new(tt.requested)))
			require.NotNil(t, apiErr)
			assert.Equal(t, tt.status, apiErr.Code)
			assert.Equal(t, tt.code, apiErr.ErrorCode)
		})
	}
}
