package orchestrator

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	orchestratorgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

type rejectedSandboxUpdateClient struct {
	orchestratorgrpc.SandboxServiceClient

	err error
}

func (c *rejectedSandboxUpdateClient) Update(context.Context, *orchestratorgrpc.SandboxUpdateRequest, ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, c.err
}

// This boundary fixture exercises the actual API/Store update consumers without
// Redis or a VM; the server tests separately bind the inactive-lifecycle code.
type updateRejectionStorage struct {
	sandbox.Storage

	sbx sandbox.Sandbox
}

func (s *updateRejectionStorage) Get(context.Context, uuid.UUID, string) (sandbox.Sandbox, error) {
	return s.sbx, nil
}

func (s *updateRejectionStorage) Update(_ context.Context, _ uuid.UUID, _ string, update func(sandbox.Sandbox) (sandbox.Sandbox, error)) (sandbox.Sandbox, error) {
	var err error
	s.sbx, err = update(s.sbx)

	return s.sbx, err
}

func TestSandboxUpdateRejectionHTTPConsumers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		code codes.Code
		want int
	}{
		{name: "inactive lifecycle", code: codes.NotFound, want: http.StatusNotFound},
		{name: "unrelated callback precondition", code: codes.FailedPrecondition, want: http.StatusInternalServerError},
		{name: "unrelated callback failure", code: codes.ResourceExhausted, want: http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sbx := sandbox.Sandbox{
				SandboxID: "inactive-update", TeamID: uuid.New(), NodeID: "node-update",
				ClusterID: uuid.Nil, State: sandbox.StateRunning,
				StartTime: time.Now(), EndTime: time.Now().Add(time.Hour), MaxInstanceLength: time.Hour,
			}
			storage := &updateRejectionStorage{sbx: sbx}
			o := &Orchestrator{
				sandboxStore: sandbox.NewStore(storage, nil, sandbox.Callbacks{}),
				nodes:        smap.New[*nodemanager.Node](),
			}
			node := nodemanager.NewTestNode(sbx.NodeID, api.NodeStatusReady, 0, 2)
			node.ClusterID = sbx.ClusterID
			node.SetSandboxClient(&rejectedSandboxUpdateClient{err: status.Error(tc.code, "update refused")})
			o.registerNode(node)
			_, timeoutErr := o.KeepAliveFor(t.Context(), sbx.TeamID, sbx.SandboxID, time.Minute, true)
			require.NotNil(t, timeoutErr)
			require.Equal(t, tc.want, timeoutErr.Code)
			networkErr := o.UpdateSandboxNetworkConfig(t.Context(), sbx.TeamID, sbx.SandboxID, nil, nil, nil, nil, nil, nil)
			require.NotNil(t, networkErr)
			require.Equal(t, tc.want, networkErr.Code)
		})
	}
}
