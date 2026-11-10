package orchestrator

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
)

func TestRouteNodeIPAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		node  *nodemanager.Node
		local bool
		want  string
	}{
		{
			name:  "uses node ip when present",
			node:  &nodemanager.Node{ClusterID: uuid.New(), IPAddress: "10.0.0.1"},
			local: false,
			want:  "10.0.0.1",
		},
		{
			name:  "local cluster falls back for local ci",
			node:  &nodemanager.Node{ClusterID: consts.LocalClusterID},
			local: true,
			want:  localSandboxIPAddress,
		},
		{
			name:  "local cluster stays empty outside local env",
			node:  &nodemanager.Node{ClusterID: consts.LocalClusterID},
			local: false,
			want:  "",
		},
		{
			name:  "remote cluster stays empty",
			node:  &nodemanager.Node{ClusterID: uuid.New()},
			local: true,
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, routeNodeIPAddress(tt.node, tt.local))
		})
	}
}

func TestResumeResponse(t *testing.T) {
	t.Parallel()

	const executionID = "execution-a"
	errNoAddress := errors.New("no orchestrator address")
	egress := &orchestrator.SandboxNetworkPlacement{
		ExecutionId: executionID, NetworkMode: "egress-router", RouterId: "router-a", IngressAddress: "10.0.0.11", IngressPort: 5007,
	}
	full := &orchestrator.SandboxNetworkPlacement{
		ExecutionId: executionID, NetworkMode: "full-router", RouterId: "router-b", IngressAddress: "10.8.0.2", IngressPort: 5010,
	}
	fullWith := func(change func(*orchestrator.SandboxNetworkPlacement)) *orchestrator.SandboxNetworkPlacement {
		placement := proto.CloneOf(full)
		change(placement)

		return placement
	}

	tests := []struct {
		name        string
		placement   *orchestrator.SandboxNetworkPlacement
		address     string
		addressErr  error
		want        *proxygrpc.SandboxResumeResponse
		wantErr     string
		wantLookups int
	}{
		{
			name:        "legacy record is answered with the orchestrator address alone",
			address:     localSandboxIPAddress,
			want:        &proxygrpc.SandboxResumeResponse{OrchestratorIp: localSandboxIPAddress},
			wantLookups: 1,
		},
		{name: "legacy record without an orchestrator address fails", addressErr: errNoAddress, wantErr: errNoAddress.Error(), wantLookups: 1},
		{
			name:        "egress-router keeps its placement and the orchestrator address",
			placement:   egress,
			address:     "10.0.0.1",
			want:        &proxygrpc.SandboxResumeResponse{OrchestratorIp: "10.0.0.1", NetworkPlacement: egress},
			wantLookups: 1,
		},
		{name: "egress-router without an orchestrator address fails", placement: egress, addressErr: errNoAddress, wantErr: errNoAddress.Error(), wantLookups: 1},
		{name: "full-router never looks up an orchestrator", placement: full, addressErr: errNoAddress, want: &proxygrpc.SandboxResumeResponse{NetworkPlacement: full}},
		{name: "predecessor placement", placement: fullWith(func(p *orchestrator.SandboxNetworkPlacement) { p.ExecutionId = "execution-0" }), wantErr: "belongs to execution"},
		{name: "full-router without router", placement: fullWith(func(p *orchestrator.SandboxNetworkPlacement) { p.RouterId = "" }), wantErr: "no usable router ingress"},
		{name: "full-router without address", placement: fullWith(func(p *orchestrator.SandboxNetworkPlacement) { p.IngressAddress = "" }), wantErr: "no usable router ingress"},
		{name: "full-router without port", placement: fullWith(func(p *orchestrator.SandboxNetworkPlacement) { p.IngressPort = 0 }), wantErr: "no usable router ingress"},
		{name: "full-router port out of range", placement: fullWith(func(p *orchestrator.SandboxNetworkPlacement) { p.IngressPort = 65536 }), wantErr: "no usable router ingress"},
		{name: "unknown mode is not legacy", placement: fullWith(func(p *orchestrator.SandboxNetworkPlacement) { p.NetworkMode = "mesh" }), address: "10.0.0.1", wantErr: "unsupported mode"},
		{name: "placement without mode is not legacy", placement: fullWith(func(p *orchestrator.SandboxNetworkPlacement) { p.NetworkMode = "" }), address: "10.0.0.1", wantErr: "unsupported mode"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stored := proto.CloneOf(tt.placement)
			lookups := 0
			resp, err := resumeResponse(sandbox.Sandbox{ExecutionID: executionID, NetworkPlacement: tt.placement}, func() (string, error) {
				lookups++

				return tt.address, tt.addressErr
			})
			assert.Equal(t, tt.wantLookups, lookups)
			// The placement is the record's own object, which the store shares
			// with every reader, so the answer must not write into it.
			assert.True(t, proto.Equal(stored, tt.placement))
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.Nil(t, resp)

				return
			}
			require.NoError(t, err)
			assert.Truef(t, proto.Equal(tt.want, resp), "want %v, got %v", tt.want, resp)
		})
	}
}
