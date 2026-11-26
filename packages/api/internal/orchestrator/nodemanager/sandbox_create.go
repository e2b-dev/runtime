package nodemanager

import (
	"context"
	"net/http"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	sandbox_network "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-network"
)

func (n *Node) SandboxCreate(ctx context.Context, sbxRequest *orchestrator.SandboxCreateRequest) (*orchestrator.SandboxCreateResponse, error) {
	client, ctx := n.GetSandboxCreateCtx(ctx, sbxRequest)

	return client.Sandbox.Create(ctx, sbxRequest)
}

// ValidateLegacyProxyEndpoint checks the public-only policy when the selected
// node predates endpoint admission. New workers use their own DNS and CIDRs.
func (n *Node) ValidateLegacyProxyEndpoint(ctx context.Context, address string) error {
	if address == "" || slices.Contains(n.Capabilities(), sandbox_network.CapabilityProxyEndpointAdmission) {
		return nil
	}

	return sandbox_network.ValidatePublicProxyEndpoint(ctx, address, nil)
}

// LegacyProxyCreateRefusal returns the same client error as public-only
// admission when placement has no upgraded worker to try.
func (n *Node) LegacyProxyCreateRefusal(ctx context.Context, address string) error {
	if err := n.ValidateLegacyProxyEndpoint(ctx, address); err != nil {
		msg := sandbox_network.InvalidProxyEndpointPrefix + err.Error()
		st, detailErr := status.New(codes.InvalidArgument, msg).WithDetails(&orchestrator.UserError{
			Code: orchestrator.UserErrorCode_SANDBOX_CREATE_REFUSED, Message: msg, HttpStatus: http.StatusBadRequest,
		})
		if detailErr != nil {
			return detailErr
		}

		return st.Err()
	}

	return nil
}
