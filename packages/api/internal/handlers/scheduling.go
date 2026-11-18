package handlers

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
)

func newSchedulingClient(address string, creds credentials.TransportCredentials) (*grpc.ClientConn, error) {
	return newReadinessBalancedClient(address, creds, statepb.StateService_ServiceDesc.ServiceName, "scheduling-api")
}
