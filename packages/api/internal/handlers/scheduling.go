package handlers

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
)

func newSchedulingClient(address string, creds credentials.TransportCredentials) (*grpc.ClientConn, error) {
	return newReadinessBalancedClient(address, creds, scheduling.StateService_ServiceDesc.ServiceName, "scheduling-api")
}
