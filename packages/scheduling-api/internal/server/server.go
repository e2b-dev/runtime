package server

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	unaryDeadline   = 10 * time.Second
	internalMessage = "internal error"
)

func New(tel *telemetry.Client, tlsConfig mtls.ServerConfig) *grpc.Server {
	return e2bgrpc.NewGRPCServer(
		tel,
		e2bgrpc.WithTransportCredentials(mtls.NewServerCredentials(tlsConfig)),
		e2bgrpc.WithUnaryInterceptors(mtls.UnaryServerInterceptor(tlsConfig)),
		e2bgrpc.WithStreamInterceptors(mtls.StreamServerInterceptor(tlsConfig)),
		e2bgrpc.WithRecoveryHandler(func(any) error { return status.Error(codes.Internal, internalMessage) }),
		e2bgrpc.WithUnaryDeadline(unaryDeadline),
		e2bgrpc.WithWaitForHandlers(),
	)
}
