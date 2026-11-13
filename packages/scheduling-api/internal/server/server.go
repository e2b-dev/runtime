package server

import (
	"fmt"
	"time"

	"buf.build/go/protovalidate"
	protovalidatemw "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/reservation"
	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	unaryDeadline = 10 * time.Second
	// maxWait must stay below unaryDeadline, or a WaitForStart that waits the
	// whole time returns DEADLINE_EXCEEDED instead of WAITING.
	maxWait         = 5 * time.Second
	internalMessage = "internal error"
)

func New(tel *telemetry.Client, tlsConfig mtls.ServerConfig, reservations sandboxtypes.ReservationStorage) (*grpc.Server, error) {
	validator, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("create the request validator: %w", err)
	}

	server := e2bgrpc.NewGRPCServer(
		tel,
		e2bgrpc.WithTransportCredentials(mtls.NewServerCredentials(tlsConfig)),
		e2bgrpc.WithUnaryInterceptors(mtls.UnaryServerInterceptor(tlsConfig)),
		e2bgrpc.WithStreamInterceptors(mtls.StreamServerInterceptor(tlsConfig)),
		e2bgrpc.WithRecoveryHandler(func(any) error { return status.Error(codes.Internal, internalMessage) }),
		e2bgrpc.WithUnaryDeadline(unaryDeadline),
		e2bgrpc.WithUnaryInterceptors(protovalidatemw.UnaryServerInterceptor(validator)),
		e2bgrpc.WithWaitForHandlers(),
	)
	scheduling.RegisterReservationServiceServer(server, reservation.NewServer(reservations, maxWait))

	return server, nil
}
