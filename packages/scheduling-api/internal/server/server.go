package server

import (
	"fmt"
	"time"

	"buf.build/go/protovalidate"
	protovalidatemw "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/reservation"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/state"
	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	reservationpb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/reservation"
	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	// unaryDeadline must stay above the store's one-minute wait for a
	// record's lock, or a state call that waits for it fails where the API's
	// in-process call finishes. maxWait must stay below unaryDeadline, or a
	// long poll that waits the whole time returns DEADLINE_EXCEEDED instead of
	// WAITING.
	unaryDeadline = 70 * time.Second
	maxWait       = 5 * time.Second
	// maxMessageSize must fit one sandbox record in one message.
	maxMessageSize  = 32 << 20
	internalMessage = "internal error"
)

func New(tel *telemetry.Client, tlsConfig mtls.ServerConfig, reservations sandboxtypes.ReservationStorage, storage sandboxtypes.Storage) (*grpc.Server, error) {
	validator, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("create the request validator: %w", err)
	}

	recoverPanic := func(any) error { return status.Error(codes.Internal, internalMessage) }
	server := e2bgrpc.NewGRPCServer(
		tel,
		e2bgrpc.WithTransportCredentials(mtls.NewServerCredentials(tlsConfig)),
		e2bgrpc.WithUnaryInterceptors(mtls.UnaryServerInterceptor(tlsConfig)),
		// The shared server recovers panics of unary handlers only.
		e2bgrpc.WithStreamInterceptors(recovery.StreamServerInterceptor(recovery.WithRecoveryHandler(recoverPanic))),
		e2bgrpc.WithStreamInterceptors(mtls.StreamServerInterceptor(tlsConfig)),
		e2bgrpc.WithRecoveryHandler(recoverPanic),
		e2bgrpc.WithUnaryDeadline(unaryDeadline),
		e2bgrpc.WithUnaryInterceptors(protovalidatemw.UnaryServerInterceptor(validator)),
		e2bgrpc.WithStreamInterceptors(protovalidatemw.StreamServerInterceptor(validator)),
		e2bgrpc.WithMaxMessageSize(maxMessageSize),
		e2bgrpc.WithWaitForHandlers(),
	)
	reservationpb.RegisterReservationServiceServer(server, reservation.NewServer(reservations, maxWait))
	statepb.RegisterStateServiceServer(server, state.NewServer(storage, maxWait))

	return server, nil
}
