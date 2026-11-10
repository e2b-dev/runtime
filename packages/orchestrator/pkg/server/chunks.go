//go:build linux

package server

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerserver"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

var (
	peerNotAvailable = &orchestrator.PeerAvailability{NotAvailable: true}
	peerUseStorage   = &orchestrator.PeerAvailability{UseStorage: true}
)

// seekableStreamSender implements peerserver.Sender over a gRPC server stream (for seekable files).
type seekableStreamSender struct {
	stream orchestrator.ChunkService_ReadAtBuildSeekableServer
}

func (s *seekableStreamSender) Send(data []byte) error {
	return s.stream.Send(&orchestrator.ReadAtBuildSeekableResponse{Data: data})
}

// blobStreamSender implements peerserver.Sender over a gRPC server stream (for blob files).
type blobStreamSender struct {
	stream orchestrator.ChunkService_GetBuildBlobServer
}

func (s *blobStreamSender) Send(data []byte) error {
	return s.stream.Send(&orchestrator.GetBuildBlobResponse{Data: data})
}

// toGRPCError translates peerserver errors to gRPC status codes.
func toGRPCError(err error) error {
	switch {
	case errors.Is(err, peerserver.ErrUnknownFile),
		errors.Is(err, peerserver.ErrNotSupported):
		return status.Errorf(codes.InvalidArgument, "%v", err)
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}

func (s *Server) GetBuildFileSize(ctx context.Context, req *orchestrator.GetBuildFileSizeRequest) (*orchestrator.GetBuildFileSizeResponse, error) {
	telemetry.SetAttributes(ctx, telemetry.WithBuildID(req.GetBuildId()), attribute.String("file_name", req.GetName()))

	if s.uploadedBuilds.Get(req.GetBuildId()) != nil {
		telemetry.SetAttributes(ctx, attribute.Bool("uploaded", true))

		return &orchestrator.GetBuildFileSizeResponse{Availability: peerUseStorage}, nil
	}

	src, err := peerserver.ResolveSeekable(s.templateCache, req.GetBuildId(), req.GetName())
	if err != nil {
		if errors.Is(err, peerserver.ErrNotAvailable) {
			return &orchestrator.GetBuildFileSizeResponse{Availability: peerNotAvailable}, nil
		}

		return nil, toGRPCError(err)
	}

	telemetry.ReportEvent(ctx, "getting file size", telemetry.WithBuildID(req.GetBuildId()), attribute.String("file_name", req.GetName()))

	size, err := src.Size(ctx)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &orchestrator.GetBuildFileSizeResponse{TotalSize: size}, nil
}

func (s *Server) GetBuildFileExists(ctx context.Context, req *orchestrator.GetBuildFileExistsRequest) (*orchestrator.GetBuildFileExistsResponse, error) {
	telemetry.SetAttributes(ctx, telemetry.WithBuildID(req.GetBuildId()), attribute.String("file_name", req.GetName()))

	if s.uploadedBuilds.Get(req.GetBuildId()) != nil {
		telemetry.SetAttributes(ctx, attribute.Bool("uploaded", true))

		return &orchestrator.GetBuildFileExistsResponse{Availability: peerUseStorage}, nil
	}

	src, release, err := peerserver.ResolveBlob(ctx, s.templateCache, req.GetBuildId(), req.GetName())
	defer release()
	if err != nil {
		if errors.Is(err, peerserver.ErrNotAvailable) {
			return &orchestrator.GetBuildFileExistsResponse{Availability: peerNotAvailable}, nil
		}

		return nil, toGRPCError(err)
	}

	exists, err := src.Exists(ctx)
	if err != nil {
		return nil, toGRPCError(err)
	}

	if !exists {
		return &orchestrator.GetBuildFileExistsResponse{Availability: peerNotAvailable}, nil
	}

	return &orchestrator.GetBuildFileExistsResponse{}, nil
}

// ReadAtBuildSeekable streams a range from a seekable diff file (memfile, rootfs.ext4).
func (s *Server) ReadAtBuildSeekable(req *orchestrator.ReadAtBuildSeekableRequest, stream orchestrator.ChunkService_ReadAtBuildSeekableServer) error {
	ctx := stream.Context()
	offset := req.GetOffset()
	length := req.GetLength()

	if offset < 0 || length < 0 {
		return status.Error(codes.InvalidArgument, "offset and length must be non-negative")
	}

	telemetry.SetAttributes(ctx,
		telemetry.WithBuildID(req.GetBuildId()),
		attribute.String("file_name", req.GetName()),
		attribute.Int64("offset", offset),
		attribute.Int64("length", length),
	)

	if s.uploadedBuilds.Get(req.GetBuildId()) != nil {
		telemetry.SetAttributes(ctx, attribute.Bool("uploaded", true))

		return stream.Send(&orchestrator.ReadAtBuildSeekableResponse{Availability: peerUseStorage})
	}

	src, err := peerserver.ResolveSeekable(s.templateCache, req.GetBuildId(), req.GetName())
	if err != nil {
		if errors.Is(err, peerserver.ErrNotAvailable) {
			return stream.Send(&orchestrator.ReadAtBuildSeekableResponse{Availability: peerNotAvailable})
		}

		return toGRPCError(err)
	}

	if err := src.Stream(ctx, offset, length, &seekableStreamSender{stream}); err != nil {
		if errors.Is(err, peerserver.ErrNotAvailable) {
			return stream.Send(&orchestrator.ReadAtBuildSeekableResponse{Availability: peerNotAvailable})
		}

		return toGRPCError(err)
	}

	return nil
}

// GetBuildBlob streams an entire blob file (snapfile, metadata, headers).
func (s *Server) GetBuildBlob(req *orchestrator.GetBuildBlobRequest, stream orchestrator.ChunkService_GetBuildBlobServer) error {
	ctx := stream.Context()

	telemetry.SetAttributes(ctx,
		telemetry.WithBuildID(req.GetBuildId()),
		attribute.String("file_name", req.GetName()),
	)

	if s.uploadedBuilds.Get(req.GetBuildId()) != nil {
		telemetry.SetAttributes(ctx, attribute.Bool("uploaded", true))

		return stream.Send(&orchestrator.GetBuildBlobResponse{Availability: peerUseStorage})
	}

	src, release, err := peerserver.ResolveBlob(ctx, s.templateCache, req.GetBuildId(), req.GetName())
	defer release()
	if err != nil {
		if errors.Is(err, peerserver.ErrNotAvailable) {
			return stream.Send(&orchestrator.GetBuildBlobResponse{Availability: peerNotAvailable})
		}

		return toGRPCError(err)
	}

	if err := src.Stream(ctx, &blobStreamSender{stream}); err != nil {
		if errors.Is(err, peerserver.ErrNotAvailable) {
			return stream.Send(&orchestrator.GetBuildBlobResponse{Availability: peerNotAvailable})
		}

		return toGRPCError(err)
	}

	return nil
}
