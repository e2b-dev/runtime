package process

import (
	"context"
	"fmt"

	"connectrpc.com/connect"

	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
)

func (s *Service) Update(_ context.Context, req *connect.Request[rpc.UpdateRequest]) (*connect.Response[rpc.UpdateResponse], error) {
	proc, err := s.getProcess(req.Msg.GetProcess())
	if err != nil {
		return nil, err
	}

	if req.Msg.GetPty() != nil {
		size := req.Msg.GetPty().GetSize()
		err := proc.ResizeTty(uint16(size.GetCols()), uint16(size.GetRows()))
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error resizing tty: %w", err))
		}
	}

	return connect.NewResponse(&rpc.UpdateResponse{}), nil
}
