package process

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	"github.com/e2b-dev/infra/packages/envd/internal/logs"
	"github.com/e2b-dev/infra/packages/envd/internal/permissions"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
)

func (s *Service) Connect(ctx context.Context, req *connect.Request[rpc.ConnectRequest], stream *connect.ServerStream[rpc.ConnectResponse]) error {
	return logs.LogServerStreamWithoutEvents(ctx, s.logger, req, stream, s.handleConnect)
}

func (s *Service) handleConnect(ctx context.Context, req *connect.Request[rpc.ConnectRequest], stream *connect.ServerStream[rpc.ConnectResponse]) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	proc, err := s.getProcess(req.Msg.GetProcess())
	if err != nil {
		// The process is gone from the live map. It may have exited during a
		// window when no client was subscribed (e.g. a live-upgrade handover
		// gap). Serve the retained terminal event if we still have it, so the
		// caller still learns the exit code.
		if ret, ok := s.lookupTerminated(req.Msg.GetProcess()); ok {
			return s.serveTerminated(stream, ret)
		}

		return err
	}

	exitChan := make(chan struct{})

	data, dataCancel := proc.DataEvent.Fork()
	defer dataCancel()

	end, endCancel := proc.EndEvent.Fork()
	defer endCancel()

	setStreamWriteDeadline(ctx)
	streamErr := stream.Send(&rpc.ConnectResponse{
		Event: &rpc.ProcessEvent{
			Event: &rpc.ProcessEvent_Start{
				Start: &rpc.ProcessEvent_StartEvent{
					Pid: proc.Pid(),
				},
			},
		},
	})
	clearStreamWriteDeadline(ctx)

	if streamErr != nil {
		return connect.NewError(connect.CodeUnknown, fmt.Errorf("error sending start event: %w", streamErr))
	}

	go func() {
		defer close(exitChan)

		keepaliveTicker, resetKeepalive := permissions.GetKeepAliveTicker(req)
		defer keepaliveTicker.Stop()

	dataLoop:
		for {
			select {
			case <-keepaliveTicker.C:
				setStreamWriteDeadline(ctx)
				streamErr := stream.Send(&rpc.ConnectResponse{
					Event: &rpc.ProcessEvent{
						Event: &rpc.ProcessEvent_Keepalive{
							Keepalive: &rpc.ProcessEvent_KeepAlive{},
						},
					},
				})
				clearStreamWriteDeadline(ctx)

				if streamErr != nil {
					cancel(connect.NewError(connect.CodeUnknown, fmt.Errorf("error sending keepalive: %w", streamErr)))

					return
				}
			case <-ctx.Done():
				cancel(ctx.Err())

				return
			case event, ok := <-data:
				if !ok {
					break dataLoop
				}

				setStreamWriteDeadline(ctx)
				streamErr := stream.Send(&rpc.ConnectResponse{
					Event: &rpc.ProcessEvent{
						Event: &event,
					},
				})
				clearStreamWriteDeadline(ctx)

				if streamErr != nil {
					cancel(connect.NewError(connect.CodeUnknown, fmt.Errorf("error sending data event: %w", streamErr)))

					return
				}

				resetKeepalive()
			}
		}

		select {
		case <-ctx.Done():
			cancel(ctx.Err())

			return
		case event, ok := <-end:
			if !ok {
				// The EndEvent was already emitted and closed before we
				// subscribed (the process exited during the no-subscriber
				// window). Serve the retained terminal event for THIS process if
				// we still have it. Checked here — not before subscribing — so a
				// reused PID is never served a previous process's exit.
				if ret, rok := s.terminated.Load(proc.Pid()); rok {
					if serr := stream.Send(&rpc.ConnectResponse{
						Event: &rpc.ProcessEvent{Event: &rpc.ProcessEvent_End{End: ret.end}},
					}); serr != nil {
						cancel(connect.NewError(connect.CodeUnknown, fmt.Errorf("error sending retained end event: %w", serr)))
					}

					return
				}
				cancel(connect.NewError(connect.CodeUnknown, errors.New("end event channel closed before sending end event")))

				return
			}

			setStreamWriteDeadline(ctx)
			streamErr := stream.Send(&rpc.ConnectResponse{
				Event: &rpc.ProcessEvent{
					Event: &event,
				},
			})
			clearStreamWriteDeadline(ctx)

			if streamErr != nil {
				cancel(connect.NewError(connect.CodeUnknown, fmt.Errorf("error sending end event: %w", streamErr)))

				return
			}
		}
	}()

	// Wait for the sender goroutine; returning early panics envd.
	<-exitChan

	return ctx.Err()
}

// lookupTerminated finds a retained terminal event by the same selector shape
// Connect accepts (pid or tag). It backs the work-item-#8 late-Connect path.
func (s *Service) lookupTerminated(selector *rpc.ProcessSelector) (*retainedExit, bool) {
	switch selector.GetSelector().(type) {
	case *rpc.ProcessSelector_Pid:
		return s.terminated.Load(selector.GetPid())
	case *rpc.ProcessSelector_Tag:
		tag := selector.GetTag()

		var found *retainedExit
		s.terminated.Range(func(_ uint32, v *retainedExit) bool {
			if v.tag != nil && *v.tag == tag {
				found = v

				return false
			}

			return true
		})

		return found, found != nil
	default:
		return nil, false
	}
}

// serveTerminated streams the retained Start+End pair for an already-exited
// process and returns, so a client that (re)connects after the exit still
// observes the terminal event and exit code.
func (s *Service) serveTerminated(stream *connect.ServerStream[rpc.ConnectResponse], ret *retainedExit) error {
	if err := stream.Send(&rpc.ConnectResponse{
		Event: &rpc.ProcessEvent{
			Event: &rpc.ProcessEvent_Start{
				Start: &rpc.ProcessEvent_StartEvent{
					Pid: ret.pid,
				},
			},
		},
	}); err != nil {
		return connect.NewError(connect.CodeUnknown, fmt.Errorf("error sending start event: %w", err))
	}

	if err := stream.Send(&rpc.ConnectResponse{
		Event: &rpc.ProcessEvent{
			Event: &rpc.ProcessEvent_End{
				End: ret.end,
			},
		},
	}); err != nil {
		return connect.NewError(connect.CodeUnknown, fmt.Errorf("error sending retained end event: %w", err))
	}

	return nil
}
