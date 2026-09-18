package process

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"connectrpc.com/connect"

	"github.com/e2b-dev/infra/packages/envd/internal/logs"
	"github.com/e2b-dev/infra/packages/envd/internal/permissions"
	"github.com/e2b-dev/infra/packages/envd/internal/services/process/handler"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
)

func (s *Service) Start(ctx context.Context, req *connect.Request[rpc.StartRequest], stream *connect.ServerStream[rpc.StartResponse]) error {
	return logs.LogServerStreamWithoutEvents(ctx, s.logger, req, stream, s.handleStart)
}

func (s *Service) handleStart(ctx context.Context, req *connect.Request[rpc.StartRequest], stream *connect.ServerStream[rpc.StartResponse]) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	handlerL := s.logger.With().Str(string(logs.OperationIDKey), ctx.Value(logs.OperationIDKey).(string)).Logger()

	u, err := permissions.GetAuthUser(ctx, s.defaults.User)
	if err != nil {
		return err
	}

	requestTimeout, err := determineTimeoutFromHeader(stream.Conn().RequestHeader())
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Create a new context with a timeout if provided.
	// We do not want the command to be killed if the request context is cancelled
	procCtx, cancelProc := context.Background(), func() {}
	if requestTimeout > 0 { // zero timeout means no timeout
		procCtx, cancelProc = context.WithTimeout(procCtx, requestTimeout)
	}

	// Hold snapshotMu.RLock across the whole fork+register span so a live-upgrade
	// snapshot (Upgrade takes the write lock) can never observe a child spawned
	// but not yet in s.processes — which would leave it surviving the execve with
	// no carried handler. It must be taken BEFORE handler.New: for a PTY,
	// handler.New's pty.StartWithSize already forks the child, so acquiring it
	// only before proc.Start would miss that fork. RLock so concurrent Starts
	// don't serialize with each other, only against the (rare) upgrade.
	s.snapshotMu.RLock()

	proc, err := handler.New( //nolint:contextcheck // TODO: fix this later
		procCtx,
		u,
		req.Msg,
		&handlerL,
		s.defaults,
		s.cgroupManager,
		cancelProc,
	)
	if err != nil {
		s.snapshotMu.RUnlock()
		// Ensure the process cancel is called to cleanup resources.
		cancelProc()

		return err
	}

	exitChan := make(chan struct{})

	// Buffered so the send below never blocks when the receiver
	// goroutine has already exited on a cancelled context.
	start := make(chan rpc.ProcessEvent_Start, 1)

	data, dataCancel := proc.DataEvent.Fork()
	defer dataCancel()

	end, endCancel := proc.EndEvent.Fork()
	defer endCancel()

	go func() {
		defer close(exitChan)

		select {
		case <-ctx.Done():
			cancel(ctx.Err())

			return
		case event := <-start:
			setStreamWriteDeadline(ctx)
			streamErr := stream.Send(&rpc.StartResponse{
				Event: &rpc.ProcessEvent{
					Event: &event,
				},
			})
			clearStreamWriteDeadline(ctx)

			if streamErr != nil {
				cancel(connect.NewError(connect.CodeUnknown, fmt.Errorf("error sending start event: %w", streamErr)))

				return
			}
		}

		keepaliveTicker, resetKeepalive := permissions.GetKeepAliveTicker(req)
		defer keepaliveTicker.Stop()

	dataLoop:
		for {
			select {
			case <-keepaliveTicker.C:
				setStreamWriteDeadline(ctx)
				streamErr := stream.Send(&rpc.StartResponse{
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
				streamErr := stream.Send(&rpc.StartResponse{
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
				cancel(connect.NewError(connect.CodeUnknown, errors.New("end event channel closed before sending end event")))

				return
			}

			setStreamWriteDeadline(ctx)
			streamErr := stream.Send(&rpc.StartResponse{
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

	pid, err := proc.Start(requestTimeout)
	if err != nil {
		s.snapshotMu.RUnlock()

		return connect.NewError(handler.StartErrorCode(err), err)
	}

	// Drop any retained exit left over from a previous process that used this
	// PID, so a Connect to the new process can't be served the old exit code.
	s.terminated.Delete(pid)
	// A Connect can also resolve by tag (lookupTerminated returns the first tag
	// match), so if this process reuses a tag, drop any predecessor's retained
	// exit under that tag too — else a late Connect-by-tag could get a stale code.
	if proc.Tag != nil {
		s.clearTerminatedForTag(*proc.Tag)
	}
	s.processes.Store(pid, proc)
	s.snapshotMu.RUnlock()

	// Retain the terminal event synchronously when the process exits — Wait
	// invokes this hook before it closes EndEvent, so a late Connect that falls
	// back to the retention cache is guaranteed to find the exit (no race with an
	// async retain). Set before the reaper goroutine below can run. Same
	// mechanism the re-adopt path uses.
	proc.OnExit = func(end *rpc.ProcessEvent_EndEvent) {
		s.finalizeTermination(pid, proc, end)
	}

	start <- rpc.ProcessEvent_Start{
		Start: &rpc.ProcessEvent_StartEvent{
			Pid: pid,
		},
	}

	go func() {
		// Reap the process. Removal from s.processes is owned solely by
		// finalizeTermination (invoked via proc.OnExit above), which retains the
		// exit code BEFORE deleting and is identity-guarded against PID reuse.
		// Deleting here too would race that retention away and lose the exit for a
		// late Connect or the pre-upgrade handover.
		proc.Wait()
	}()

	// Wait for the sender goroutine; returning early panics envd.
	<-exitChan

	return ctx.Err()
}

func determineTimeoutFromHeader(header http.Header) (time.Duration, error) {
	timeoutHeader := header.Get("Connect-Timeout-Ms")

	if timeoutHeader == "" {
		return 0, nil
	}

	timeout, err := strconv.Atoi(timeoutHeader)
	if err != nil {
		return 0, err
	}

	return time.Duration(timeout) * time.Millisecond, nil
}
