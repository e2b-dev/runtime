package filesystem

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"connectrpc.com/connect"
	"github.com/e2b-dev/fsnotify"

	"github.com/e2b-dev/infra/packages/envd/internal/logs"
	"github.com/e2b-dev/infra/packages/envd/internal/permissions"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/filesystem"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
)

func (s Service) WatchDir(ctx context.Context, req *connect.Request[rpc.WatchDirRequest], stream *connect.ServerStream[rpc.WatchDirResponse]) error {
	return logs.LogServerStreamWithoutEvents(ctx, s.logger, req, stream, s.watchHandler)
}

func (s Service) watchHandler(ctx context.Context, req *connect.Request[rpc.WatchDirRequest], stream *connect.ServerStream[rpc.WatchDirResponse]) error {
	u, err := permissions.GetAuthUser(ctx, s.defaults.User)
	if err != nil {
		return err
	}

	watchPath, err := permissions.ExpandAndResolve(req.Msg.GetPath(), u, s.defaults.Workdir)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}

	info, err := os.Stat(watchPath)
	if err != nil {
		if os.IsNotExist(err) {
			return connect.NewError(connect.CodeNotFound, fmt.Errorf("path %s not found: %w", watchPath, err))
		}

		return connect.NewError(connect.CodeInternal, fmt.Errorf("error statting path %s: %w", watchPath, err))
	}

	if !info.IsDir() {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("path %s is not a directory", watchPath))
	}

	// Check if path is on a network filesystem mount
	if !req.Msg.GetAllowNetworkMounts() {
		isNetworkMount, err := IsPathOnNetworkMount(watchPath)
		if err != nil {
			return connect.NewError(connect.CodeInternal, fmt.Errorf("error checking mount status: %w", err))
		}
		if isNetworkMount {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("cannot watch path on network filesystem: %s", watchPath))
		}
	}

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("error creating watcher: %w", err))
	}
	defer w.Close()

	err = w.Add(utils.FsnotifyPath(watchPath, req.Msg.GetRecursive()))
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("error adding path %s to watcher: %w", watchPath, err))
	}

	err = stream.Send(&rpc.WatchDirResponse{
		Event: &rpc.WatchDirResponse_Start{
			Start: &rpc.WatchDirResponse_StartEvent{},
		},
	})
	if err != nil {
		return connect.NewError(connect.CodeUnknown, fmt.Errorf("error sending start event: %w", err))
	}

	keepaliveTicker, resetKeepalive := permissions.GetKeepAliveTicker(req)
	defer keepaliveTicker.Stop()

	for {
		select {
		case <-keepaliveTicker.C:
			streamErr := stream.Send(&rpc.WatchDirResponse{
				Event: &rpc.WatchDirResponse_Keepalive{
					Keepalive: &rpc.WatchDirResponse_KeepAlive{},
				},
			})
			if streamErr != nil {
				return connect.NewError(connect.CodeUnknown, fmt.Errorf("error sending keepalive: %w", streamErr))
			}
		case <-ctx.Done():
			return ctx.Err()
		case chErr, ok := <-w.Errors:
			if !ok {
				return connect.NewError(connect.CodeInternal, errors.New("watcher error channel closed"))
			}

			return connect.NewError(connect.CodeInternal, fmt.Errorf("watcher error: %w", chErr))
		case e, ok := <-w.Events:
			if !ok {
				return connect.NewError(connect.CodeInternal, errors.New("watcher event channel closed"))
			}

			// One event can have multiple operations.
			ops := []rpc.EventType{}

			if fsnotify.Create.Has(e.Op) {
				ops = append(ops, rpc.EventType_EVENT_TYPE_CREATE)
			}

			if fsnotify.Rename.Has(e.Op) {
				ops = append(ops, rpc.EventType_EVENT_TYPE_RENAME)
			}

			if fsnotify.Chmod.Has(e.Op) {
				ops = append(ops, rpc.EventType_EVENT_TYPE_CHMOD)
			}

			if fsnotify.Write.Has(e.Op) {
				ops = append(ops, rpc.EventType_EVENT_TYPE_WRITE)
			}

			if fsnotify.Remove.Has(e.Op) {
				ops = append(ops, rpc.EventType_EVENT_TYPE_REMOVE)
			}

			for _, op := range ops {
				name, nameErr := filepath.Rel(watchPath, e.Name)
				if nameErr != nil {
					return connect.NewError(connect.CodeInternal, fmt.Errorf("error getting relative path: %w", nameErr))
				}

				filesystemEvent := &rpc.FilesystemEvent{
					Name: name,
					Type: op,
				}

				if req.Msg.GetIncludeEntry() && opCarriesEntry(op) {
					filesystemEvent.Entry = eventEntryInfo(s.logger, e.Name)
				}

				event := &rpc.WatchDirResponse{
					Event: &rpc.WatchDirResponse_Filesystem{
						Filesystem: filesystemEvent,
					},
				}

				if streamErr := stream.Send(event); streamErr != nil {
					return connect.NewError(connect.CodeUnknown, fmt.Errorf("error sending filesystem event: %w", streamErr))
				}

				resetKeepalive()
			}
		}
	}
}
