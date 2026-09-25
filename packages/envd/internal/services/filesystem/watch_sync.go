package filesystem

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"connectrpc.com/connect"
	"github.com/e2b-dev/fsnotify"
	"github.com/rs/zerolog"

	"github.com/e2b-dev/infra/packages/envd/internal/permissions"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/filesystem"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
)

type FileWatcher struct {
	watcher *fsnotify.Watcher
	Events  []*rpc.FilesystemEvent
	cancel  func()
	Error   error

	// Config captured so the watcher can be re-armed after an envd
	// live-upgrade. WatchPath is the already-resolved
	// absolute path.
	WatchPath        string
	Recursive        bool
	IncludeEntryInfo bool

	Lock sync.Mutex
}

func CreateFileWatcher(ctx context.Context, logger *zerolog.Logger, watchPath string, recursive bool, includeEntryInfo bool) (*FileWatcher, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error creating watcher: %w", err))
	}

	// We don't want to cancel the context when the request is finished
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))

	err = w.Add(utils.FsnotifyPath(watchPath, recursive))
	if err != nil {
		_ = w.Close()
		cancel()

		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error adding path %s to watcher: %w", watchPath, err))
	}
	fw := &FileWatcher{
		watcher:          w,
		cancel:           cancel,
		Events:           []*rpc.FilesystemEvent{},
		Error:            nil,
		WatchPath:        watchPath,
		Recursive:        recursive,
		IncludeEntryInfo: includeEntryInfo,
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case chErr, ok := <-w.Errors:
				if !ok {
					fw.setError(connect.NewError(connect.CodeInternal, errors.New("watcher error channel closed")))

					return
				}

				fw.setError(connect.NewError(connect.CodeInternal, fmt.Errorf("watcher error: %w", chErr)))

				return
			case e, ok := <-w.Events:
				if !ok {
					fw.setError(connect.NewError(connect.CodeInternal, errors.New("watcher event channel closed")))

					return
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
						fw.setError(connect.NewError(connect.CodeInternal, fmt.Errorf("error getting relative path: %w", nameErr)))

						return
					}

					filesystemEvent := &rpc.FilesystemEvent{
						Name: name,
						Type: op,
					}

					if includeEntryInfo && opCarriesEntry(op) {
						filesystemEvent.Entry = eventEntryInfo(logger, e.Name)
					}

					fw.Lock.Lock()
					fw.Events = append(fw.Events, filesystemEvent)
					fw.Lock.Unlock()
				}
			}
		}
	}()

	return fw, nil
}

func (fw *FileWatcher) Close() {
	_ = fw.watcher.Close()
	fw.cancel()
}

// setError records a terminal watcher error. Guarded by Lock so it is safe against
// concurrent GetWatcherEvents reads.
func (fw *FileWatcher) setError(err error) {
	fw.Lock.Lock()
	defer fw.Lock.Unlock()

	fw.Error = err
}

func (s Service) CreateWatcher(ctx context.Context, req *connect.Request[rpc.CreateWatcherRequest]) (*connect.Response[rpc.CreateWatcherResponse], error) {
	u, err := permissions.GetAuthUser(ctx, s.defaults.User)
	if err != nil {
		return nil, err
	}

	watchPath, err := permissions.ExpandAndResolve(req.Msg.GetPath(), u, s.defaults.Workdir)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	info, err := os.Stat(watchPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("path %s not found: %w", watchPath, err))
		}

		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error statting path %s: %w", watchPath, err))
	}

	if !info.IsDir() {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("path %s is not a directory", watchPath))
	}

	// Check if path is on a network filesystem mount
	if !req.Msg.GetAllowNetworkMounts() {
		isNetworkMount, err := IsPathOnNetworkMount(watchPath)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error checking mount status: %w", err))
		}
		if isNetworkMount {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("cannot watch path on network filesystem: %s", watchPath))
		}
	}

	watcherId := "w" + id.Generate()

	w, err := CreateFileWatcher(ctx, s.logger, watchPath, req.Msg.GetRecursive(), req.Msg.GetIncludeEntry())
	if err != nil {
		return nil, err
	}

	s.watchersMu.Lock()
	s.watchers.Store(watcherId, w)
	s.watchersMu.Unlock()

	return connect.NewResponse(&rpc.CreateWatcherResponse{
		WatcherId: watcherId,
	}), nil
}

func (s Service) GetWatcherEvents(_ context.Context, req *connect.Request[rpc.GetWatcherEventsRequest]) (*connect.Response[rpc.GetWatcherEventsResponse], error) {
	watcherId := req.Msg.GetWatcherId()

	w, ok := s.watchers.Load(watcherId)
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("watcher with id %s not found", watcherId))
	}

	// Serialize the drain against a live-upgrade handover snapshot
	// (ExportWatchersHold holds watchersMu across the execve). Draining a
	// watcher's events in that window would consume events the snapshot already
	// carried, so the re-armed watcher would re-deliver them after the swap.
	// watchersMu is taken before w.Lock, matching Export/CreateWatcher ordering.
	s.watchersMu.Lock()
	defer s.watchersMu.Unlock()

	w.Lock.Lock()
	defer w.Lock.Unlock()

	if w.Error != nil {
		return nil, w.Error
	}

	events := w.Events
	w.Events = []*rpc.FilesystemEvent{}

	return connect.NewResponse(&rpc.GetWatcherEventsResponse{
		Events: events,
	}), nil
}

func (s Service) RemoveWatcher(_ context.Context, req *connect.Request[rpc.RemoveWatcherRequest]) (*connect.Response[rpc.RemoveWatcherResponse], error) {
	watcherId := req.Msg.GetWatcherId()

	w, ok := s.watchers.Load(watcherId)
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("watcher with id %s not found", watcherId))
	}

	w.Close()
	s.watchersMu.Lock()
	s.watchers.Delete(watcherId)
	s.watchersMu.Unlock()

	return connect.NewResponse(&rpc.RemoveWatcherResponse{}), nil
}
