package filesystem

import (
	"context"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"connectrpc.com/authn"
	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/filesystem"
)

// collectEvents polls GetWatcherEvents until at least one event is returned or the
// deadline is reached. fsnotify delivers events asynchronously, so we can't assume
// they are available immediately after the filesystem operation.
func collectEvents(t *testing.T, ctx context.Context, svc Service, watcherID string) []*filesystem.FilesystemEvent {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := svc.GetWatcherEvents(ctx, connect.NewRequest(&filesystem.GetWatcherEventsRequest{
			WatcherId: watcherID,
		}))
		require.NoError(t, err)

		if len(resp.Msg.GetEvents()) > 0 {
			return resp.Msg.GetEvents()
		}

		time.Sleep(20 * time.Millisecond)
	}

	return nil
}

func TestWatcherIncludeEntryInfo(t *testing.T) {
	t.Parallel()

	u, err := user.Current()
	require.NoError(t, err)

	tests := []struct {
		name             string
		includeEntryInfo bool
		wantEntry        bool
	}{
		{name: "entry info included when requested", includeEntryInfo: true, wantEntry: true},
		{name: "entry info omitted when not requested", includeEntryInfo: false, wantEntry: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			svc := mockService()
			ctx := authn.SetInfo(t.Context(), u)

			created, err := svc.CreateWatcher(ctx, connect.NewRequest(&filesystem.CreateWatcherRequest{
				Path:         root,
				IncludeEntry: tt.includeEntryInfo,
			}))
			require.NoError(t, err)
			watcherID := created.Msg.GetWatcherId()
			t.Cleanup(func() {
				_, _ = svc.RemoveWatcher(context.WithoutCancel(ctx), connect.NewRequest(&filesystem.RemoveWatcherRequest{
					WatcherId: watcherID,
				}))
			})

			// Trigger an event that leaves a stat-able entry behind.
			filePath := filepath.Join(root, "file.txt")
			require.NoError(t, os.WriteFile(filePath, []byte("hello"), 0o644))

			events := collectEvents(t, ctx, svc, watcherID)
			require.NotEmpty(t, events, "expected at least one filesystem event")

			for _, e := range events {
				assert.Equal(t, "file.txt", e.GetName())

				if tt.wantEntry {
					require.NotNil(t, e.GetEntry(), "expected entry info on event %s", e.GetType())
					assert.Equal(t, "file.txt", e.GetEntry().GetName())
					assert.Equal(t, filePath, e.GetEntry().GetPath())
					assert.Equal(t, filesystem.FileType_FILE_TYPE_FILE, e.GetEntry().GetType())
				} else {
					assert.Nil(t, e.GetEntry(), "expected no entry info on event %s", e.GetType())
				}
			}
		})
	}
}

// TestWatcherIncludeEntryInfo_RemoveDoesNotCarryReplacement guards against a TOCTOU race:
// if an entry is removed and a different entry is created at the same path before the
// watcher handles the remove event, stat-ing the path would succeed and could attach the
// replacement's info to the remove event. Remove/rename events must never carry entry info.
func TestWatcherIncludeEntryInfo_RemoveDoesNotCarryReplacement(t *testing.T) {
	t.Parallel()

	u, err := user.Current()
	require.NoError(t, err)

	root := t.TempDir()

	// File exists before we start watching, so its removal is what we observe.
	filePath := filepath.Join(root, "file.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("hello"), 0o644))

	svc := mockService()
	ctx := authn.SetInfo(t.Context(), u)

	created, err := svc.CreateWatcher(ctx, connect.NewRequest(&filesystem.CreateWatcherRequest{
		Path:         root,
		IncludeEntry: true,
	}))
	require.NoError(t, err)
	watcherID := created.Msg.GetWatcherId()
	t.Cleanup(func() {
		_, _ = svc.RemoveWatcher(context.WithoutCancel(ctx), connect.NewRequest(&filesystem.RemoveWatcherRequest{
			WatcherId: watcherID,
		}))
	})

	require.NoError(t, os.Remove(filePath))
	// Recreate a different entry at the same path before the watcher handles the remove
	// event, so the path is occupied (and stat-able) by the time the event is processed.
	require.NoError(t, os.WriteFile(filePath, []byte("replacement"), 0o644))

	// Accumulate events until we have observed both the removal and the replacement, or time out.
	var removeEvent, replacementEvent *filesystem.FilesystemEvent
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && (removeEvent == nil || replacementEvent == nil) {
		resp, eventsErr := svc.GetWatcherEvents(ctx, connect.NewRequest(&filesystem.GetWatcherEventsRequest{
			WatcherId: watcherID,
		}))
		require.NoError(t, eventsErr)

		for _, e := range resp.Msg.GetEvents() {
			switch e.GetType() {
			case filesystem.EventType_EVENT_TYPE_REMOVE:
				removeEvent = e
			case filesystem.EventType_EVENT_TYPE_CREATE, filesystem.EventType_EVENT_TYPE_WRITE:
				if replacementEvent == nil {
					replacementEvent = e
				}
			}
		}

		time.Sleep(20 * time.Millisecond)
	}

	require.NotNil(t, removeEvent, "expected a remove event")
	assert.Nil(t, removeEvent.GetEntry(), "remove event must not carry entry info even when a new entry occupies the path")

	// Sanity check: the replacement is stat-able, so the nil above is by design — not just
	// because the path happens to be empty.
	require.NotNil(t, replacementEvent, "expected a create/write event for the replacement")
	assert.NotNil(t, replacementEvent.GetEntry(), "event for the existing replacement should carry entry info")
}

func TestCreateWatcherOnNetworkMount(t *testing.T) {
	t.Parallel()

	// FUSE mounts via bindfs are exercised on Linux only.
	if runtime.GOOS != "linux" {
		t.Skip("FUSE bindfs mount test runs only on Linux")
	}

	_, err := exec.LookPath("bindfs")
	require.NoError(t, err, "bindfs must be installed for this test")
	_, err = exec.LookPath("fusermount")
	require.NoError(t, err, "fusermount must be installed for this test")

	u, err := user.Current()
	require.NoError(t, err)

	sourceDir := t.TempDir()
	mountDir := t.TempDir()

	require.NoError(t, exec.CommandContext(t.Context(), "bindfs", sourceDir, mountDir).Run(), "failed to mount bindfs")
	t.Cleanup(func() {
		_ = exec.CommandContext(context.WithoutCancel(t.Context()), "fusermount", "-u", mountDir).Run()
	})

	svc := mockService()
	ctx := authn.SetInfo(t.Context(), u)

	// Without the flag, watching a network mount is rejected.
	_, err = svc.CreateWatcher(ctx, connect.NewRequest(&filesystem.CreateWatcherRequest{
		Path: mountDir,
	}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// With allow_network_mounts, the watcher is created.
	created, err := svc.CreateWatcher(ctx, connect.NewRequest(&filesystem.CreateWatcherRequest{
		Path:               mountDir,
		AllowNetworkMounts: true,
	}))
	require.NoError(t, err)
	watcherID := created.Msg.GetWatcherId()
	t.Cleanup(func() {
		_, _ = svc.RemoveWatcher(context.WithoutCancel(ctx), connect.NewRequest(&filesystem.RemoveWatcherRequest{
			WatcherId: watcherID,
		}))
	})
	assert.NotEmpty(t, watcherID)
}

func TestCreateWatcher_NotADirectory(t *testing.T) {
	t.Parallel()

	u, err := user.Current()
	require.NoError(t, err)

	filePath := filepath.Join(t.TempDir(), "file.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("content"), 0o644))

	svc := mockService()
	ctx := authn.SetInfo(t.Context(), u)

	_, err = svc.CreateWatcher(ctx, connect.NewRequest(&filesystem.CreateWatcherRequest{
		Path: filePath,
	}))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "is not a directory")
	assert.NotContains(t, err.Error(), "%!w")
}

