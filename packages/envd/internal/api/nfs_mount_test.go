//go:build unix

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeNFS records mounts. A path's mount ID changes when it is mounted, unless the
// path is in noop, which models a mount that reports success but does nothing.
type fakeNFS struct {
	ids       map[string]uint64
	next      uint64
	mounted   []string
	nconnects []int    // nconnect of each recorded mount, parallel to mounted
	ops       []string // "unmount <path>" and "mount <path>", in call order
	failing   map[string]error
	noop      map[string]bool
	timeouts  map[string]int    // mounts of the path that time out before one succeeds
	refusals  map[string]int    // mounts of the path that fail at once before one succeeds
	untimed   []string          // paths whose mount point was created without a deadline
	covers    map[string]string // mounting the key covers the value's mount
}

func newFakeNFS() *fakeNFS {
	return &fakeNFS{
		ids: map[string]uint64{}, next: 100, failing: map[string]error{}, noop: map[string]bool{},
		timeouts: map[string]int{}, refusals: map[string]int{}, covers: map[string]string{},
	}
}

func (f *fakeNFS) Mount(_ context.Context, _, path string, nconnect int) error {
	f.mounted = append(f.mounted, path)
	f.nconnects = append(f.nconnects, nconnect)
	f.ops = append(f.ops, "mount "+path)
	if err := f.failing[path]; err != nil {
		return err
	}
	if f.timeouts[path] > 0 {
		f.timeouts[path]--

		return context.DeadlineExceeded
	}
	if f.refusals[path] > 0 {
		f.refusals[path]--

		return errors.New("mount.nfs: Connection refused")
	}
	if covered, ok := f.covers[path]; ok {
		f.next++
		f.ids[covered] = f.next
	}
	if !f.noop[path] {
		f.next++
		f.ids[path] = f.next
	}

	return nil
}

func (f *fakeNFS) MakeDir(ctx context.Context, path string) error {
	if _, ok := ctx.Deadline(); !ok {
		f.untimed = append(f.untimed, path)
	}

	return os.MkdirAll(path, 0o755)
}

func (f *fakeNFS) Unmount(path string) error {
	f.ops = append(f.ops, "unmount "+path)

	return nil
}

func (f *fakeNFS) MountID(path string) (uint64, error) { return f.ids[path], nil }

func newNFSTestAPI(t *testing.T) (*API, *fakeNFS, string) {
	t.Helper()

	fake := newFakeNFS()

	return &API{nfs: fake}, fake, t.TempDir()
}

func volumes(root string, paths ...string) []VolumeMount {
	out := make([]VolumeMount, 0, len(paths))
	for _, p := range paths {
		out = append(out, VolumeMount{NfsTarget: "127.0.0.1:/" + filepath.Base(p), Path: filepath.Join(root, p)})
	}

	return out
}

func TestSetupNFSMountsParentsBeforeChildren(t *testing.T) {
	t.Parallel()

	a, fake, root := newNFSTestAPI(t)
	mounts := volumes(root, "data/sub/deeper", "data/sub", "data", "other")

	require.NoError(t, a.setupNFS(t.Context(), zerolog.Nop(), new("lc-1"), mounts))

	want := []string{mounts[2].Path, mounts[3].Path, mounts[1].Path, mounts[0].Path}
	assert.Equal(t, want, fake.mounted)
}

func TestNfsMountOptions(t *testing.T) {
	t.Parallel()

	base := "fg,hard," + nfsOptions

	assert.Equal(t, base, nfsMountOptions(0), "0 keeps the kernel default")
	assert.Equal(t, base+",nconnect=1", nfsMountOptions(1))
	assert.Equal(t, base+",nconnect=4", nfsMountOptions(4))
}

func TestSetupNFSPassesEachVolumeNconnectToTheMount(t *testing.T) {
	t.Parallel()

	a, fake, root := newNFSTestAPI(t)
	mounts := volumes(root, "a", "b")
	mounts[0].Nconnect = new(4)

	require.NoError(t, a.setupNFS(t.Context(), zerolog.Nop(), new("lc-1"), mounts))

	assert.Equal(t, []int{4, 0}, fake.nconnects)
}

func TestSetupNFSRemountsOnlyForANewLifecycle(t *testing.T) {
	t.Parallel()

	a, fake, root := newNFSTestAPI(t)
	mounts := volumes(root, "a", "b")

	require.NoError(t, a.setupNFS(t.Context(), zerolog.Nop(), new("lc-1"), mounts))
	require.NoError(t, a.setupNFS(t.Context(), zerolog.Nop(), new("lc-1"), mounts))
	assert.Len(t, fake.mounted, 2, "the same lifecycle must not remount")

	require.NoError(t, a.setupNFS(t.Context(), zerolog.Nop(), new("lc-2"), mounts))
	assert.Len(t, fake.mounted, 4, "a new lifecycle remounts every volume")
}

func TestSetupNFSFailsWhenThePathIsNotANewMount(t *testing.T) {
	t.Parallel()

	a, fake, root := newNFSTestAPI(t)
	mounts := volumes(root, "a", "b", "c")
	fake.noop[mounts[1].Path] = true

	err := a.setupNFS(t.Context(), zerolog.Nop(), new("lc-1"), mounts)
	require.ErrorContains(t, err, mounts[1].Path)
	require.ErrorContains(t, err, "not on a new mount")

	assert.Equal(t, []string{mounts[0].Path, mounts[1].Path}, fake.mounted, "stops at the first failure")
	_, recorded := a.mountedPaths.Load(mounts[1].Path)
	assert.False(t, recorded)
}

func TestSetupNFSRepeatsAFailureForTheSameLifecycle(t *testing.T) {
	t.Parallel()

	a, fake, root := newNFSTestAPI(t)
	mounts := volumes(root, "a", "b")
	fake.failing[mounts[0].Path] = errors.New("access denied by server")

	first := a.setupNFS(t.Context(), zerolog.Nop(), new("lc-1"), mounts)
	require.ErrorContains(t, first, "access denied by server")

	delete(fake.failing, mounts[0].Path)
	again := a.setupNFS(t.Context(), zerolog.Nop(), new("lc-1"), mounts)
	require.ErrorIs(t, again, first)
	assert.Len(t, fake.mounted, 1, "a retry of a failed lifecycle must not mount again")

	require.NoError(t, a.setupNFS(t.Context(), zerolog.Nop(), new("lc-2"), mounts))
	assert.Len(t, fake.mounted, 3, "a new lifecycle mounts again")
}

func TestSetupNFSWithoutLifecycleDoesNotRememberFailures(t *testing.T) {
	t.Parallel()

	a, fake, root := newNFSTestAPI(t)
	mounts := volumes(root, "a")
	fake.failing[mounts[0].Path] = errors.New("timed out")

	require.Error(t, a.setupNFS(t.Context(), zerolog.Nop(), nil, mounts))

	delete(fake.failing, mounts[0].Path)
	require.NoError(t, a.setupNFS(t.Context(), zerolog.Nop(), nil, mounts))
}

func TestRunProcessGroupKillsChildrenOnTimeout(t *testing.T) {
	t.Parallel()

	pidFile := filepath.Join(t.TempDir(), "child")
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	_, err := runProcessGroup(ctx, "sh", "-c", `sleep 60 & echo $! > "$0"; wait`, pidFile)
	require.Error(t, err)

	data, err := os.ReadFile(pidFile)
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
	}, 2*time.Second, 10*time.Millisecond, "the forked child must be killed with the command")
}

func TestSetupNFSDetachesEveryStaleMountBeforeMounting(t *testing.T) {
	t.Parallel()

	api, fake, root := newNFSTestAPI(t)
	mounts := volumes(root, "a", "b", "c")
	first := "lc-1"
	require.NoError(t, api.setupNFS(t.Context(), zerolog.Nop(), &first, mounts))

	fake.ops = nil
	resumed := "lc-2"
	require.NoError(t, api.setupNFS(t.Context(), zerolog.Nop(), &resumed, mounts))

	var want []string
	for _, m := range mounts {
		want = append(want, "unmount "+m.Path)
	}
	for _, m := range mounts {
		want = append(want, "mount "+m.Path)
	}
	assert.Equal(t, want, fake.ops)
}

func TestSetupNFSMountsAParentReachedThroughASymlinkFirst(t *testing.T) {
	t.Parallel()

	api, fake, root := newNFSTestAPI(t)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "home", "user", "workspace"), 0o755))
	require.NoError(t, os.Symlink(filepath.Join(root, "home", "user", "workspace"), filepath.Join(root, "workspace")))

	mounts := volumes(root, "workspace/sub", "home/user/workspace")
	lifecycle := "lc-1"
	require.NoError(t, api.setupNFS(t.Context(), zerolog.Nop(), &lifecycle, mounts))

	assert.Equal(t, []string{mounts[1].Path, mounts[0].Path}, fake.mounted)
}

func TestSetupNFSFailsWhenAMountIsCoveredByALaterOne(t *testing.T) {
	t.Parallel()

	api, fake, root := newNFSTestAPI(t)
	mounts := volumes(root, "a", "b")
	fake.covers[mounts[1].Path] = mounts[0].Path

	lifecycle := "lc-1"
	err := api.setupNFS(t.Context(), zerolog.Nop(), &lifecycle, mounts)

	require.ErrorContains(t, err, "hidden by another volume mounted over it")
	assert.Contains(t, err.Error(), mounts[0].Path)
}

func TestSetupNFSRetriesATimedOutMountOnce(t *testing.T) {
	t.Parallel()

	api, fake, root := newNFSTestAPI(t)
	mounts := volumes(root, "a")
	fake.timeouts[mounts[0].Path] = 1

	lifecycle := "lc-1"
	require.NoError(t, api.setupNFS(t.Context(), zerolog.Nop(), &lifecycle, mounts))
	assert.Len(t, fake.mounted, 2)

	api, fake, root = newNFSTestAPI(t)
	mounts = volumes(root, "a")
	fake.timeouts[mounts[0].Path] = 2
	require.ErrorIs(t, api.setupNFS(t.Context(), zerolog.Nop(), &lifecycle, mounts), context.DeadlineExceeded)
	assert.Len(t, fake.mounted, nfsMountAttempts)
}

func TestPostInitNamesTheFailedVolume(t *testing.T) {
	t.Parallel()

	fake := newFakeNFS()
	api := newTestAPI(nil, &mockMMDSClient{})
	api.isNotFC = true
	api.nfs = fake
	api.nfsMountRetryFor = 0

	root := t.TempDir()
	mounts := volumes(root, "a", "b")
	fake.failing[mounts[1].Path] = errors.New("access denied by server")

	now := time.Now()
	lifecycle := "lc-1"
	body, err := json.Marshal(PostInitJSONBody{Timestamp: &now, LifecycleID: &lifecycle, VolumeMounts: &mounts})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	api.PostInit(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/init", bytes.NewReader(body)))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, mounts[1].Path, rec.Header().Get(failedVolumeHeader))
}

func TestSetupNFSCreatesEveryMountPointUnderADeadline(t *testing.T) {
	t.Parallel()

	api, fake, root := newNFSTestAPI(t)
	mounts := volumes(root, "data", "data/sub")

	lifecycle := "lc-1"
	require.NoError(t, api.setupNFS(t.Context(), zerolog.Nop(), &lifecycle, mounts))

	assert.Empty(t, fake.untimed)
	assert.DirExists(t, mounts[1].Path)
}

// A mount refused while the NFS proxy starts up must be retried, not reported as a
// failed /init and remembered for the lifecycle.
func TestSetupNFSRetriesAMountThatFailsAtOnce(t *testing.T) {
	t.Parallel()

	api, fake, root := newNFSTestAPI(t)
	api.nfsMountRetryFor = 5 * time.Second
	api.nfsMountRetryDelay = time.Millisecond
	mounts := volumes(root, "a")
	fake.refusals[mounts[0].Path] = 2

	lifecycle := "lc-1"
	require.NoError(t, api.setupNFS(t.Context(), zerolog.Nop(), &lifecycle, mounts))
	assert.Len(t, fake.mounted, 3)
}

// A mount that keeps failing at once stops at nfsMountRetryFor and reports the last
// attempt's error rather than a bare timeout.
func TestSetupNFSStopsRetryingAfterTheRetryWindow(t *testing.T) {
	t.Parallel()

	api, fake, root := newNFSTestAPI(t)
	api.nfsMountRetryFor = 50 * time.Millisecond
	api.nfsMountRetryDelay = 5 * time.Millisecond
	mounts := volumes(root, "a")
	fake.failing[mounts[0].Path] = errors.New("mount.nfs: Connection refused")

	lifecycle := "lc-1"
	err := api.setupNFS(t.Context(), zerolog.Nop(), &lifecycle, mounts)

	require.ErrorContains(t, err, "Connection refused")
	assert.Greater(t, len(fake.mounted), 1)
	assert.Contains(t, err.Error(), "after "+strconv.Itoa(len(fake.mounted))+" attempts")
}

// A helper that escapes the process group cannot be killed by the cancel; WaitDelay
// still stops the command from waiting on its pipes forever.
func TestRunProcessGroupBoundsTheWaitWhenAHelperEscapesTheGroup(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid not available")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := runProcessGroup(ctx, "sh", "-c", "setsid sleep 30 & wait")

	require.Error(t, err)
	assert.Less(t, time.Since(start), nfsCmdWaitDelay+5*time.Second, "the command waited on an escaped helper's pipes")
}
