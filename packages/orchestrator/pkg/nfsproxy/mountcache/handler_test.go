package mountcache

import (
	"context"
	"net"
	"testing"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/stretchr/testify/require"
	"github.com/willscott/go-nfs"
)

type testFilesystem struct {
	billy.Filesystem
	owner Owner
}

func (f *testFilesystem) NFSCacheOwner() Owner {
	return f.owner
}

type testHandler struct {
	lifecycles []string
}

func (h *testHandler) Mount(context.Context, net.Conn, nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	lifecycleID := "lifecycle-a"
	if len(h.lifecycles) > 0 {
		lifecycleID = h.lifecycles[0]
		h.lifecycles = h.lifecycles[1:]
	}

	filesystem := &testFilesystem{
		Filesystem: memfs.New(),
		owner:      Owner{SandboxID: "sandbox-a", LifecycleID: lifecycleID},
	}

	return nfs.MountStatusOk, filesystem, nil
}

func (*testHandler) Change(context.Context, billy.Filesystem) billy.Change       { return nil }
func (*testHandler) FSStat(context.Context, billy.Filesystem, *nfs.FSStat) error { return nil }
func (*testHandler) ToHandle(context.Context, billy.Filesystem, []string) []byte {
	panic("not reached")
}
func (*testHandler) FromHandle(context.Context, []byte) (billy.Filesystem, []string, error) {
	panic("not reached")
}
func (*testHandler) InvalidateHandle(context.Context, billy.Filesystem, []byte) error {
	panic("not reached")
}
func (*testHandler) HandleLimit() int { return 1024 }

func requireStaleHandle(t *testing.T, err error) {
	t.Helper()

	var statusErr *nfs.NFSStatusError
	require.ErrorAs(t, err, &statusErr)
	require.Equal(t, nfs.NFSStatusStale, statusErr.NFSStatus)
}

func TestMountCreatesIndependentCaches(t *testing.T) {
	t.Parallel()

	handler := NewHandler(&testHandler{}, 2)
	_, filesystemA, _ := handler.Mount(t.Context(), nil, nfs.MountRequest{})
	_, filesystemB, _ := handler.Mount(t.Context(), nil, nfs.MountRequest{})

	handleA := handler.ToHandle(t.Context(), filesystemA, nil)
	handleB := handler.ToHandle(t.Context(), filesystemB, nil)
	require.Len(t, handleA, 2*uuidLength)
	require.Len(t, handleB, 2*uuidLength)
	require.NotEqual(t, handleA[:uuidLength], handleB[:uuidLength])

	handler.ToHandle(t.Context(), filesystemA, []string{"one"})
	handler.ToHandle(t.Context(), filesystemA, []string{"two"})
	_, _, err := handler.FromHandle(t.Context(), handleA)
	requireStaleHandle(t, err)

	resolved, path, err := handler.FromHandle(t.Context(), handleB)
	require.NoError(t, err)
	require.Same(t, filesystemB, resolved)
	require.Empty(t, path)
}

func TestRemoveOwnerInvalidatesAllMounts(t *testing.T) {
	t.Parallel()

	handler := NewHandler(&testHandler{}, 1024)
	_, filesystemA, _ := handler.Mount(t.Context(), nil, nfs.MountRequest{})
	_, filesystemB, _ := handler.Mount(t.Context(), nil, nfs.MountRequest{})
	handleA := handler.ToHandle(t.Context(), filesystemA, nil)
	handleB := handler.ToHandle(t.Context(), filesystemB, nil)

	handler.RemoveOwner(Owner{SandboxID: "sandbox-a", LifecycleID: "lifecycle-a"})

	_, _, err := handler.FromHandle(t.Context(), handleA)
	requireStaleHandle(t, err)
	_, _, err = handler.FromHandle(t.Context(), handleB)
	requireStaleHandle(t, err)
}

func TestRemoveOwnerOnlyInvalidatesMatchingLifecycle(t *testing.T) {
	t.Parallel()

	handler := NewHandler(&testHandler{lifecycles: []string{"lifecycle-old", "lifecycle-new"}}, 1024)
	_, oldFilesystem, _ := handler.Mount(t.Context(), nil, nfs.MountRequest{})
	_, newFilesystem, _ := handler.Mount(t.Context(), nil, nfs.MountRequest{})
	oldHandle := handler.ToHandle(t.Context(), oldFilesystem, nil)
	newHandle := handler.ToHandle(t.Context(), newFilesystem, nil)

	handler.RemoveOwner(Owner{SandboxID: "sandbox-a", LifecycleID: "lifecycle-old"})

	_, _, err := handler.FromHandle(t.Context(), oldHandle)
	requireStaleHandle(t, err)
	resolved, _, err := handler.FromHandle(t.Context(), newHandle)
	require.NoError(t, err)
	require.Same(t, newFilesystem, resolved)
}

func TestHandlerDoesNotExposeVerifierCaching(t *testing.T) {
	t.Parallel()

	handler := NewHandler(&testHandler{}, 1024)
	_, implementsCachingHandler := any(handler).(nfs.CachingHandler)
	require.False(t, implementsCachingHandler)
}
