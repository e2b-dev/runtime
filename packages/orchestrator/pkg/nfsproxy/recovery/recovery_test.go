package recovery

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	nfs "github.com/willscott/go-nfs"

	nfsproxymocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/mocks"
)

// ---- Tests: file.go ----

func TestFile_Write_PanicRecovered(t *testing.T) {
	t.Parallel()
	mf := nfsproxymocks.NewMockFile(t)
	mf.EXPECT().Write(mock.Anything).
		RunAndReturn(func(_ []byte) (int, error) { panic("File.Write") })
	f := wrapFile(t.Context(), mf)
	n, err := f.Write([]byte("abc"))
	require.ErrorIs(t, err, ErrPanic)
	require.Equal(t, 0, n)
}

func TestFile_Truncate_Happy(t *testing.T) {
	t.Parallel()
	mf := nfsproxymocks.NewMockFile(t)
	mf.EXPECT().Truncate(int64(0)).Return(nil)
	f := wrapFile(t.Context(), mf)
	require.NoError(t, f.Truncate(0))
}

func TestFile_Name_Panic_NoCrash(t *testing.T) {
	t.Parallel()
	mf := nfsproxymocks.NewMockFile(t)
	mf.EXPECT().Name().RunAndReturn(func() string { panic("File.Name") })
	f := wrapFile(t.Context(), mf)
	// Should not panic; should return zero value
	got := f.Name()
	require.Empty(t, got)
}

func TestFile_Write_Error_Propagates(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	mf := nfsproxymocks.NewMockFile(t)
	mf.EXPECT().Write(mock.Anything).Return(0, boom)
	f := wrapFile(t.Context(), mf)
	_, err := f.Write([]byte("x"))
	require.ErrorIs(t, err, boom)
}

// ---- Tests: fs.go ----

func TestFS_Stat_PanicRecovered(t *testing.T) {
	t.Parallel()
	mfs := nfsproxymocks.NewMockFilesystem(t)
	mfs.EXPECT().Stat("/x").RunAndReturn(func(string) (os.FileInfo, error) { panic("FS.Stat") })

	fs := wrapFS(t.Context(), mfs)
	_, err := fs.Stat("/x")
	require.ErrorIs(t, err, ErrPanic)
}

func TestFS_Create_Happy_WrapsFile(t *testing.T) {
	t.Parallel()
	mfs := nfsproxymocks.NewMockFilesystem(t)
	mf := nfsproxymocks.NewMockFile(t)
	mfs.EXPECT().Create("/file.txt").Return(mf, nil)
	fs := wrapFS(t.Context(), mfs)
	f, err := fs.Create("/file.txt")
	require.NoError(t, err)
	// ensure the returned file is our wrapped type
	require.IsType(t, &file{}, f)
}

func TestFS_Join_Panic_NoCrash(t *testing.T) {
	t.Parallel()
	mfs := nfsproxymocks.NewMockFilesystem(t)
	// The generated mock treats variadic args as a single []string parameter in expectation.
	mfs.EXPECT().Join([]string{"a", "b"}).
		RunAndReturn(func(_ ...string) string { panic("Join") })
	fs := wrapFS(t.Context(), mfs)
	require.NotPanics(t, func() { _ = fs.Join("a", "b") }) // should not panic
}

func TestFS_Remove_Error_Propagates(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	mfs := nfsproxymocks.NewMockFilesystem(t)
	mfs.EXPECT().Remove("/x").Return(boom)
	fs := wrapFS(t.Context(), mfs)
	err := fs.Remove("/x")
	require.ErrorIs(t, err, boom)
}

// ---- Tests: change.go ----

func TestChange_Chmod_PanicRecovered(t *testing.T) {
	t.Parallel()
	mch := nfsproxymocks.NewMockChange(t)
	mch.EXPECT().Chmod("/x", os.FileMode(0o644)).
		RunAndReturn(func(string, os.FileMode) error { panic("Change.Chmod") })
	ch := wrapChange(t.Context(), mch)
	require.ErrorIs(t, ch.Chmod("/x", 0o644), ErrPanic)
}

func TestChange_Chown_Happy(t *testing.T) {
	t.Parallel()
	mch := nfsproxymocks.NewMockChange(t)
	mch.EXPECT().Chown("/x", 1, 1).Return(nil)
	ch := wrapChange(t.Context(), mch)
	require.NoError(t, ch.Chown("/x", 1, 1))
}

func TestChange_Chtimes_Error_Propagates(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	mch := nfsproxymocks.NewMockChange(t)
	mch.EXPECT().Chtimes("/x", mock.Anything, mock.Anything).Return(boom)
	ch := wrapChange(t.Context(), mch)
	err := ch.Chtimes("/x", time.Unix(0, 0), time.Unix(0, 0))
	require.ErrorIs(t, err, boom)
}

// ---- Tests: main.go (Handler) ----

func TestHandler_FSStat_PanicRecovered(t *testing.T) {
	t.Parallel()
	mh := nfsproxymocks.NewMockHandler(t)
	mh.EXPECT().FSStat(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(context.Context, billy.Filesystem, *nfs.FSStat) error { panic("Handler.FSStat") })
	h := WrapWithRecovery(t.Context(), mh)
	var stat nfs.FSStat
	require.ErrorIs(t, h.FSStat(t.Context(), nil, &stat), ErrPanic)
}

func TestHandler_Mount_Panic_ReturnsServerFault(t *testing.T) {
	t.Parallel()
	mh := nfsproxymocks.NewMockHandler(t)
	mh.EXPECT().Mount(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(context.Context, net.Conn, nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
			panic("Mount")
		})
	h := WrapWithRecovery(t.Context(), mh)
	status, fs, auth := h.Mount(t.Context(), nil, nfs.MountRequest{})
	// Returning zero status would send a successful MOUNT with no root handle.
	require.Equal(t, nfs.MountStatusErrServerFault, status)
	require.Nil(t, fs)
	require.Nil(t, auth)
}

func TestHandler_Mount_WrapsFS(t *testing.T) {
	t.Parallel()
	base := nfsproxymocks.NewMockFilesystem(t)
	mh := nfsproxymocks.NewMockHandler(t)
	flavors := []nfs.AuthFlavor{nfs.AuthFlavor(1)}
	mh.EXPECT().Mount(mock.Anything, mock.Anything, mock.Anything).Return(nfs.MountStatusOk, base, flavors)
	h := WrapWithRecovery(t.Context(), mh)
	status, fs, auth := h.Mount(t.Context(), nil, nfs.MountRequest{})
	require.Equal(t, nfs.MountStatusOk, status)
	require.IsType(t, &filesystem{}, fs)
	require.Same(t, base, fs.(*filesystem).inner)
	require.Equal(t, flavors, auth)
}

func TestHandler_Mount_Error_Propagates(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		status nfs.MountStatus
	}{
		{name: "permission denied", status: nfs.MountStatusErrAcces},
		{name: "missing export", status: nfs.MountStatusErrNoEnt},
		{name: "server fault", status: nfs.MountStatusErrServerFault},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mh := nfsproxymocks.NewMockHandler(t)
			mh.EXPECT().Mount(mock.Anything, mock.Anything, mock.Anything).Return(tc.status, nil, nil)
			h := WrapWithRecovery(t.Context(), mh)
			status, fs, auth := h.Mount(t.Context(), nil, nfs.MountRequest{})
			require.Equal(t, tc.status, status)
			require.Nil(t, fs)
			require.Nil(t, auth)
		})
	}
}

func TestHandler_FromHandle_WrapsFSForOperationRecovery(t *testing.T) {
	t.Parallel()
	base := nfsproxymocks.NewMockFilesystem(t)
	base.EXPECT().Open("file").RunAndReturn(func(string) (billy.File, error) { panic("FS.Open") })
	mh := nfsproxymocks.NewMockHandler(t)
	mh.EXPECT().FromHandle(mock.Anything, mock.Anything).Return(base, []string{"file"}, nil)
	h := WrapWithRecovery(t.Context(), mh)
	fs, path, err := h.FromHandle(t.Context(), []byte("handle"))
	require.NoError(t, err)
	require.Equal(t, []string{"file"}, path)
	require.IsType(t, &filesystem{}, fs)
	_, err = fs.Open(path[0])
	require.ErrorIs(t, err, ErrPanic)
}

func TestHandler_FromHandle_PanicRecovered(t *testing.T) {
	t.Parallel()
	mh := nfsproxymocks.NewMockHandler(t)
	mh.EXPECT().FromHandle(mock.Anything, mock.Anything).
		RunAndReturn(func(context.Context, []byte) (billy.Filesystem, []string, error) { panic("Handler.FromHandle") })
	h := WrapWithRecovery(t.Context(), mh)
	_, _, err := h.FromHandle(t.Context(), []byte("x"))
	require.ErrorIs(t, err, ErrPanic)
}

func TestHandler_InvalidateHandle_PanicRecovered(t *testing.T) {
	t.Parallel()
	mh := nfsproxymocks.NewMockHandler(t)
	mh.EXPECT().InvalidateHandle(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(context.Context, billy.Filesystem, []byte) error { panic("Handler.InvalidateHandle") })
	h := WrapWithRecovery(t.Context(), mh)
	require.ErrorIs(t, h.InvalidateHandle(t.Context(), nil, []byte("x")), ErrPanic)
}

func TestHandler_Error_Propagation(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	mh := nfsproxymocks.NewMockHandler(t)
	// FSStat
	mh.EXPECT().FSStat(mock.Anything, mock.Anything, mock.Anything).Return(boom)
	h := WrapWithRecovery(t.Context(), mh)
	var stat nfs.FSStat
	err := h.FSStat(t.Context(), nil, &stat)
	require.ErrorIs(t, err, boom)

	// FromHandle
	mh2 := nfsproxymocks.NewMockHandler(t)
	mh2.EXPECT().FromHandle(mock.Anything, mock.Anything).Return(billy.Filesystem(nil), nil, boom)
	h2 := WrapWithRecovery(t.Context(), mh2)
	_, _, err = h2.FromHandle(t.Context(), []byte("x"))
	require.ErrorIs(t, err, boom)

	// InvalidateHandle
	mh3 := nfsproxymocks.NewMockHandler(t)
	mh3.EXPECT().InvalidateHandle(mock.Anything, mock.Anything, mock.Anything).Return(boom)
	h3 := WrapWithRecovery(t.Context(), mh3)
	err = h3.InvalidateHandle(t.Context(), nil, nil)
	require.ErrorIs(t, err, boom)
}
