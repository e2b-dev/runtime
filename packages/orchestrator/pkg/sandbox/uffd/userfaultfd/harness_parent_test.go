//go:build linux

package userfaultfd

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/testutils"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/testutils/testharness"
)

type MemorySlicer struct {
	content  []byte
	pagesize int64
}

var (
	_ block.Slicer = (*MemorySlicer)(nil)
	_ PageReader   = (*MemorySlicer)(nil)
)

func NewMemorySlicer(content []byte, pagesize int64) *MemorySlicer {
	return &MemorySlicer{content: content, pagesize: pagesize}
}

func (s *MemorySlicer) Slice(_ context.Context, offset, size int64) ([]byte, error) {
	return s.content[offset : offset+size], nil
}

func (s *MemorySlicer) ReadAt(_ context.Context, p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(s.content)) {
		return 0, io.EOF
	}

	return copy(p, s.content[off:]), nil
}

func (s *MemorySlicer) Size() (int64, error) {
	return int64(len(s.content)), nil
}

func (s *MemorySlicer) Content() []byte {
	return s.content
}

func (s *MemorySlicer) BlockSize() int64 {
	return s.pagesize
}

func RandomPages(pagesize, numberOfPages uint64) *MemorySlicer {
	size := pagesize * numberOfPages
	buf := make([]byte, int(size))
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}

	return NewMemorySlicer(buf, int64(pagesize))
}

const envHelperFlag = "GO_TEST_HELPER_PROCESS"

func configureCrossProcessTest(ctx context.Context, t *testing.T, tt testConfig) (*testHandler, error) {
	t.Helper()

	data := RandomPages(tt.pagesize, tt.numberOfPages)

	if tt.sourcePatcher != nil {
		tt.sourcePatcher(data.Content())
	}

	size, err := data.Size()
	require.NoError(t, err)

	memoryArea, memoryStart, err := testutils.NewPageMmap(t, uint64(size), tt.pagesize)
	require.NoError(t, err)

	uffdFd, err := newFd(syscall.O_CLOEXEC | syscall.O_NONBLOCK)
	require.NoError(t, err)
	t.Cleanup(func() { uffdFd.close() })

	require.NoError(t, configureApi(uffdFd, tt.pagesize, tt.removeEnabled, tt.syncWP))
	require.NoError(t, register(uffdFd, memoryStart, uint64(size), UFFDIO_REGISTER_MODE_MISSING|UFFDIO_REGISTER_MODE_WP))
	t.Cleanup(func() {
		// Unregister before close (LIFO): a future test enabling
		// UFFD_FEATURE_EVENT_REMOVE would otherwise see munmap block on
		// un-acked REMOVE events against a still-registered range.
		assert.NoError(t, unregister(uffdFd, memoryStart, uint64(size)))
	})

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=TestHelperServingProcess", "-test.timeout=0")
	cmd.Env = append(os.Environ(), envHelperFlag+"=1")

	// F_DUPFD_CLOEXEC dup's atomically with CLOEXEC set; a concurrent
	// fork cannot inherit the fd before we hand it off via ExtraFiles.
	dup, err := unix.FcntlInt(uintptr(uffdFd), unix.F_DUPFD_CLOEXEC, 0)
	require.NoError(t, err)
	uffdFile := os.NewFile(uintptr(dup), "uffd")

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	parentEnd := os.NewFile(uintptr(fds[0]), "rpc-parent")
	childEnd := os.NewFile(uintptr(fds[1]), "rpc-child")

	cmd.ExtraFiles = []*os.File{uffdFile, childEnd}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	startErr := cmd.Start()
	uffdFile.Close()
	childEnd.Close()
	if startErr != nil {
		parentEnd.Close()
		require.NoError(t, startErr)
	}

	var client *testharness.Client
	t.Cleanup(func() {
		if client != nil {
			_ = client.Shutdown()
			_ = client.Close()
		} else {
			_ = cmd.Process.Kill()
		}
		_ = parentEnd.Close()

		waitErr := cmd.Wait()
		if waitErr != nil {
			if _, ok := errors.AsType[*exec.ExitError](waitErr); !ok {
				t.Logf("helper process Wait: %v", waitErr)
			}
		}
	})

	parentConn, err := net.FileConn(parentEnd)
	parentEnd.Close()
	require.NoError(t, err)

	client = testharness.NewClient(parentConn)

	h := &testHandler{
		memoryArea: &memoryArea,
		pagesize:   tt.pagesize,
		data:       data,
		client:     client,
	}

	if err := client.Bootstrap(testharness.BootstrapArgs{
		MmapStart: uint64(memoryStart),
		Pagesize:  int64(tt.pagesize),
		TotalSize: size,
		AlwaysWP:  tt.alwaysWP,
		Barriers:  tt.barriers,
		Content:   data.Content(),
	}); err != nil {
		return nil, fmt.Errorf("Lifecycle.Bootstrap: %w", err)
	}

	if err := client.WaitReady(); err != nil {
		return nil, fmt.Errorf("Lifecycle.WaitReady: %w", err)
	}

	return h, nil
}
