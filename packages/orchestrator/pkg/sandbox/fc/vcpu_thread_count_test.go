//go:build linux

package fc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// fakeProc lays out <dir>/task/<tid>/comm the way /proc/<pid> does.
func fakeProc(t *testing.T, comms ...string) string {
	t.Helper()

	dir := t.TempDir()
	for i, comm := range comms {
		task := filepath.Join(dir, "task", string(rune('0'+i)))
		require.NoError(t, os.MkdirAll(task, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(task, "comm"), []byte(comm+"\n"), 0o644))
	}

	return dir
}

func TestVcpuThreadCount(t *testing.T) {
	t.Parallel()

	n, err := vcpuThreadCount(fakeProc(t, "firecracker", "fc_api", "fc_vcpu 0", "fc_vcpu 1", "fc_vcpu 2", "fc_vcpu 3"))
	require.NoError(t, err)
	assert.Equal(t, int64(4), n, "only the vCPU threads count")

	_, err = vcpuThreadCount(fakeProc(t, "firecracker", "fc_api"))
	require.Error(t, err, "a VM without vCPU threads is not a size")

	_, err = vcpuThreadCount(filepath.Join(t.TempDir(), "gone"))
	require.Error(t, err)
}

// A snapshot whose size is only known after load can still be smaller than the request.
func TestLimitVcpusBeforeResumeRefusesASmallerVM(t *testing.T) {
	t.Parallel()

	p := &Process{files: &storage.SandboxFiles{SandboxID: "test"}}
	var noCgroup *cgroup.CgroupHandle

	err := p.limitVcpusBeforeResume(t.Context(), noCgroup, 4, 2)
	require.ErrorIs(t, err, ErrVcpuExceedsSnapshot, "the server maps it to InvalidArgument, like the check before load")
	require.ErrorContains(t, err, "snapshot has 2 vcpus, request asks for 4")
	require.NoError(t, p.limitVcpusBeforeResume(t.Context(), noCgroup, 2, 2))
}

// A recorded size is used as is; the threads are counted only for a snapshot that never recorded one.
func TestResolveVmVcpusTrustsARecordedSize(t *testing.T) {
	t.Parallel()

	p := &Process{files: &storage.SandboxFiles{SandboxID: "test"}}

	assert.Equal(t, int64(16), p.resolveVmVcpus(t.Context(), 16, true))
	assert.Equal(t, int64(16), p.VmVcpus())
}

// Resume runs the cap step's error through this, so a smaller VM fails it only when a size was given.
func TestVcpuCapResumeErr(t *testing.T) {
	t.Parallel()

	smaller := fmt.Errorf("%w: snapshot has 16 vcpus, request asks for 20", ErrVcpuExceedsSnapshot)
	require.ErrorIs(t, vcpuCapResumeErr(smaller, true), ErrVcpuExceedsSnapshot)
	require.NoError(t, vcpuCapResumeErr(smaller, false), "without a VM size the resume runs as before")
	require.Error(t, vcpuCapResumeErr(errors.New("cgroup write failed"), false), "a failed cap always fails the resume")
	require.NoError(t, vcpuCapResumeErr(nil, true))
}
