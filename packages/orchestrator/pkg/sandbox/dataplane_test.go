package sandbox

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// The in-process data plane hands back the userfaultfd handler for the
// request's socket and leaves the root disk to NBD.
func TestInProcessDataPlaneServesMemoryInProcessAndLeavesRootfsToNBD(t *testing.T) {
	t.Parallel()
	dp := NewInProcessDataPlane()
	sp, err := dp.ForSandbox(t.Context(), SandboxDataPlaneRequest{
		SandboxID:      "sbx",
		UffdSocketPath: t.TempDir() + "/uffd.sock",
		Logger:         logger.NewNopLogger(),
	})
	require.NoError(t, err)

	h, err := sp.Memory(t.Context(), nil)
	require.NoError(t, err)
	_, isUffd := h.(*uffd.Uffd)
	assert.True(t, isUffd, "the in-process handler")

	p, ok, err := sp.Rootfs(t.Context(), nil)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, p)
}

type recordingDataPlane struct {
	req  SandboxDataPlaneRequest
	prov rootfs.Provider
}

type recordingSandbox struct{ dp *recordingDataPlane }

func (d *recordingDataPlane) ForSandbox(_ context.Context, req SandboxDataPlaneRequest) (SandboxDataPlane, error) {
	d.req = req

	return recordingSandbox{dp: d}, nil
}

func (s recordingSandbox) Memory(context.Context, block.ReadonlyDevice) (uffd.Handler, error) {
	return nil, nil
}

func (s recordingSandbox) Rootfs(context.Context, block.ReadonlyDevice) (rootfs.Provider, bool, error) {
	return s.dp.prov, s.dp.prov != nil, nil
}

func (recordingSandbox) Close(context.Context) error { return nil }

// A factory serves through the data plane it was given, and falls back to
// the in-process one when given none.
func TestFactoryWithDataPlane(t *testing.T) {
	t.Parallel()
	f := &Factory{dataPlane: NewInProcessDataPlane()}
	assert.Same(t, f, f.WithDataPlane(nil))
	_, isDefault := f.dataPlane.(*InProcessDataPlane)
	assert.True(t, isDefault, "nil keeps the in-process data plane")

	rec := &recordingDataPlane{}
	f.WithDataPlane(rec)
	sp, err := f.dataPlane.ForSandbox(t.Context(), SandboxDataPlaneRequest{SandboxID: "sbx"})
	require.NoError(t, err)
	assert.Equal(t, "sbx", rec.req.SandboxID)
	_, ok, err := sp.Rootfs(t.Context(), nil)
	require.NoError(t, err)
	assert.False(t, ok, "no provider means NBD")
}
