//go:build linux

package optimize

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
)

type namedGuest struct{ guest.Profile }

func (namedGuest) Name() string { return "named" }

// The prefetch resume runs the final snapshot as the build's guest, with the
// device names its memory image bakes, as every other layer of the build does.
func TestOptimizeSandboxConfigCarriesTheBuildsMachine(t *testing.T) {
	t.Parallel()

	pb := &OptimizeBuilder{BuildContext: buildcontext.BuildContext{Config: config.TemplateConfig{
		KernelVersion:      "kernel",
		FirecrackerVersion: "fc",
		Guest:              namedGuest{guest.Default()},
		RootfsDriveID:      "root",
		NetIfaceID:         "net0",
		RootfsFile:         "disk.img",
	}}}

	got := pb.sandboxConfig().FirecrackerConfig
	assert.Equal(t, pb.Config.FirecrackerConfig(), got)
	assert.Equal(t, "named", got.GuestProfile().Name())
	assert.Equal(t, "root", got.RootfsDriveID)
}
