//go:build linux

package fc

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
)

func TestForColdBootClearsOnlyTheDeviceIDs(t *testing.T) {
	t.Parallel()

	foreign := Config{
		KernelVersion:      "vmlinux-test",
		FirecrackerVersion: "v-test",
		Guest:              foreignProfile{Profile: guest.Default()},
		RootfsDriveID:      "root",
		NetIfaceID:         "net0",
		RootfsFile:         "guest.disk",
	}
	boot := foreign.ForColdBoot()
	assert.Equal(t, Config{
		KernelVersion:      "vmlinux-test",
		FirecrackerVersion: "v-test",
		Guest:              foreign.Guest,
		RootfsFile:         "guest.disk",
	}, boot)
	assert.Equal(t, "rootfs", boot.RootfsDriveIDOrDefault())
	assert.Equal(t, "slot-default", boot.NetIfaceIDOrDefault("slot-default"))
	assert.Equal(t, "root", foreign.RootfsDriveID, "the receiver is untouched")

	linux := Config{}.ForColdBoot()
	assert.Equal(t, Config{}, linux, "a configuration without names is unchanged")
}
