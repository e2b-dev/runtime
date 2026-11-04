//go:build linux

package resumebuild

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
)

// A restore addresses the devices the build's metadata records unless a flag
// names others; a build that records none keeps the runtime's own names.
func TestSnapshotDevicesFallBackToTheMetadata(t *testing.T) {
	t.Parallel()

	recorded := metadata.Template{}.WithDeviceNames("root", "net0", "disk.img")

	got := snapshotDevices(fc.Config{}, recorded)
	assert.Equal(t, "root", got.RootfsDriveIDOrDefault())
	assert.Equal(t, "net0", got.NetIfaceIDOrDefault("eth0"))
	assert.Equal(t, "disk.img", got.RootfsFileName())

	got = snapshotDevices(fc.Config{RootfsDriveID: "disk", NetIfaceID: "eth1", RootfsFile: "other.img"}, recorded)
	assert.Equal(t, "disk", got.RootfsDriveIDOrDefault(), "a flag overrides the record")
	assert.Equal(t, "eth1", got.NetIfaceIDOrDefault("eth0"))
	assert.Equal(t, "other.img", got.RootfsFileName())

	got = snapshotDevices(fc.Config{}, metadata.Template{})
	assert.Equal(t, "rootfs", got.RootfsDriveIDOrDefault())
	assert.Equal(t, "eth0", got.NetIfaceIDOrDefault("eth0"))
	assert.Equal(t, "rootfs.ext4", got.RootfsFileName())
}
