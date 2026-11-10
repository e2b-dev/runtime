package metadata

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The rootfs file name reaches the start script's shell unquoted, so a file
// that names anything but a bare file name is refused when it is read.
func TestDeserializeRefusesARootfsFileTheShellWouldSplit(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"guest.disk", "rootfs.ext4", "fc-9b_disk"} {
		got, err := deserialize(strings.NewReader(`{"version":2,"rootfs_file":"` + name + `"}`))
		require.NoError(t, err, name)
		assert.Equal(t, name, got.RootfsFile)
	}

	for _, name := range []string{"a b", "a\tb", "a\nb", "x;id", "$(id)", "../rootfs", "dir/file", "dir\\file", ".", "..", "a'b", `a"b`, "disk\u00e9", "disk\u200b"} {
		doc, err := json.Marshal(map[string]any{"version": 2, "rootfs_file": name})
		require.NoError(t, err, name)
		_, err = deserialize(bytes.NewReader(doc))
		require.Error(t, err, name)
		assert.False(t, ValidRootfsFile(name), name)
	}
	assert.False(t, ValidRootfsFile(""), "an empty name is not a file name")

	_, err := deserialize(strings.NewReader(`{"version":2}`))
	require.NoError(t, err, "no rootfs file is the runtime's own name")
}

// The device names a foreign memory image bakes follow that image through every
// copy-constructor that keeps the memory, and BasedOn drops them: its build
// cold-boots and names its own devices.
func TestCopyConstructorsCarryDeviceNames(t *testing.T) {
	t.Parallel()

	base := Template{Version: CurrentVersion, Template: TemplateMetadata{BuildID: "build"}}.WithDeviceNames("root", "net0", "guest.disk")
	next := TemplateMetadata{BuildID: "child"}

	carried := map[string]Template{
		"SameVersionTemplate":    base.SameVersionTemplate(next),
		"NewVersionTemplate":     base.NewVersionTemplate(next),
		"WithPrefetch":           base.WithPrefetch(nil),
		"WithBalloon":            base.WithBalloon(true, false),
		"MarkFilesystemOnly":     base.MarkFilesystemOnly(true),
		"MarkFsQuiesced":         base.MarkFsQuiesced(true),
		"WithVcpuCount":          base.WithVcpuCount(2),
		"WithRunningEnvdVersion": base.WithRunningEnvdVersion("0.9.0"),
	}
	for name, got := range carried {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, "root", got.RootfsDriveID)
			assert.Equal(t, "net0", got.NetIfaceID)
			assert.Equal(t, "guest.disk", got.RootfsFile)
		})
	}

	child := base.BasedOn(FromTemplate{Alias: "a", BuildID: "build"})
	assert.Empty(t, child.RootfsDriveID, "a build from a template names its own devices")
	assert.Empty(t, child.NetIfaceID)
	assert.Empty(t, child.RootfsFile)
}
