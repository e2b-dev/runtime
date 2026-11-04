//go:build linux

package resumebuild

import (
	"cmp"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
)

// snapshotDevices fills each device name the flags left empty from the build's
// metadata, as the served path does, so a flag only overrides the record.
func snapshotDevices(config fc.Config, meta metadata.Template) fc.Config {
	config.RootfsDriveID = cmp.Or(config.RootfsDriveID, meta.RootfsDriveID)
	config.NetIfaceID = cmp.Or(config.NetIfaceID, meta.NetIfaceID)
	config.RootfsFile = cmp.Or(config.RootfsFile, meta.RootfsFile)

	return config
}
