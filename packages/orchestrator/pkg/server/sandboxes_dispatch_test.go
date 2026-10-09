//go:build linux

package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

func TestFilesystemBoot_MetadataOrRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		filesystemOnly bool
		filesystemBoot *bool
		want           bool
	}{
		{"memory snapshot, field absent: memory resume", false, nil, false},
		{"memory snapshot, request demands cold boot: reboot", false, new(true), true},
		{"fs-only snapshot, field absent: reboot", true, nil, true},
		{"fs-only snapshot, request demands cold boot: reboot", true, new(true), true},
		{"memory snapshot, explicit false: memory resume", false, new(false), false},
		{"fs-only snapshot, explicit false cannot force a memory restore", true, new(false), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			meta := metadata.Template{FilesystemOnly: tt.filesystemOnly}
			req := &orchestrator.SandboxCreateRequest{FilesystemBoot: tt.filesystemBoot}
			assert.Equal(t, tt.want, filesystemBoot(meta, req))
		})
	}
}

// An old-shaped request — serialized without the filesystem_boot field — must
// dispatch exactly as before the field existed: the snapshot's own metadata
// alone selects the boot path.
func TestFilesystemBoot_OldShapedRequestPreservesDispatch(t *testing.T) {
	t.Parallel()

	oldWire, err := proto.Marshal(&orchestrator.SandboxCreateRequest{
		Sandbox: &orchestrator.SandboxConfig{SandboxId: "sbx", Snapshot: true},
	})
	require.NoError(t, err)

	var req orchestrator.SandboxCreateRequest
	require.NoError(t, proto.Unmarshal(oldWire, &req))
	require.Nil(t, req.FilesystemBoot)

	for _, fsOnly := range []bool{false, true} {
		meta := metadata.Template{FilesystemOnly: fsOnly}
		assert.Equal(t, meta.IsFilesystemOnly(), filesystemBoot(meta, &req))
	}
}

// The resumed event reports the boot path that ran, not the one requested: a
// cold-boot request on a memory snapshot reads reboot, and a memory-restore
// request on a filesystem-only snapshot still reads reboot.
func TestResumeModeFollowsBootPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		filesystemOnly bool
		filesystemBoot *bool
		want           string
	}{
		{"memory snapshot, plain resume", false, nil, "restore"},
		{"memory snapshot, cold boot requested", false, new(true), "reboot"},
		{"filesystem-only snapshot, plain resume", true, nil, "reboot"},
		{"filesystem-only snapshot, memory restore requested", true, new(false), "reboot"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			meta := metadata.Template{FilesystemOnly: tt.filesystemOnly}
			req := &orchestrator.SandboxCreateRequest{FilesystemBoot: tt.filesystemBoot}
			eventData := map[string]any{}
			addResumeMode(eventData, filesystemBoot(meta, req))

			assert.Equal(t, tt.want, eventData["resume_mode"])
		})
	}
}
