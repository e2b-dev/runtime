//go:build linux

package resumebuild

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
)

// A pause under -firecracker records the binary that made the snapshot, not the parent's.
func TestPausedBuildMetadataRecordsTheRunningVM(t *testing.T) {
	t.Parallel()

	parent := metadata.Template{
		Version:  metadata.CurrentVersion,
		Template: metadata.TemplateMetadata{BuildID: "parent", KernelVersion: "kernel-a", FirecrackerVersion: "v1.18.0-dev_parent"},
		Context:  metadata.Context{User: "user"},
	}
	running := fc.Config{KernelVersion: "kernel-a", FirecrackerVersion: "v1.17-100.0.1"}

	got := pausedBuildMetadata(parent, "child", running)
	assert.Equal(t, "child", got.Template.BuildID)
	assert.Equal(t, "v1.17-100.0.1", got.Template.FirecrackerVersion, "the binary the VM ran restores the snapshot")
	assert.Equal(t, "kernel-a", got.Template.KernelVersion)
	assert.Equal(t, "user", got.Context.User, "the rest of the parent's metadata carries over")
	assert.Equal(t, "v1.18.0-dev_parent", parent.Template.FirecrackerVersion, "the parent's metadata is not mutated")

	got = pausedBuildMetadata(parent, "child", fc.Config{KernelVersion: "kernel-b", FirecrackerVersion: "v1.18.0-dev_parent"})
	assert.Equal(t, "kernel-b", got.Template.KernelVersion, "the kernel slot the VM booted is recorded too")
}
