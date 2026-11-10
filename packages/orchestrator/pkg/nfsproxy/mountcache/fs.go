//go:build linux

package mountcache

import (
	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
)

// Owner identifies one sandbox lifecycle. A replacement lifecycle for the same
// sandbox must never share cleanup ownership with the lifecycle it replaces.
type Owner struct {
	SandboxID   string
	LifecycleID string
}

func (o Owner) valid() bool {
	return o.SandboxID != "" && o.LifecycleID != ""
}

// MountKey identifies one logical volume mount for the lifetime of a sandbox.
// Repeated NFS MOUNT requests for the same key reuse one handle-cache shard.
type MountKey struct {
	Owner    Owner
	VolumeID uuid.UUID
}

func (k MountKey) valid() bool {
	return k.Owner.valid() && k.VolumeID != uuid.Nil
}

type mountedFS struct {
	billy.Filesystem

	mountID uuid.UUID
	key     MountKey
}

func (f *mountedFS) Unwrap() billy.Filesystem {
	return f.Filesystem
}

func (f *mountedFS) NFSCacheMountID() uuid.UUID {
	return f.mountID
}

func (f *mountedFS) NFSCacheMountKey() MountKey {
	return f.key
}

var _ billy.Filesystem = (*mountedFS)(nil)
