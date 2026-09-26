package mountcache

import (
	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
)

type mountedFS struct {
	billy.Filesystem
	mountID uuid.UUID
	owner   Owner
}

func (f *mountedFS) Unwrap() billy.Filesystem {
	return f.Filesystem
}

func (f *mountedFS) NFSCacheMountID() uuid.UUID {
	return f.mountID
}

func (f *mountedFS) NFSCacheOwner() Owner {
	return f.owner
}

var _ billy.Filesystem = (*mountedFS)(nil)
