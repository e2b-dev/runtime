package api

import (
	"context"
	"fmt"
	"strings"
)

// nfsMounter runs the mount operations; tests replace it.
type nfsMounter interface {
	Mount(ctx context.Context, nfsTarget, path string) error
	// MakeDir creates path and its parents; ctx bounds it even when NFS stops answering.
	MakeDir(ctx context.Context, path string) error
	// Unmount detaches whatever is mounted at path; nothing mounted is not an error.
	Unmount(path string) error
	// MountID returns the ID of the mount that path resolves to.
	MountID(path string) (uint64, error)
}

type execNFSMounter struct{}

func (execNFSMounter) Mount(ctx context.Context, nfsTarget, path string) error {
	args := []string{"-v", "-t", "nfs", "-o", "fg,hard," + nfsOptions, nfsTarget, path}

	if output, err := runProcessGroup(ctx, "mount", args...); err != nil {
		return fmt.Errorf("`mount %s` failed: %w: %s", strings.Join(args, " "), err, output)
	}

	return nil
}

func (execNFSMounter) MakeDir(ctx context.Context, path string) error {
	if output, err := runProcessGroup(ctx, "mkdir", "-p", path); err != nil {
		return fmt.Errorf("`mkdir -p %s` failed: %w: %s", path, err, output)
	}

	return nil
}
