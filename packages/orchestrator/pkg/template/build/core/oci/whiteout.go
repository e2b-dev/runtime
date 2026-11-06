//go:build linux

package oci

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// markWhiteoutParents prepares extracted layers for use as read-only overlay
// lowerdirs. Without an origin marker, overlayfs can enumerate a non-merged
// directory directly, exposing whiteouts that subsequent lookups hide. An empty
// origin makes overlayfs filter those entries without hiding surviving lower
// files, unlike marking the directory opaque.
// Linux treats an empty origin as "copied up but origin unknown" and sets
// OVL_WHITEOUTS even when the directory exists in only one lower layer:
// https://github.com/torvalds/linux/blob/v6.8/fs/overlayfs/inode.c#L1289-L1294
// A layer filesystem without xattr support is skipped, not failed.
func markWhiteoutParents(ctx context.Context, layerPath string) error {
	marked := make(map[string]struct{})

	return filepath.WalkDir(layerPath, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// WalkDir does not follow symlinks. Inspect the final extracted tree so
		// entries replaced later in the tar cannot leave stale whiteout paths.
		if entry.Type()&os.ModeCharDevice == 0 {
			return nil
		}

		var stat unix.Stat_t
		if err := unix.Lstat(path, &stat); err != nil {
			return fmt.Errorf("stat whiteout %s: %w", path, err)
		}
		if stat.Rdev != 0 || stat.Mode&unix.S_IFMT != unix.S_IFCHR {
			return nil
		}

		parent := filepath.Dir(path)
		if _, ok := marked[parent]; ok {
			return nil
		}
		// Preserve any existing origin, including a real file handle. This
		// hint is only for temporary, privately extracted read-only lowerdirs.
		err = unix.Lsetxattr(parent, "trusted.overlay.origin", nil, unix.XATTR_CREATE)
		if errors.Is(err, unix.ENOTSUP) {
			// The marker is only a hint. Without it the rootfs copy behaves as it
			// did before and still fails loudly on unfiltered whiteouts.
			logger.L().Warn(ctx, "layer filesystem does not support overlay xattrs, skipping whiteout markers", zap.String("layer_path", layerPath), zap.Error(err))

			return filepath.SkipAll
		}
		if err != nil && !errors.Is(err, unix.EEXIST) {
			return fmt.Errorf("mark whiteout parent %s: %w", parent, err)
		}
		marked[parent] = struct{}{}

		return nil
	})
}
