//go:build linux

package nbd

import (
	"context"
	"fmt"
	"os"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// GetNBDDevice provisions a one-shot device pool, opens a direct-path mount
// against the supplied backend, and returns the resulting device path along
// with a Cleaner that tears everything down. Intended for the mount-build-rootfs
// debug utility and package tests.
func GetNBDDevice(ctx context.Context, backend block.Device, featureFlags *featureflags.Client, mountOpts ...MountOption) (DevicePath, *testutils.Cleaner, error) {
	var cleaner testutils.Cleaner

	devicePool, err := NewDevicePool(64)
	if err != nil {
		return "", &cleaner, fmt.Errorf("failed to create device pool: %w", err)
	}

	poolClosed := make(chan struct{})

	cleaner.Add(func(cleanupCtx context.Context) error {
		<-poolClosed

		err = devicePool.Close(cleanupCtx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to close device pool: %v\n", err)
		}

		return nil
	})

	poolCtx, poolCancel := context.WithCancel(ctx)

	cleaner.Add(func(context.Context) error {
		poolCancel()

		return nil
	})

	go func() {
		devicePool.Populate(poolCtx)
		close(poolClosed)
	}()

	mnt := NewDirectPathMount(backend, devicePool, featureFlags, logger.L(), mountOpts...)

	mntIndex, err := mnt.Open(ctx)
	if err != nil {
		return "", &cleaner, fmt.Errorf("failed to open nbd mount: %w", err)
	}

	cleaner.Add(func(cleanupCtx context.Context) error {
		err = mnt.Close(cleanupCtx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to close nbd mount: %v\n", err)
		}

		return nil
	})

	return GetDevicePath(mntIndex), &cleaner, nil
}
