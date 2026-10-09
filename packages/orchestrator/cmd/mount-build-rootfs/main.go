//go:build linux

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/orchestrator/cmd/internal/cmdutil"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func main() {
	build := flag.String("build", "", "build ID (required unless -empty)")
	storagePath := flag.String("storage", ".local-build", "storage: local path or gs://bucket")
	mountPath := flag.String("mount", "", "mount path")
	verify := flag.Bool("verify", false, "verify rootfs integrity")
	logging := flag.Bool("log", false, "enable logging")
	empty := flag.Bool("empty", false, "create an empty rootfs instead of loading a build")
	size := flag.Int64("size", 1024*1024*1024, "size of the rootfs (only used with -empty)")
	blockSize := flag.Int64("block-size", 4096, "block size of the rootfs (only used with -empty)")

	flag.Parse()

	if !*empty && *build == "" {
		log.Fatal("-build required (or use -empty for empty rootfs)")
	}
	if *verify && *mountPath == "" {
		log.Fatal("-verify requires -mount")
	}

	// Resolve template storage from the -storage flag.
	templateSpec, err := cmdutil.TemplateSpec(*storagePath)
	if err != nil {
		log.Fatal(err)
	}

	// Suppress noisy output unless logging enabled
	if !*logging {
		cmdutil.SuppressNoisyLogsKeepStdLog()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt)

	// Logger is very spammy, because Populate on device pool periodically logs errors if the number of acquirable devices is less than the number of requested devices.
	if *logging {
		l, err := logger.NewDevelopmentLogger()
		if err != nil {
			panic(fmt.Errorf("failed to create logger: %w", err))
		}
		logger.ReplaceGlobals(ctx, l)
	}

	go func() {
		<-done

		cancel()
	}()

	// We use a separate ctx for majority of the operations as cancelling context for the NBD+storage and *then* doing cleanup for these often resulted in deadlocks.
	nbdContext := context.Background()

	featureFlags, err := featureflags.NewClient("", "", "")
	if err != nil {
		panic(fmt.Errorf("failed to create feature flags client: %w", err))
	}

	if *empty {
		err := runEmpty(ctx, nbdContext, featureFlags, *size, *blockSize)
		if err != nil {
			panic(fmt.Errorf("failed to create empty rootfs: %w", err))
		}
	} else {
		err := run(ctx, nbdContext, featureFlags, templateSpec, *build, *mountPath, *verify)
		if err != nil {
			panic(fmt.Errorf("failed to mount rootfs: %w", err))
		}
	}
}

func runEmpty(ctx, nbdContext context.Context, featureFlags *featureflags.Client, size int64, blockSize int64) error {
	cowCachePath := filepath.Join(os.TempDir(), fmt.Sprintf("rootfs.ext4.cow.cache-%s", uuid.New().String()))

	emptyDevice, err := testutils.NewZeroDevice(size, blockSize)
	if err != nil {
		return fmt.Errorf("failed to create zero device: %w", err)
	}

	defer os.RemoveAll(cowCachePath)

	cache, err := block.NewCache(
		size,
		blockSize,
		cowCachePath,
		false,
	)
	if err != nil {
		return fmt.Errorf("failed to create cache: %w", err)
	}

	fmt.Printf("caching writes to: %+v\n", cowCachePath)

	overlay := block.NewOverlay(emptyDevice, cache)
	defer overlay.Close()

	devicePath, deviceCleanup, err := nbd.GetNBDDevice(nbdContext, testutils.NewLoggerOverlay(overlay), featureFlags)
	defer deviceCleanup.Run(ctx, 30*time.Second)
	if err != nil {
		return fmt.Errorf("failed to get nbd device: %w", err)
	}

	fmt.Printf("rootfs exposed as device: %s\n", devicePath)

	<-ctx.Done()

	fmt.Println("closing rootfs mount")

	return nil
}

func run(ctx, nbdContext context.Context, featureFlags *featureflags.Client, templateSpec storage.Spec, buildID, mountPath string, verify bool) error {
	rootfs, rootfsCleanup, err := testutils.TemplateRootfs(ctx, templateSpec, buildID)
	defer rootfsCleanup.Run(ctx, 30*time.Second)
	if err != nil {
		return fmt.Errorf("failed to get template rootfs: %w", err)
	}

	cowCachePath := filepath.Join(os.TempDir(), fmt.Sprintf("%s-rootfs.ext4.cow.cache-%s", buildID, uuid.New().String()))

	defer os.RemoveAll(cowCachePath)

	cache, err := block.NewCache(
		int64(rootfs.Header().Metadata.Size),
		int64(rootfs.Header().Metadata.BlockSize),
		cowCachePath,
		false,
	)
	if err != nil {
		return fmt.Errorf("failed to create cache: %w", err)
	}

	fmt.Printf("caching writes to: %+v\n", cowCachePath)

	overlay := block.NewOverlay(rootfs, cache)
	defer overlay.Close()

	devicePath, deviceCleanup, err := nbd.GetNBDDevice(nbdContext, overlay, featureFlags)
	defer deviceCleanup.Run(ctx, 30*time.Second)
	if err != nil {
		return fmt.Errorf("failed to get nbd device: %w", err)
	}

	fmt.Printf("rootfs exposed as device: %s\n", devicePath)

	if mountPath != "" {
		err = os.MkdirAll(mountPath, 0o755)
		if err != nil {
			return fmt.Errorf("failed to create mount path directory: %w", err)
		}

		fmt.Fprintf(os.Stdout, "creating mount path directory: %s\n", mountPath)

		mountCleanup, err := nbd.MountNBDDevice(devicePath, mountPath)
		defer mountCleanup.Run(ctx, 30*time.Second)
		if err != nil {
			return fmt.Errorf("failed to mount device to mount path: %w", err)
		}

		// We don't remove the dir as it might have been user created.

		fmt.Printf("rootfs mounted at path: %s\n", mountPath)
	}

	if verify {
		fmt.Println("\nverifying rootfs integrity...")

		cmd := exec.CommandContext(ctx, "e2fsck", "-nfv", devicePath)

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		err := cmd.Run()
		if err != nil {
			return fmt.Errorf("failed to verify rootfs integrity: %w", err)
		}

		fmt.Println("\nrootfs integrity verified")

		journalDir := filepath.Join(mountPath, "var", "log", "journal")
		journalFiles, err := os.ReadDir(journalDir)
		if err != nil {
			return fmt.Errorf("failed to read journal directory: %w", err)
		}

		for _, journalFile := range journalFiles {
			cmd := exec.CommandContext(ctx, "journalctl", "--verify", "--directory", filepath.Join(journalDir, journalFile.Name()))
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr

			err := cmd.Run()
			if err != nil {
				return fmt.Errorf("failed to verify journal file: %w", err)
			}
		}

		fmt.Println("\njournal files verified")

		return nil
	}

	<-ctx.Done()

	fmt.Println("closing rootfs mount")

	return nil
}
