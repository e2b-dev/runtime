//go:build linux

package filesystem

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/filesystem")

var (
	groupHeaderPattern     = regexp.MustCompile(`(?m)^[ \t]*Group[ \t]+\d+:`)
	groupFreeBlocksPattern = regexp.MustCompile(`(?m)^[ \t]+(\d+)[ \t]+free blocks?,`)
	reservedBlocksPattern  = regexp.MustCompile(`Reserved block count:\s+(\d+)`)
)

const (
	// creates an inode for every bytes-per-inode byte of space on the disk
	inodesRatio = int64(4096)
	// reservedBlocksPercentage is 0 because reserved blocks are set post-creation via tune2fs -r after the final resize.
	reservedBlocksPercentage = int64(0)
)

// e2fsck exit statuses are a bitmask (e2fsck(8), "EXIT CODE"); bits not
// named here are always failures.
const (
	e2fsckErrorsCorrected   = 1
	e2fsckRebootRecommended = 2
)

func Make(ctx context.Context, rootfsPath string, sizeMb int64, blockSize int64) error {
	ctx, tuneSpan := tracer.Start(ctx, "make-ext4")
	defer tuneSpan.End()

	if blockSize < inodesRatio {
		return errors.New("block size must be greater than inodes ratio")
	}

	// Explicit feature list for the rootfs. Defaults (resize_inode, sparse_super,
	// has_journal, metadata_csum, dir_index) are kept; we toggle only what we want
	// to add or strip below.
	features := []string{
		"^64bit",
		"^dir_nlink",
		"ext_attr",
		"extent",
		"extra_isize",
		"filetype",
		"flex_bg",
		"huge_file",
		// Pack file data <~160 B inside the inode to avoid a 4 KiB data block per tiny file.
		"inline_data",
		"large_file",
		"sparse_super2",
	}

	cmd := exec.CommandContext(ctx,
		"mkfs.ext4",
		"-O", strings.Join(features, ","),
		"-b", strconv.FormatInt(blockSize, 10),
		"-m", strconv.FormatInt(reservedBlocksPercentage, 10),
		"-i", strconv.FormatInt(inodesRatio, 10),
		rootfsPath,
		strconv.FormatInt(sizeMb, 10)+"M",
	)

	tuneStdoutWriter := telemetry.NewEventWriter(ctx, "stdout")
	cmd.Stdout = tuneStdoutWriter

	tuneStderrWriter := telemetry.NewEventWriter(ctx, "stderr")
	cmd.Stderr = tuneStderrWriter

	return cmd.Run()
}

func Mount(ctx context.Context, rootfsPath string, mountPoint string) error {
	ctx, mountSpan := tracer.Start(ctx, "mount-ext4")
	defer mountSpan.End()

	cmd := exec.CommandContext(ctx, "mount", "-o", "loop", rootfsPath, mountPoint)

	mountStdoutWriter := telemetry.NewEventWriter(ctx, "stdout")
	cmd.Stdout = mountStdoutWriter

	mountStderrWriter := telemetry.NewEventWriter(ctx, "stderr")
	cmd.Stderr = mountStderrWriter

	return cmd.Run()
}

func Unmount(ctx context.Context, rootfsPath string) error {
	ctx, unmountSpan := tracer.Start(ctx, "unmount-ext4")
	defer unmountSpan.End()

	cmd := exec.CommandContext(ctx, "umount", rootfsPath)

	unmountStdoutWriter := telemetry.NewEventWriter(ctx, "stdout")
	cmd.Stdout = unmountStdoutWriter

	unmountStderrWriter := telemetry.NewEventWriter(ctx, "stderr")
	cmd.Stderr = unmountStderrWriter

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error unmounting ext4 filesystem: %w", err)
	}

	return nil
}

func MakeWritable(ctx context.Context, rootfsPath string) error {
	ctx, tuneSpan := tracer.Start(ctx, "tune-ext4-writable")
	defer tuneSpan.End()

	cmd := exec.CommandContext(ctx, "tune2fs", "-O ^read-only", rootfsPath)

	tuneStdoutWriter := telemetry.NewEventWriter(ctx, "stdout")
	cmd.Stdout = tuneStdoutWriter

	tuneStderrWriter := telemetry.NewEventWriter(ctx, "stderr")
	cmd.Stderr = tuneStderrWriter

	return cmd.Run()
}

func Enlarge(ctx context.Context, rootfsPath string, addSize int64) (int64, error) {
	ctx, resizeSpan := tracer.Start(ctx, "enlarge-ext4")
	defer resizeSpan.End()

	stat, err := os.Stat(rootfsPath)
	if err != nil {
		return 0, fmt.Errorf("error stating rootfs file: %w", err)
	}
	finalSize := stat.Size() + addSize

	return Resize(ctx, rootfsPath, finalSize)
}

func Resize(ctx context.Context, rootfsPath string, targetSize int64) (int64, error) {
	ctx, resizeSpan := tracer.Start(ctx, "resize-ext4")
	defer resizeSpan.End()

	// Resize the ext4 filesystem
	// The underlying file must be synced to the filesystem
	cmd := exec.CommandContext(ctx, "resize2fs", rootfsPath, strconv.FormatInt(units.BytesToMB(targetSize), 10)+"M")
	resizeStdoutWriter := telemetry.NewEventWriter(ctx, "stdout")
	cmd.Stdout = resizeStdoutWriter
	resizeStderrWriter := telemetry.NewEventWriter(ctx, "stderr")
	cmd.Stderr = resizeStderrWriter
	err := cmd.Run()
	if err != nil {
		LogMetadata(ctx, rootfsPath)

		return 0, fmt.Errorf("error resizing rootfs file: %w", err)
	}

	stat, err := os.Stat(rootfsPath)
	if err != nil {
		return 0, fmt.Errorf("error stating rootfs file after resize: %w", err)
	}

	return stat.Size(), err
}

func Shrink(ctx context.Context, rootfsPath string) (int64, error) {
	ctx, resizeSpan := tracer.Start(ctx, "shrink-ext4")
	defer resizeSpan.End()

	// Shrink the ext4 filesystem
	// The underlying file must be synced to the filesystem
	cmd := exec.CommandContext(ctx, "resize2fs", "-M", rootfsPath)
	resizeStdoutWriter := telemetry.NewEventWriter(ctx, "stdout")
	cmd.Stdout = resizeStdoutWriter
	resizeStderrWriter := telemetry.NewEventWriter(ctx, "stderr")
	cmd.Stderr = resizeStderrWriter
	err := cmd.Run()
	if err != nil {
		LogMetadata(ctx, rootfsPath)

		return 0, fmt.Errorf("error shrinking rootfs file: %w", err)
	}

	stat, err := os.Stat(rootfsPath)
	if err != nil {
		return 0, fmt.Errorf("error stating rootfs file after resize: %w", err)
	}

	return stat.Size(), err
}

func GetFreeSpace(ctx context.Context, rootfsPath string, blockSize int64) (int64, error) {
	_, statSpan := tracer.Start(ctx, "stat-ext4-file")
	defer statSpan.End()

	cmd := exec.CommandContext(ctx, "debugfs", "-R", "stats", rootfsPath)
	// The parser below relies on debugfs's English field names.
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	output := out.String()
	if err != nil {
		logger.L().Error(ctx, "Error getting free space", zap.Error(err), zap.String("output", output))

		return 0, fmt.Errorf("error statting ext4: %w", err)
	}

	freeBlocks, err := parseFreeBlocks(output)
	if err != nil {
		return 0, fmt.Errorf("could not parse free blocks: %w", err)
	}

	reservedBlocks, err := parseReservedBlocks(output)
	if err != nil {
		return 0, fmt.Errorf("could not parse reserved blocks: %w", err)
	}

	freeBytes := (freeBlocks - reservedBlocks) * blockSize

	return freeBytes, nil
}

func CheckIntegrity(ctx context.Context, rootfsPath string, fix bool) (out string, err error) {
	// e2fsck reads the whole filesystem metadata (and lazily fetches the backing
	// rootfs chunks), so it usually dominates the resize-disk phase wall time.
	ctx, span := tracer.Start(ctx, "e2fsck", trace.WithAttributes(
		attribute.Bool("filesystem.e2fsck.fix", fix),
	))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	LogMetadata(ctx, rootfsPath)
	acceptedStatus := 0
	args := "-nfv"
	if fix {
		acceptedStatus = e2fsckErrorsCorrected | e2fsckRebootRecommended
		args = "-pfv"
	}
	cmd := exec.CommandContext(ctx, "e2fsck", args, rootfsPath)
	output, err := cmd.CombinedOutput()

	exitCode := -1
	if cmd.ProcessState != nil {
		exitCode = cmd.ProcessState.ExitCode()
		span.SetAttributes(attribute.Int("filesystem.e2fsck.exit_code", exitCode))
	}

	if err != nil {
		// Negative means e2fsck never ran to completion (failed to start or was
		// signaled); an unchecked filesystem must not pass as healthy.
		if exitCode < 0 {
			return string(output), fmt.Errorf("error running e2fsck: %w\n%s", err, output)
		}

		if exitCode&^acceptedStatus != 0 {
			return string(output), fmt.Errorf("error running e2fsck [exit %d]\n%s", exitCode, output)
		}
	}

	return strings.TrimSpace(string(output)), nil
}

// ReplayJournal applies committed ext4 journal transactions without a full
// filesystem check. Guest sync makes journal commits durable but may leave
// them uncheckpointed, while debugfs reads raw free-block metadata without
// replaying the journal. Replaying first reflects the latest durable state.
func ReplayJournal(ctx context.Context, rootfsPath string) (out string, err error) {
	ctx, span := tracer.Start(ctx, "replay-journal")
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	cmd := exec.CommandContext(ctx, "e2fsck", "-p", "-E", "journal_only", rootfsPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() < 0 || exitErr.ExitCode()&^(1|2) != 0 {
			return string(output), fmt.Errorf("error replaying ext4 journal: %w\n%s", err, output)
		}
	}

	return strings.TrimSpace(string(output)), nil
}

func RemoveFile(ctx context.Context, rootfsPath string, filePath string) error {
	_, statSpan := tracer.Start(ctx, "ext4-remove-file")
	defer statSpan.End()

	// -w is used to open the filesystem in writable mode
	cmd := exec.CommandContext(ctx, "debugfs", "-w", "-R", fmt.Sprintf("rm \"%s\"", filePath), rootfsPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		logger.L().Error(ctx, "error removing file", zap.Error(err), zap.String("output", string(out)))

		return fmt.Errorf("error removing file: %w", err)
	}

	return nil
}

// MountOverlayFS mounts an overlay filesystem with the specified layers at the given mount point.
// It requires kernel version 6.8 or later to use the fsconfig interface for overlayfs.
// Older mount syscall is not used because it has lowerdirs character limit (4096 characters).
func MountOverlayFS(ctx context.Context, layers []string, mountPoint string) error {
	_, mountSpan := tracer.Start(ctx, "mount-overlay-fs", trace.WithAttributes(
		attribute.String("mount", mountPoint),
		attribute.StringSlice("layers", layers),
	))
	defer mountSpan.End()

	// Open the filesystem for configuration
	fsfd, err := unix.Fsopen("overlay", unix.FSOPEN_CLOEXEC)
	if err != nil {
		return fmt.Errorf("fsopen failed: %w", err)
	}
	defer unix.Close(fsfd)

	// Set lowerdir using FSCONFIG_SET_STRING
	for _, layer := range layers {
		// https://docs.kernel.org/filesystems/overlayfs.html
		if err := unix.FsconfigSetString(fsfd, "lowerdir+", layer); err != nil {
			return fmt.Errorf("fsconfig lowerdir failed: %w", err)
		}
	}

	// Finalize configuration
	if err := unix.FsconfigCreate(fsfd); err != nil {
		return fmt.Errorf("fsconfig create failed: %w", err)
	}

	// Create the mount
	mfd, err := unix.Fsmount(fsfd, 0, 0)
	if err != nil {
		return fmt.Errorf("fsmount failed: %w", err)
	}
	defer unix.Close(mfd)

	// Mount to target
	if err := unix.MoveMount(mfd, "", -1, mountPoint, unix.MOVE_MOUNT_F_EMPTY_PATH); err != nil {
		return fmt.Errorf("move mount failed: %w", err)
	}

	return nil
}

// SetReservedBlocksOnHost sets the number of reserved filesystem blocks based on the desired reserved space in MB.
// Reserved blocks are only usable by root (uid 0).
func SetReservedBlocksOnHost(ctx context.Context, rootfsPath string, reservedSpaceMB int64, blockSize int64) error {
	if reservedSpaceMB <= 0 {
		return nil
	}

	ctx, span := tracer.Start(ctx, "set-reserved-blocks")
	defer span.End()

	blocks := units.MBToBytes(reservedSpaceMB) / blockSize

	cmd := exec.CommandContext(ctx, "tune2fs", "-r", strconv.FormatInt(blocks, 10), rootfsPath)

	stdoutWriter := telemetry.NewEventWriter(ctx, "stdout")
	cmd.Stdout = stdoutWriter

	stderrWriter := telemetry.NewEventWriter(ctx, "stderr")
	cmd.Stderr = stderrWriter

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error setting reserved blocks: %w", err)
	}

	return nil
}

func LogMetadata(ctx context.Context, rootfsPath string, extraFields ...zap.Field) {
	cmd := exec.CommandContext(ctx, "tune2fs", "-l", rootfsPath)
	output, err := cmd.CombinedOutput()

	logger.L().With(extraFields...).Debug(ctx, "tune2fs -l output", zap.String("path", rootfsPath), zap.String("output", string(output)), zap.Error(err))
}

// parseFreeBlocks returns the sum of the free-block counters stored in the
// block-group descriptors. e2fsck documents that the global count can be stale
// after an unclean unmount and normally rebuilds it from the group counts, but
// its journal_only path skips that repair:
// https://github.com/tytso/e2fsprogs/blob/v1.47.0/e2fsck/unix.c#L370-L441
// debugfs prints each group from ext2fs_bg_free_blocks_count(), so those are the
// counters we must sum after replaying only the journal:
// https://github.com/tytso/e2fsprogs/blob/v1.47.0/debugfs/debugfs.c#L486-L501
func parseFreeBlocks(debugfsOutput string) (int64, error) {
	groups := groupHeaderPattern.FindAllStringIndex(debugfsOutput, -1)
	matches := groupFreeBlocksPattern.FindAllStringSubmatch(debugfsOutput, -1)
	// Require one counter per group so truncated or unexpected debugfs output
	// cannot silently undercount free space and trigger an incorrect resize.
	if len(groups) == 0 || len(matches) != len(groups) {
		return 0, fmt.Errorf("could not parse free blocks for every block group: found %d groups and %d counters", len(groups), len(matches))
	}

	var freeBlocks int64
	for _, match := range matches {
		groupFreeBlocks, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("could not parse block-group free blocks: %w", err)
		}
		freeBlocks += groupFreeBlocks
	}

	return freeBlocks, nil
}

// parseReservedBlocks extracts the "Reserved block count:" value from debugfs output.
func parseReservedBlocks(debugfsOutput string) (int64, error) {
	matches := reservedBlocksPattern.FindStringSubmatch(debugfsOutput)
	if len(matches) < 2 {
		return 0, errors.New("could not find reserved blocks in debugfs output")
	}
	reservedBlocks, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("could not parse reserved blocks: %w", err)
	}

	return reservedBlocks, nil
}
