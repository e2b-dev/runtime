//go:build linux

package chroot

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
	"github.com/willscott/go-nfs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

var (
	meter = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/chroot")

	ErrVolumeNotFound   = errors.New("volume not found")
	ErrInvalidTeamID    = errors.New("invalid team ID")
	ErrVolumeID         = errors.New("invalid volume ID")
	ErrInvalidMountPath = errors.New("invalid mount path")
	ErrUnknownSandbox   = errors.New("unknown sandbox")
)

type NFSHandler struct {
	mu sync.Mutex

	builder   *chrooted.Builder
	sandboxes *sandbox.Map

	chrootsByLifecycleID  map[string][]*chrooted.Chrooted
	chrootMountsCounter   metric.Int64Counter
	chrootUnmountsCounter metric.Int64Counter
}

var _ nfs.Handler = (*NFSHandler)(nil)

func NewNFSHandler(
	builder *chrooted.Builder,
	sandboxes *sandbox.Map,
) (*NFSHandler, error) {
	chrootMountsCounter, err := meter.Int64Counter("nfs.chroot.mounts")
	if err != nil {
		return nil, fmt.Errorf("failed to create chroot mounts counter: %w", err)
	}

	chrootUnmountsCounter, err := meter.Int64Counter("nfs.chroot.unmounts")
	if err != nil {
		return nil, fmt.Errorf("failed to create chroot unmounts counter: %w", err)
	}

	h := &NFSHandler{
		builder:               builder,
		sandboxes:             sandboxes,
		chrootsByLifecycleID:  make(map[string][]*chrooted.Chrooted),
		chrootMountsCounter:   chrootMountsCounter,
		chrootUnmountsCounter: chrootUnmountsCounter,
	}

	sandboxes.Subscribe(h)

	// don't need to keep a reference around, just create it
	if _, err = meter.Int64ObservableGauge("nfs.chroots.gauge", metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
		var count int

		h.mu.Lock()
		for _, chroots := range h.chrootsByLifecycleID {
			count += len(chroots)
		}
		h.mu.Unlock()

		observer.Observe(int64(count))

		return nil
	})); err != nil {
		return nil, fmt.Errorf("failed to create chroots gauge: %w", err)
	}

	return h, nil
}

func (h *NFSHandler) OnInsert(_ context.Context, _ *sandbox.Sandbox) {}

// OnStopping is called when a sandbox leaves the live registry.
func (h *NFSHandler) OnStopping(_ context.Context, _ *sandbox.Sandbox) {}

func (h *NFSHandler) OnNetworkRelease(ctx context.Context, sbx *sandbox.Sandbox) {
	lifecycleID := sbx.LifecycleID

	h.mu.Lock()
	chroots := h.chrootsByLifecycleID[lifecycleID]
	delete(h.chrootsByLifecycleID, lifecycleID)
	h.mu.Unlock()

	for _, chroot := range chroots {
		h.removeNFSOrphans(ctx, sbx.Runtime.SandboxID, chroot.Root())

		err := chroot.Close()
		if err != nil {
			logger.L().Warn(ctx, "failed to close chroot",
				logger.WithSandboxID(sbx.Runtime.SandboxID),
				logger.WithLifecycleID(lifecycleID),
				zap.String("path", chroot.Root()),
				zap.Error(err),
			)
		}
		h.chrootUnmountsCounter.Add(ctx, 1)
	}
}

// removeNFSOrphans removes .nfs* files left by the NFS silly-rename mechanism.
// When an NFS client deletes a file that still has open fds, it renames the file
// to .nfsXXXX instead of removing it, and sends a REMOVE only after the last fd
// is closed. If the client VM is killed before closing its fds, the .nfs* file
// remains on the server permanently. This function cleans them up at sandbox teardown.
func (h *NFSHandler) removeNFSOrphans(ctx context.Context, sandboxID, dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			logger.L().Warn(ctx, "failed to read volume dir for NFS orphan cleanup",
				logger.WithSandboxID(sandboxID),
				zap.String("dir", dir),
				zap.Error(err),
			)
		}

		return
	}

	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), ".nfs") {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			logger.L().Warn(ctx, "failed to remove NFS orphan file",
				logger.WithSandboxID(sandboxID),
				zap.String("path", path),
				zap.Error(err),
			)
		} else {
			logger.L().Info(ctx, "removed NFS orphan file",
				logger.WithSandboxID(sandboxID),
				zap.String("path", path),
			)
		}
	}
}

func (h *NFSHandler) Mount(
	ctx context.Context,
	conn net.Conn,
	request nfs.MountRequest,
) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	fs, err := h.getChroot(ctx, conn.RemoteAddr(), request)
	if err != nil {
		sourceIP, _, _ := net.SplitHostPort(conn.RemoteAddr().String())

		logger.L().Warn(ctx, "failed to get path",
			zap.String("request", string(request.Dirpath)),
			logger.WithSandboxIP(sourceIP),
			zap.Error(err))

		return nfs.MountStatusErrAcces, mountFailedFS{}, nil
	}

	return nfs.MountStatusOk, wrapChrooted(fs), nil
}

var mountPath = regexp.MustCompile(`^/[^/]+$`)

func (h *NFSHandler) getChroot(ctx context.Context, remoteAddr net.Addr, request nfs.MountRequest) (*chrooted.Chrooted, error) {
	sbx, err := h.sandboxes.GetByHostPort(remoteAddr.String())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnknownSandbox, err)
	}

	// normalize the mount path
	requestedPath := string(request.Dirpath)
	regexpMatch := mountPath.MatchString(requestedPath)
	if !regexpMatch {
		return nil, fmt.Errorf(`%w: expected "/volume_name", got %q`, ErrInvalidMountPath, requestedPath)
	}

	volumeName := requestedPath[1:]

	// find the local volume mount
	var volumeMount *sandbox.VolumeMountConfig
	for _, sbxVolumeMount := range sbx.Config.VolumeMounts {
		if sbxVolumeMount.Name == volumeName {
			volumeMount = &sbxVolumeMount

			break
		}
	}
	if volumeMount == nil {
		return nil, fmt.Errorf("failed to mount %q: %w", volumeName, ErrVolumeNotFound)
	}

	teamID, ok := pkg.TryParseUUID(sbx.Metadata.Runtime.TeamID)
	if !ok {
		return nil, ErrInvalidTeamID
	}

	if volumeMount.ID == uuid.Nil {
		return nil, ErrVolumeID
	}

	fs, err := h.builder.Chroot(volumeMount.Type, teamID, volumeMount.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to mount %q: %w", volumeName, err)
	}

	lifecycleID := sbx.LifecycleID
	h.mu.Lock()
	h.chrootsByLifecycleID[lifecycleID] = append(h.chrootsByLifecycleID[lifecycleID], fs)
	h.mu.Unlock()

	h.chrootMountsCounter.Add(ctx, 1)

	return fs, nil
}

func (h *NFSHandler) Change(_ context.Context, filesystem billy.Filesystem) billy.Change {
	for {
		isolated, ok := filesystem.(*wrappedFS)
		if ok {
			return wrapChange(isolated.chroot)
		}

		unwrappable, ok := filesystem.(interface{ Unwrap() billy.Filesystem })
		if !ok {
			panic(fmt.Sprintf("no idea how to find an *Chrooted from this filesystem: %T", filesystem))
		}

		filesystem = unwrappable.Unwrap()
	}
}

// FSStat describes the state of the exported file system. Things like total files, total bytes, available bytes, etc.
// We offer volumes that are unlimited in size, so we leave all values to their defaults, which is 1 << 62.
func (h *NFSHandler) FSStat(_ context.Context, _ billy.Filesystem, _ *nfs.FSStat) error {
	return nil
}

func (h *NFSHandler) ToHandle(_ context.Context, _ billy.Filesystem, _ []string) []byte {
	panic("this should be intercepted by the caching handler")
}

func (h *NFSHandler) FromHandle(_ context.Context, _ []byte) (billy.Filesystem, []string, error) {
	panic("this should be intercepted by the caching handler")
}

func (h *NFSHandler) InvalidateHandle(_ context.Context, _ billy.Filesystem, _ []byte) error {
	panic("this should be intercepted by the caching handler")
}

func (h *NFSHandler) HandleLimit() int {
	panic("this should be intercepted by the caching handler")
}
