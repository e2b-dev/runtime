//go:build linux

package chroot

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"syscall"

	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
	"github.com/willscott/go-nfs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/mountcache"
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
	ErrLifecycleClosed  = errors.New("sandbox lifecycle mount admission is closed")
	ErrVolumeBroken     = errors.New("volume root is no longer usable")
)

// Builder allocates one chroot for a persistent volume.
type Builder interface {
	Chroot(volumeType string, teamID, volumeID uuid.UUID) (*chrooted.Chrooted, error)
}

type NFSHandler struct {
	mu sync.Mutex

	builder   Builder
	sandboxes *sandbox.Map
	gate      *mountcache.LifecycleGate

	chrootsByOwner        map[mountcache.Owner]map[uuid.UUID]*chrooted.Chrooted
	chrootMountsCounter   metric.Int64Counter
	chrootUnmountsCounter metric.Int64Counter
}

var _ nfs.Handler = (*NFSHandler)(nil)

func NewNFSHandler(
	builder Builder,
	sandboxes *sandbox.Map,
	gate *mountcache.LifecycleGate,
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
		mu:                    sync.Mutex{},
		builder:               builder,
		sandboxes:             sandboxes,
		gate:                  gate,
		chrootsByOwner:        make(map[mountcache.Owner]map[uuid.UUID]*chrooted.Chrooted),
		chrootMountsCounter:   chrootMountsCounter,
		chrootUnmountsCounter: chrootUnmountsCounter,
	}

	// don't need to keep a reference around, just create it
	_, err = meter.Int64ObservableGauge("nfs.chroots.gauge", metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
		observer.Observe(int64(h.Count()))

		return nil
	}))
	if err != nil {
		return nil, fmt.Errorf("failed to create chroots gauge: %w", err)
	}

	return h, nil
}

// Count returns the number of registered chroots represented by the gauge.
func (h *NFSHandler) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	var count int
	for _, chroots := range h.chrootsByOwner {
		count += len(chroots)
	}

	return count
}

// RemoveOwner closes all chroots registered to one exact sandbox lifecycle.
func (h *NFSHandler) RemoveOwner(ctx context.Context, owner mountcache.Owner) error {
	h.mu.Lock()
	chroots := h.chrootsByOwner[owner]
	delete(h.chrootsByOwner, owner)
	h.mu.Unlock()

	var errs []error
	for _, chroot := range chroots {
		if err := chroot.CloseAsync(); err != nil {
			logger.L().Warn(ctx, "failed to start chroot close",
				logger.WithSandboxID(owner.SandboxID),
				logger.WithLifecycleID(owner.LifecycleID),
				zap.Error(err),
			)
		}
	}
	for _, chroot := range chroots {
		if err := chroot.WaitClosed(ctx); err != nil {
			errs = append(errs, err)
			logger.L().Warn(ctx, "failed to close chroot",
				logger.WithSandboxID(owner.SandboxID),
				logger.WithLifecycleID(owner.LifecycleID),
				zap.String("path", chroot.Root()),
				zap.Error(err),
			)

			continue
		}
		h.chrootUnmountsCounter.Add(ctx, 1)
	}

	return errors.Join(errs...)
}

func (h *NFSHandler) Mount(
	ctx context.Context,
	conn net.Conn,
	request nfs.MountRequest,
) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	remoteAddr := conn.RemoteAddr().String()
	sbx, err := h.sandboxes.GetByHostPort(remoteAddr)
	if err != nil {
		h.logMountError(ctx, remoteAddr, request, fmt.Errorf("%w: %w", ErrUnknownSandbox, err))

		return nfs.MountStatusErrAcces, mountFailedFS{}, nil
	}

	owner := mountcache.Owner{SandboxID: sbx.Runtime.SandboxID, LifecycleID: sbx.LifecycleID}
	// The caller includes its lifecycle in the export path. An old TCP socket
	// may be accepted or dispatch MOUNT after its source IP has been reassigned;
	// neither Accept nor the current IP alone proves the request's lifecycle.
	volumeName, matches := strings.CutPrefix(string(request.Dirpath), sbx.NFSMountPath(""))
	if owner.LifecycleID == "" || !matches || volumeName == "" || strings.Contains(volumeName, "/") {
		h.logMountError(ctx, remoteAddr, request, ErrInvalidMountPath)

		return nfs.MountStatusErrAcces, mountFailedFS{}, nil
	}
	admission, admitted := h.gate.Begin(owner)
	if !admitted {
		h.logMountError(ctx, remoteAddr, request, ErrLifecycleClosed)

		return nfs.MountStatusErrAcces, mountFailedFS{}, nil
	}
	transferred := false
	defer func() {
		if !transferred {
			admission.Done()
		}
	}()

	current, currentErr := h.sandboxes.GetByHostPort(remoteAddr)
	if currentErr != nil || current.Runtime.SandboxID != owner.SandboxID || current.LifecycleID != owner.LifecycleID {
		admission.Close()
		h.logMountError(ctx, remoteAddr, request, ErrLifecycleClosed)

		return nfs.MountStatusErrAcces, mountFailedFS{}, nil
	}

	fs, volumeID, err := h.getChroot(ctx, sbx, owner, admission, volumeName)
	if err != nil {
		h.logMountError(ctx, remoteAddr, request, err)

		return nfs.MountStatusErrAcces, mountFailedFS{}, nil
	}

	transferred = true

	return nfs.MountStatusOk, wrapChrooted(fs, mountcache.MountKey{
		Owner:    owner,
		VolumeID: volumeID,
	}, admission), nil
}

func (h *NFSHandler) logMountError(ctx context.Context, remoteAddr string, request nfs.MountRequest, err error) {
	sourceIP, _, _ := net.SplitHostPort(remoteAddr)
	logger.L().Warn(ctx, "failed to get path",
		zap.String("request", string(request.Dirpath)),
		logger.WithSandboxIP(sourceIP),
		zap.Error(err))
}

func (h *NFSHandler) getChroot(
	ctx context.Context,
	sbx *sandbox.Sandbox,
	owner mountcache.Owner,
	admission *mountcache.MountAdmission,
	volumeName string,
) (*chrooted.Chrooted, uuid.UUID, error) {
	// find the local volume mount
	var volumeMount *sandbox.VolumeMountConfig
	for _, sbxVolumeMount := range sbx.Config.VolumeMounts {
		if sbxVolumeMount.Name == volumeName {
			volumeMount = &sbxVolumeMount

			break
		}
	}
	if volumeMount == nil {
		return nil, uuid.Nil, fmt.Errorf("failed to mount %q: %w", volumeName, ErrVolumeNotFound)
	}

	teamID, ok := pkg.TryParseUUID(sbx.Metadata.Runtime.TeamID)
	if !ok {
		return nil, uuid.Nil, ErrInvalidTeamID
	}

	if volumeMount.ID == uuid.Nil {
		return nil, uuid.Nil, ErrVolumeID
	}

	var existing *chrooted.Chrooted
	if !admission.Register(func() {
		h.mu.Lock()
		existing = h.chrootsByOwner[owner][volumeMount.ID]
		h.mu.Unlock()
	}) {
		return nil, uuid.Nil, ErrLifecycleClosed
	}
	if existing != nil {
		if err := rootUsable(existing); err != nil {
			return nil, uuid.Nil, fmt.Errorf("failed to mount %q: %w: %w", volumeName, ErrVolumeBroken, err)
		}

		return existing, volumeMount.ID, nil
	}

	fs, err := h.builder.Chroot(volumeMount.Type, teamID, volumeMount.ID)
	if err != nil {
		return nil, uuid.Nil, fmt.Errorf("failed to mount %q: %w", volumeName, err)
	}

	canonical := fs
	registered := admission.Register(func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if existing := h.chrootsByOwner[owner][volumeMount.ID]; existing != nil {
			canonical = existing

			return
		}
		if h.chrootsByOwner[owner] == nil {
			h.chrootsByOwner[owner] = make(map[uuid.UUID]*chrooted.Chrooted)
		}
		h.chrootsByOwner[owner][volumeMount.ID] = fs
	})
	if !registered {
		return nil, uuid.Nil, errors.Join(ErrLifecycleClosed, fs.Close())
	}

	if canonical != fs {
		if err := fs.Close(); err != nil {
			logger.L().Warn(ctx, "failed to close duplicate chroot", zap.String("path", fs.Root()), zap.Error(err))
		}

		if err := rootUsable(canonical); err != nil {
			return nil, uuid.Nil, fmt.Errorf("failed to mount %q: %w: %w", volumeName, ErrVolumeBroken, err)
		}

		return canonical, volumeMount.ID, nil
	}
	h.chrootMountsCounter.Add(ctx, 1)

	return fs, volumeMount.ID, nil
}

func rootUsable(root *chrooted.Chrooted) error {
	info, err := root.Stat("/")
	if err != nil {
		return err
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Nlink == 0 {
		return errors.New("volume root was removed")
	}

	return nil
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
