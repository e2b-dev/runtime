//go:build linux

package rootfs

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"text/template"

	"github.com/dustin/go-humanize"
	containerregistry "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/filesystem"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/oci"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/phases"
	"github.com/e2b-dev/infra/packages/shared/pkg/dockerhub"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/rootfs")

//go:embed files
var files embed.FS
var fileTemplates = template.Must(template.ParseFS(files, "files/*"))

// enableSymlinks is the content of the baked symlink layer. Package-level so
// it feeds filesHash: a change here must rotate the fallback provision
// version like any other baked-layer change.
var enableSymlinks = map[string]string{
	// Enable envd service autostart. The target MUST be absolute: the link
	// lives in multi-user.target.wants/, so a relative target would resolve
	// inside that directory and dangle — and provision.sh's offline
	// `systemctl enable $E2B_TIMESYNC_UNIT` prunes dangling .wants symlinks,
	// silently disabling envd on distros where the link dangles.
	"etc/systemd/system/multi-user.target.wants/envd.service": "/etc/systemd/system/envd.service",
	// NOTE: chrony autostart is enabled by provision.sh via `systemctl enable
	// $E2B_TIMESYNC_UNIT`, which picks the distro-correct unit name (chrony on
	// Debian, chronyd on RHEL/Arch). A static chrony.service symlink here would
	// dangle on non-Debian images where the unit is chronyd.service.
}

// filesHash is a stable hash of the embedded rootfs file templates plus the
// baked symlink layer. It is used only as part of the fallback provision
// version; explicit provision versions remain the rollout control.
var filesHash = func() string {
	entries, _ := fs.ReadDir(files, "files")
	h := sha256.New()
	for _, e := range entries {
		data, _ := files.ReadFile("files/" + e.Name())
		fmt.Fprintf(h, "%s\x00%x\x00", e.Name(), data)
	}
	links := make([]string, 0, len(enableSymlinks))
	for name := range enableSymlinks {
		links = append(links, name)
	}
	slices.Sort(links)
	for _, name := range links {
		fmt.Fprintf(h, "%s\x00%s\x00", name, enableSymlinks[name])
	}

	return hex.EncodeToString(h.Sum(nil))
}()

// FilesHash returns a stable hash over the embedded rootfs file templates.
func FilesHash() string { return filesHash }

const (
	BusyBoxPath     = "usr/bin/busybox"
	BusyBoxInitPath = "usr/bin/init"

	// SandboxBusyBoxPath is the absolute path to busybox inside the sandbox.
	SandboxBusyBoxPath = "/" + BusyBoxPath

	ProvisioningExitPrefix = "E2B_PROVISIONING_EXIT:"
)

type Rootfs struct {
	buildContext        buildcontext.BuildContext
	dockerhubRepository dockerhub.RemoteRepository
	featureFlags        *featureflags.Client
}

func New(
	dockerhubRepository dockerhub.RemoteRepository,
	buildContext buildcontext.BuildContext,
	featureFlags *featureflags.Client,
) *Rootfs {
	return &Rootfs{
		buildContext:        buildContext,
		dockerhubRepository: dockerhubRepository,
		featureFlags:        featureFlags,
	}
}

func (r *Rootfs) CreateExt4Filesystem(
	ctx context.Context,
	l logger.Logger,
	phaseMetadata phases.PhaseMeta,
	rootfsPath string,
	provisionScript string,
	provisionLogPrefix string,
	provisionResultPath string,
) (c containerregistry.Config, e error) {
	template := r.buildContext.Config

	childCtx, childSpan := tracer.Start(ctx, "create-ext4-file")
	defer childSpan.End()

	defer func() {
		if e != nil {
			telemetry.ReportCriticalError(childCtx, "failed to create ext4 filesystem", e)
		}
	}()

	l.Debug(ctx, "Requesting Docker Image")

	img, err := oci.GetPublicImage(childCtx, r.dockerhubRepository, template.FromImage, template.RegistryAuthProvider)
	if err != nil {
		return containerregistry.Config{}, phases.NewPhaseBuildError(phaseMetadata, err)
	}

	imageSize, err := oci.GetImageSize(img)
	if err != nil {
		return containerregistry.Config{}, fmt.Errorf("error getting image size: %w", err)
	}
	l.Info(ctx, fmt.Sprintf("Base Docker image size: %s", humanize.Bytes(uint64(imageSize))))

	l.Debug(ctx, "Setting up system files")
	layers, err := additionalOCILayers(r.buildContext, provisionScript, provisionLogPrefix, provisionResultPath)
	if err != nil {
		return containerregistry.Config{}, fmt.Errorf("error populating filesystem: %w", err)
	}
	img, err = mutate.AppendLayers(img, layers...)
	if err != nil {
		return containerregistry.Config{}, fmt.Errorf("error appending layers: %w", err)
	}
	telemetry.ReportEvent(childCtx, "set up filesystem")

	l.Info(ctx, "Creating file system and pulling Docker image")
	maxRootfsSize := units.MBToBytes(int64(r.featureFlags.IntFlag(ctx, featureflags.BuildBaseRootfsSizeLimitMB)))
	ext4Size, err := oci.ToExt4(ctx, l, img, rootfsPath, maxRootfsSize, template.RootfsBlockSize())
	if err != nil {
		if imgErr, ok := errors.AsType[*oci.ImageTooLargeError](err); ok {
			return containerregistry.Config{}, phases.NewPhaseBuildError(phaseMetadata, imgErr)
		}

		return containerregistry.Config{}, fmt.Errorf("error converting oci to ext4: %w", err)
	}
	telemetry.ReportEvent(childCtx, "created rootfs ext4 file")

	l.Debug(ctx, "Filesystem cleanup")
	// Make rootfs writable, be default it's readonly
	err = filesystem.MakeWritable(ctx, rootfsPath)
	if err != nil {
		return containerregistry.Config{}, fmt.Errorf("error making rootfs file writable: %w", err)
	}

	// Resize rootfs
	rootfsFreeSpace, err := filesystem.GetFreeSpace(ctx, rootfsPath, template.RootfsBlockSize())
	if err != nil {
		return containerregistry.Config{}, fmt.Errorf("error getting free space: %w", err)
	}
	// We need to remove the remaining free space from the ext4 file size
	// This is a residual space that could not be shrunk when creating the filesystem,
	// but is still available for use
	diskAdd := units.MBToBytes(template.DiskSizeMB) - rootfsFreeSpace
	logger.L().Debug(ctx, "adding disk size diff to rootfs",
		zap.Int64("size_current", ext4Size),
		zap.Int64("size_add", diskAdd),
		zap.Int64("size_free", rootfsFreeSpace),
	)
	if diskAdd > 0 {
		_, err := filesystem.Enlarge(ctx, rootfsPath, diskAdd)
		if err != nil {
			return containerregistry.Config{}, fmt.Errorf("error enlarging rootfs: %w", err)
		}
	}

	// Check the rootfs filesystem corruption
	ext4Check, err := filesystem.CheckIntegrity(ctx, rootfsPath, true)
	logger.L().Debug(ctx, "filesystem ext4 integrity",
		zap.String("result", ext4Check),
		zap.Error(err),
	)
	if err != nil {
		return containerregistry.Config{}, fmt.Errorf("error checking ext4 filesystem integrity: %w", err)
	}

	config, err := img.ConfigFile()
	if err != nil {
		return containerregistry.Config{}, fmt.Errorf("error getting image config file: %w", err)
	}

	return config.Config, nil
}

func additionalOCILayers(
	buildContext buildcontext.BuildContext,
	provisionScript string,
	provisionLogPrefix string,
	provisionResultPath string,
) ([]containerregistry.Layer, error) {
	envdFileData, err := os.ReadFile(buildContext.BuilderConfig.HostEnvdPath)
	if err != nil {
		return nil, fmt.Errorf("error reading envd file: %w", err)
	}

	busyboxPath := filepath.Join(buildContext.BuilderConfig.HostBusyboxDir, buildContext.BuilderConfig.BusyboxVersion, runtime.GOARCH, "busybox")
	busyboxData, err := os.ReadFile(busyboxPath)
	if err != nil {
		return nil, fmt.Errorf("error reading busybox file %s: %w", busyboxPath, err)
	}

	filesMap := map[string]oci.File{
		storage.GuestEnvdPath: {Bytes: envdFileData, Mode: 0o777},

		// Systemd preset policy for envd. provision.sh removes /etc/machine-id,
		// so the template's next boot is a systemd FIRST boot — and on first
		// boot PID1 applies the distro preset policy to all units. On the
		// RHEL family that policy ends with "disable *" (and the systemd RPM
		// scriptlet's preset-all does the same during provisioning), which
		// deletes envd's autostart symlink no matter how it was created.
		// A 00- preset sorts before every distro policy file and wins.
		"etc/systemd/system-preset/00-e2b.preset": {Bytes: []byte("enable envd.service\n"), Mode: 0o644},

		// Provision script
		"usr/local/bin/provision.sh": {Bytes: []byte(provisionScript), Mode: 0o777},
		// Setup init system
		BusyBoxPath: {Bytes: busyboxData, Mode: 0o755},
		// Set to bin/init so it's not in conflict with systemd
		// Any rewrite of the init file when booted from it will corrupt the filesystem
		BusyBoxInitPath: {Bytes: busyboxData, Mode: 0o755},
	}

	// add templates
	for _, t := range fileTemplates.Templates() {
		model := newTemplateModel(buildContext, provisionLogPrefix, provisionResultPath)
		data, err := generateFile(t, model)
		if err != nil {
			return nil, fmt.Errorf("error generating file from %q: %w", t.Name(), err)
		}

		for _, path := range model.paths {
			filesMap[path.path] = oci.File{
				Bytes: data,
				Mode:  path.mode,
			}
		}
	}

	filesLayer, err := oci.LayerFile(filesMap)
	if err != nil {
		return nil, fmt.Errorf("error creating layer from files: %w", err)
	}

	symlinkLayer, err := oci.LayerSymlink(enableSymlinks)
	if err != nil {
		return nil, fmt.Errorf("error creating layer from symlinks: %w", err)
	}

	return []containerregistry.Layer{
		filesLayer,
		symlinkLayer,
	}, nil
}
