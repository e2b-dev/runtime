//go:build linux

package fc

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/firecracker-microvm/firecracker-go-sdk"
	openapiruntime "github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/memory"
	"github.com/e2b-dev/infra/packages/shared/pkg/fc/client"
	"github.com/e2b-dev/infra/packages/shared/pkg/fc/client/operations"
	"github.com/e2b-dev/infra/packages/shared/pkg/fc/models"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const archARM64 = "arm64"

type apiClient struct {
	client *client.Firecracker
}

func newApiClient(socketPath string) *apiClient {
	client := client.NewHTTPClient(strfmt.NewFormats())

	transport := firecracker.NewUnixSocketTransport(socketPath, nil, false)
	client.SetTransport(transport)

	return &apiClient{
		client: client,
	}
}

func (c *apiClient) loadSnapshot(
	ctx context.Context,
	uffdSocketPath string,
	uffdReady chan struct{},
	snapfile template.File,
	useMemfd bool,
	useSyncWP bool,
) error {
	ctx, span := tracer.Start(ctx, "load-snapshot")
	defer span.End()

	backendType := models.MemoryBackendBackendTypeUffd
	backend := &models.MemoryBackend{
		BackendPath: &uffdSocketPath,
		BackendType: &backendType,
	}
	if useMemfd {
		backend.UseMemfd = &useMemfd
	}
	// Only sent when set: MemBackendConfig is deny_unknown_fields, so an FC
	// build without the feature rejects the request (loud failure by design).
	if useSyncWP {
		backend.UseSyncWp = &useSyncWP
	}

	snapfilePath := snapfile.Path()

	telemetry.ReportEvent(ctx, "got snapfile path")

	snapshotConfig := operations.LoadSnapshotParams{
		Context: ctx,
		Body: &models.SnapshotLoadParams{
			ResumeVM:            false,
			EnableDiffSnapshots: false,
			MemBackend:          backend,
			SnapshotPath:        &snapfilePath,
		},
	}

	_, err := c.client.Operations.LoadSnapshot(&snapshotConfig)
	if err != nil {
		return fmt.Errorf("error loading snapshot: %w", err)
	}

	telemetry.ReportEvent(ctx, "loaded snapshot")

	select {
	case <-ctx.Done():
		return fmt.Errorf("context canceled while waiting for uffd ready: %w", ctx.Err())
	case <-uffdReady:
		// Wait for the uffd to be ready to serve requests
	}

	telemetry.ReportEvent(ctx, "uffd ready")

	return nil
}

func (c *apiClient) resumeVM(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "resume-vm")
	defer span.End()

	state := models.VMStateResumed
	pauseConfig := operations.PatchVMParams{
		Context: ctx,
		Body: &models.VM{
			State: &state,
		},
	}

	_, err := c.client.Operations.PatchVM(&pauseConfig)
	if err != nil {
		return fmt.Errorf("error resuming vm: %w", err)
	}

	return nil
}

func (c *apiClient) pauseVM(ctx context.Context) error {
	state := models.VMStatePaused
	pauseConfig := operations.PatchVMParams{
		Context: ctx,
		Body: &models.VM{
			State: &state,
		},
	}

	_, err := c.client.Operations.PatchVM(&pauseConfig)
	if err != nil {
		return fmt.Errorf("error pausing vm: %w", err)
	}

	return nil
}

func (c *apiClient) createSnapshot(
	ctx context.Context,
	snapfilePath string,
) error {
	snapshotConfig := operations.CreateSnapshotParams{
		Context: ctx,
		Body: &models.SnapshotCreateParams{
			SnapshotType: models.SnapshotCreateParamsSnapshotTypeFull,
			SnapshotPath: &snapfilePath,
		},
	}

	_, err := c.client.Operations.CreateSnapshot(&snapshotConfig)
	if err != nil {
		return fmt.Errorf("error creating snapshot: %w", err)
	}

	return nil
}

func (c *apiClient) setMmds(ctx context.Context, metadata *MmdsMetadata) error {
	ctx, span := tracer.Start(ctx, "set-mmds")
	defer span.End()

	mmdsConfig := operations.PutMmdsParams{
		Context: ctx,
		Body:    metadata,
	}

	_, err := c.client.Operations.PutMmds(&mmdsConfig)
	if err != nil {
		return fmt.Errorf("error setting mmds data: %w", err)
	}

	return nil
}

func (c *apiClient) flushMetrics(ctx context.Context) error {
	action := models.InstanceActionInfoActionTypeFlushMetrics
	params := operations.CreateSyncActionParams{
		Context: ctx,
		Info: &models.InstanceActionInfo{
			ActionType: &action,
		},
	}

	_, err := c.client.Operations.CreateSyncAction(&params)
	if err != nil {
		return fmt.Errorf("error flushing fc metrics: %w", err)
	}

	return nil
}

func (c *apiClient) setMetrics(ctx context.Context, metricsPath string) error {
	params := operations.PutMetricsParams{
		Context: ctx,
		Body: &models.Metrics{
			MetricsPath: &metricsPath,
		},
	}

	_, err := c.client.Operations.PutMetrics(&params)
	if err != nil {
		return fmt.Errorf("error setting fc metrics: %w", err)
	}

	return nil
}

func (c *apiClient) setBootSource(ctx context.Context, kernelArgs string, kernelPath string) error {
	bootSourceConfig := operations.PutGuestBootSourceParams{
		Context: ctx,
		Body: &models.BootSource{
			BootArgs:        kernelArgs,
			KernelImagePath: &kernelPath,
		},
	}

	_, err := c.client.Operations.PutGuestBootSource(&bootSourceConfig)

	return err
}

func (c *apiClient) setRootfsDrive(ctx context.Context, rootfsPath string, ioEngine *string, rateLimiter *models.RateLimiter) error {
	driveID := rootfsDriveID

	isRootDevice := true
	driversConfig := operations.PutGuestDriveByIDParams{
		Context: ctx,
		DriveID: driveID,
		Body: &models.Drive{
			DriveID:      &driveID,
			PathOnHost:   rootfsPath,
			IsRootDevice: &isRootDevice,
			IsReadOnly:   false,
			IoEngine:     ioEngine,
			RateLimiter:  rateLimiter,
		},
	}

	_, err := c.client.Operations.PutGuestDriveByID(&driversConfig)
	if err != nil {
		return fmt.Errorf("error setting fc drivers config: %w", err)
	}

	return nil
}

// buildTokenBucket constructs a Firecracker TokenBucket from a TokenBucketConfig.
// Returns nil when BucketSize < 0 (disabled).
func buildTokenBucket(b TokenBucketConfig) *models.TokenBucket {
	if b.BucketSize < 0 {
		return nil
	}

	bucket := &models.TokenBucket{
		Size:       &b.BucketSize,
		RefillTime: &b.RefillTimeMs,
	}

	if b.OneTimeBurst > 0 {
		bucket.OneTimeBurst = &b.OneTimeBurst
	}

	return bucket
}

// buildRateLimiter constructs a Firecracker RateLimiter from a RateLimiterConfig.
// Either bucket is omitted when its BucketSize is < 0.
// Returns nil only when both buckets are disabled.
func buildRateLimiter(config RateLimiterConfig) *models.RateLimiter {
	ops := buildTokenBucket(config.Ops)
	bw := buildTokenBucket(config.Bandwidth)

	if ops == nil && bw == nil {
		return nil
	}

	return &models.RateLimiter{Ops: ops, Bandwidth: bw}
}

// setTxRateLimit applies or clears a Firecracker VMM-level transmit rate limit.
// Both buckets are disabled when their BucketSize < 0; if all are disabled an empty
// RateLimiter is sent to reset any limit persisted in a snapshot.
// This always sends a PATCH so snapshot-persisted limits are overwritten.
func (c *apiClient) setTxRateLimit(ctx context.Context, ifaceID string, config RateLimiterConfig) error {
	limiter := buildRateLimiter(config)
	if limiter == nil {
		limiter = &models.RateLimiter{} // empty = reset
	}

	params := operations.PatchGuestNetworkInterfaceByIDParams{
		Context: ctx,
		IfaceID: ifaceID,
		Body: &models.PartialNetworkInterface{
			IfaceID:       &ifaceID,
			TxRateLimiter: limiter,
		},
	}

	_, err := c.client.Operations.PatchGuestNetworkInterfaceByID(&params)
	if err != nil {
		return fmt.Errorf("error setting TX rate limit: %w", err)
	}

	return nil
}

// setDriveRateLimit applies or clears a Firecracker VMM-level block device rate limit.
// Both buckets are disabled when their BucketSize < 0; if all are disabled an empty
// RateLimiter is sent to reset any limit persisted in a snapshot.
// This always sends a PATCH so snapshot-persisted limits are overwritten.
func (c *apiClient) setDriveRateLimit(ctx context.Context, driveID string, config RateLimiterConfig) error {
	limiter := buildRateLimiter(config)
	if limiter == nil {
		limiter = &models.RateLimiter{} // empty = reset
	}

	params := operations.PatchGuestDriveByIDParams{
		Context: ctx,
		DriveID: driveID,
		Body: &models.PartialDrive{
			DriveID:     &driveID,
			RateLimiter: limiter,
		},
	}

	_, err := c.client.Operations.PatchGuestDriveByID(&params)
	if err != nil {
		return fmt.Errorf("error setting drive rate limit: %w", err)
	}

	return nil
}

func (c *apiClient) setNetworkInterface(ctx context.Context, ifaceID string, tapName string, tapMac string, txRateLimiter *models.RateLimiter) error {
	networkConfig := operations.PutGuestNetworkInterfaceByIDParams{
		Context: ctx,
		IfaceID: ifaceID,
		Body: &models.NetworkInterface{
			IfaceID:       &ifaceID,
			GuestMac:      tapMac,
			HostDevName:   &tapName,
			TxRateLimiter: txRateLimiter,
		},
	}

	_, err := c.client.Operations.PutGuestNetworkInterfaceByID(&networkConfig)
	if err != nil {
		return fmt.Errorf("error setting fc network config: %w", err)
	}

	mmdsVersion := "V2"
	mmdsConfig := operations.PutMmdsConfigParams{
		Context: ctx,
		Body: &models.MmdsConfig{
			Version:           &mmdsVersion,
			NetworkInterfaces: []string{ifaceID},
		},
	}

	_, err = c.client.Operations.PutMmdsConfig(&mmdsConfig)
	if err != nil {
		return fmt.Errorf("error setting network mmds data: %w", err)
	}

	return nil
}

func (c *apiClient) setMachineConfig(
	ctx context.Context,
	vCPUCount int64,
	memoryMB int64,
	hugePages bool,
) error {
	// SMT (Simultaneous Multi-Threading / Hyper-Threading) must be disabled on
	// ARM64 because ARM processors use a different core topology (big.LITTLE,
	// efficiency/performance cores) rather than hardware threads per core.
	// Firecracker validates this against the host CPU and rejects SMT=true on ARM.
	// See: https://github.com/firecracker-microvm/firecracker/blob/main/docs/cpu_templates/cpu-features.md
	// We use runtime.GOARCH (not TARGET_ARCH) because the orchestrator binary
	// always runs on the same architecture as Firecracker.
	smt := runtime.GOARCH != archARM64
	trackDirtyPages := false
	machineConfig := &models.MachineConfiguration{
		VcpuCount:       &vCPUCount,
		MemSizeMib:      &memoryMB,
		Smt:             &smt,
		TrackDirtyPages: &trackDirtyPages,
	}
	if hugePages {
		machineConfig.HugePages = models.MachineConfigurationHugePagesNr2M
	}
	machineConfigParams := operations.PutMachineConfigurationParams{
		Context: ctx,
		Body:    machineConfig,
	}
	_, err := c.client.Operations.PutMachineConfiguration(&machineConfigParams)
	if err != nil {
		return fmt.Errorf("error setting fc machine config: %w", err)
	}

	return nil
}

// https://github.com/firecracker-microvm/firecracker/blob/main/docs/entropy.md#firecracker-implementation
func (c *apiClient) setEntropyDevice(ctx context.Context) error {
	entropyConfig := operations.PutEntropyDeviceParams{
		Context: ctx,
		Body: &models.EntropyDevice{
			RateLimiter: &models.RateLimiter{
				Bandwidth: &models.TokenBucket{
					OneTimeBurst: new(entropyOneTimeBurst),
					Size:         new(entropyBytesSize),
					RefillTime:   new(entropyRefillTime),
				},
			},
		},
	}

	_, err := c.client.Operations.PutEntropyDevice(&entropyConfig)
	if err != nil {
		return fmt.Errorf("error setting fc entropy config: %w", err)
	}

	return nil
}

func (c *apiClient) startVM(ctx context.Context) error {
	start := models.InstanceActionInfoActionTypeInstanceStart
	startActionParams := operations.CreateSyncActionParams{
		Context: ctx,
		Info: &models.InstanceActionInfo{
			ActionType: &start,
		},
	}

	_, err := c.client.Operations.CreateSyncAction(&startActionParams)
	if err != nil {
		return fmt.Errorf("error starting fc: %w", err)
	}

	return nil
}

// installBalloon attaches a zero-MiB balloon device. Individual balloon
// features (free-page-reporting, free-page-hinting) are toggled via
// parameters so callers can opt in to any subset independently.
func (c *apiClient) installBalloon(ctx context.Context, freePageReporting, freePageHinting bool) error {
	ctx, span := tracer.Start(ctx, "install-balloon")
	defer span.End()

	amountMib := int64(0)
	deflateOnOom := false

	balloonConfig := operations.PutBalloonParams{
		Context: ctx,
		Body: &models.Balloon{
			AmountMib:         &amountMib,
			DeflateOnOom:      &deflateOnOom,
			FreePageReporting: freePageReporting,
			FreePageHinting:   freePageHinting,
		},
	}

	_, err := c.client.Operations.PutBalloon(&balloonConfig)
	if err != nil {
		return fmt.Errorf("error installing balloon device: %w", err)
	}

	return nil
}

func (c *apiClient) startBalloonHinting(ctx context.Context, acknowledgeOnStop bool) error {
	params := operations.StartBalloonHintingParams{
		Context: ctx,
		Body:    &models.BalloonStartCmd{AcknowledgeOnStop: acknowledgeOnStop},
	}
	_, err := c.client.Operations.StartBalloonHinting(&params)
	if err != nil {
		// FC returns 204 (no content) on success, but the FC OpenAPI spec only
		// declares 200/400 — go-swagger treats any other 2xx as "unexpected
		// success" and surfaces it as a *runtime.APIError. Honour the 2xx.
		var apiErr *openapiruntime.APIError
		if errors.As(err, &apiErr) && apiErr.IsSuccess() {
			return nil
		}

		return fmt.Errorf("error starting balloon hinting: %w", err)
	}

	return nil
}

// pauseFreePageReporting defers all balloon free-page-reporting discards
// until resumeFreePageReporting: reports stay queued in the device,
// unacknowledged, and the guest keeps the reported pages isolated. Used to
// guarantee no REMOVE-style discard fires while a CoW window captures guest
// memory. Idempotent.
func (c *apiClient) pauseFreePageReporting(ctx context.Context) error {
	params := operations.PauseBalloonReportingParams{Context: ctx}
	if _, err := c.client.Operations.PauseBalloonReporting(&params); err != nil {
		// FC returns 204 (no content) on success, but the spec declares 200 —
		// go-swagger routes the 204 into the default-response error (or a
		// *runtime.APIError when no default is declared). Honour any 2xx.
		var defErr *operations.PauseBalloonReportingDefault
		var apiErr *openapiruntime.APIError
		if (errors.As(err, &defErr) && defErr.IsSuccess()) ||
			(errors.As(err, &apiErr) && apiErr.IsSuccess()) {
			return nil
		}

		return fmt.Errorf("error pausing balloon free-page reporting: %w", err)
	}

	return nil
}

// resumeFreePageReporting re-enables free-page-reporting discards and
// processes anything deferred while paused. Idempotent.
func (c *apiClient) resumeFreePageReporting(ctx context.Context) error {
	params := operations.ResumeBalloonReportingParams{Context: ctx}
	if _, err := c.client.Operations.ResumeBalloonReporting(&params); err != nil {
		// Same 204-on-success handling as pauseFreePageReporting above.
		var defErr *operations.ResumeBalloonReportingDefault
		var apiErr *openapiruntime.APIError
		if (errors.As(err, &defErr) && defErr.IsSuccess()) ||
			(errors.As(err, &apiErr) && apiErr.IsSuccess()) {
			return nil
		}

		return fmt.Errorf("error resuming balloon free-page reporting: %w", err)
	}

	return nil
}

// freePageReportingPaused reports whether balloon free-page reporting is
// currently paused — the positive confirmation the deferred memory export
// requires before trusting its no-discard window, and the version gate for
// free: an FC build without /balloon/reporting errors here, and no
// confirmation means no deferral.
func (c *apiClient) freePageReportingPaused(ctx context.Context) (bool, error) {
	params := operations.DescribeBalloonReportingParams{Context: ctx}
	res, err := c.client.Operations.DescribeBalloonReporting(&params)
	if err != nil {
		return false, fmt.Errorf("error describing balloon free-page reporting: %w", err)
	}
	if res.Payload == nil || res.Payload.Paused == nil {
		return false, errors.New("balloon free-page reporting status carried no paused field")
	}

	return *res.Payload.Paused, nil
}

// balloonFreePageReporting reports whether THIS VM's balloon device runs
// continuous free-page reporting — the boot-time device truth, which a
// resumed sandbox cannot know from its config (the device travels with the
// snapshot). A VM without a balloon reports false.
func (c *apiClient) balloonFreePageReporting(ctx context.Context) (bool, error) {
	params := operations.DescribeBalloonConfigParams{Context: ctx}
	res, err := c.client.Operations.DescribeBalloonConfig(&params)
	if err != nil {
		// FC answers 400 when no balloon device is installed: no balloon, no
		// free-page reporting.
		if _, ok := errors.AsType[*operations.DescribeBalloonConfigBadRequest](err); ok {
			return false, nil
		}

		return false, fmt.Errorf("error describing balloon config: %w", err)
	}

	return res.Payload.FreePageReporting, nil
}

func (c *apiClient) describeBalloonHinting(ctx context.Context) (hostCmd int64, err error) {
	params := operations.DescribeBalloonHintingParams{Context: ctx}
	res, err := c.client.Operations.DescribeBalloonHinting(&params)
	if err != nil {
		return 0, err
	}
	if res.Payload.HostCmd != nil {
		hostCmd = *res.Payload.HostCmd
	}

	return hostCmd, nil
}

func (c *apiClient) memoryMapping(ctx context.Context) (*memory.Mapping, error) {
	params := operations.GetMemoryMappingsParams{
		Context: ctx,
	}

	res, err := c.client.Operations.GetMemoryMappings(&params)
	if err != nil {
		return nil, fmt.Errorf("error getting memory mappings: %w", err)
	}

	return memory.NewMappingFromFc(res.Payload.Mappings)
}

func (c *apiClient) memoryInfo(ctx context.Context, blockSize int64) (*header.DiffMetadata, error) {
	params := operations.GetMemoryParams{
		Context: ctx,
	}

	res, err := c.client.Operations.GetMemory(&params)
	if err != nil {
		return nil, fmt.Errorf("error getting memory: %w", err)
	}

	return &header.DiffMetadata{
		Dirty:     roaring.FromDense(res.Payload.Resident, false),
		Empty:     roaring.FromDense(res.Payload.Empty, false),
		BlockSize: blockSize,
	}, nil
}

func (c *apiClient) dirtyMemory(ctx context.Context, blockSize int64) (*header.DiffMetadata, error) {
	params := operations.GetDirtyMemoryParams{
		Context: ctx,
	}

	res, err := c.client.Operations.GetDirtyMemory(&params)
	if err != nil {
		return nil, fmt.Errorf("error getting dirty memory: %w", err)
	}

	return &header.DiffMetadata{
		Dirty:     roaring.FromDense(res.Payload.Bitmap, false),
		Empty:     roaring.New(),
		BlockSize: blockSize,
	}, nil
}
