//go:build linux

package fc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/firecracker-microvm/firecracker-go-sdk"
	openapiruntime "github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc/cputemplate"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/memory"
	"github.com/e2b-dev/infra/packages/shared/pkg/fc/client"
	"github.com/e2b-dev/infra/packages/shared/pkg/fc/client/operations"
	"github.com/e2b-dev/infra/packages/shared/pkg/fc/models"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const archARM64 = "arm64"

// MaxVcpus is the most vCPUs Firecracker accepts in a machine config.
const MaxVcpus = 32

// ValidMachineVcpus reports whether Firecracker accepts a physical VM size on this host.
// With SMT enabled on x86, vCPUs above one must come in sibling pairs.
func ValidMachineVcpus(vcpus int64) bool {
	return vcpus >= 1 && vcpus <= MaxVcpus &&
		(runtime.GOARCH == archARM64 || vcpus == 1 || vcpus%2 == 0)
}

// ValidateVcpus checks the online vCPUs and the VM size (vmVcpus, 0 = unset) against what a VM can have.
// onlineName and vmName are the caller's field or flag names, used in the error messages.
func ValidateVcpus(onlineVcpus, vmVcpus int64, onlineName, vmName string) error {
	if onlineVcpus < 1 {
		return fmt.Errorf("%s %d must be at least 1", onlineName, onlineVcpus)
	}
	if onlineVcpus > MaxVcpus {
		return fmt.Errorf("%s %d is above the %d a VM can have", onlineName, onlineVcpus, MaxVcpus)
	}
	if vmVcpus != 0 && vmVcpus < onlineVcpus {
		return fmt.Errorf("%s %d must be 1..%s %d", onlineName, onlineVcpus, vmName, vmVcpus)
	}
	if vmVcpus > MaxVcpus {
		return fmt.Errorf("%s %d is above the %d a VM can have", vmName, vmVcpus, MaxVcpus)
	}

	return nil
}

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
	clockRealtime bool,
	netOverride *models.NetworkOverride,
	cpuTemplate string,
	teamID string,
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

	body := &models.SnapshotLoadParams{
		ResumeVM:            false,
		EnableDiffSnapshots: false,
		MemBackend:          backend,
		SnapshotPath:        &snapfilePath,
	}
	// Only sent when set: SnapshotLoadParams is deny_unknown_fields, and the
	// fleet's FC does not know the field. A guest whose envd does not step its
	// clock after the restore needs KVM to advance kvmclock by the paused time.
	if clockRealtime {
		body.ClockRealtime = clockRealtime
	}
	// A snapshot baked with another host's tap name reopens the slot's tap.
	if netOverride != nil {
		body.NetworkOverrides = []*models.NetworkOverride{netOverride}
	}

	snapshotConfig := operations.LoadSnapshotParams{
		Context: ctx,
		Body:    body,
	}

	_, err := c.client.Operations.LoadSnapshot(&snapshotConfig)
	if err != nil {
		reason := classifySnapshotLoadFailure(err)
		fcSnapshotLoadFailures.Add(ctx, 1, metric.WithAttributes(
			attribute.String("reason", string(reason)),
			attribute.String("cpu_template", cpuTemplate),
			telemetry.WithTeamID(teamID),
		))
		span.SetAttributes(attribute.String("snapshot_load.failure_reason", string(reason)))

		return fmt.Errorf("error loading snapshot: %w", err)
	}

	telemetry.ReportEvent(ctx, "loaded snapshot")

	select {
	case <-ctx.Done():
		return fmt.Errorf("context canceled while waiting for uffd ready: %w", context.Cause(ctx))
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

// mmdsStoreMissing reports Firecracker's refusal of a metadata put on a microVM
// restored from a snapshot that carries no MMDS store: an external image, or the
// lineage this runtime pauses from one, since a restore never configures the store.
func mmdsStoreMissing(err error) bool {
	var bad *operations.PutMmdsBadRequest
	if !errors.As(err, &bad) || bad.Payload == nil {
		return false
	}

	return strings.Contains(strings.ToLower(bad.Payload.FaultMessage), "data store is not initialized")
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
	return telemetry.Observe0(ctx, tracer, "set-metrics-fc", func(ctx context.Context) error {
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
	})
}

func (c *apiClient) setBootSource(ctx context.Context, kernelArgs string, kernelPath string) error {
	return telemetry.Observe0(ctx, tracer, "set-boot-source-fc", func(ctx context.Context) error {
		bootSourceConfig := operations.PutGuestBootSourceParams{
			Context: ctx,
			Body: &models.BootSource{
				BootArgs:        kernelArgs,
				KernelImagePath: &kernelPath,
			},
		}

		_, err := c.client.Operations.PutGuestBootSource(&bootSourceConfig)

		return err
	})
}

// setRootfsDrive attaches the rootfs. isRootDevice makes Firecracker name the
// drive as the root device on the kernel command line; a firmware boot finds
// its boot partition on the disk itself and does not want that.
//
// blkSize is the logical block size the guest sees; 0 leaves it to
// Firecracker, which reports the backing block device's own logical block
// size (the NBD device's, 4096) or 512 for a regular file.
func (c *apiClient) setRootfsDrive(ctx context.Context, rootfsPath string, ioEngine *string, rateLimiter *models.RateLimiter, isRootDevice bool, blkSize int64) error {
	return telemetry.Observe0(ctx, tracer, "set-rootfs-drive-fc", func(ctx context.Context) error {
		driveID := rootfsDriveID

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
				BlkSize:      blkSize,
			},
		}

		_, err := c.client.Operations.PutGuestDriveByID(&driversConfig)
		if err != nil {
			return fmt.Errorf("error setting fc drivers config: %w", err)
		}

		return nil
	})
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
	return telemetry.Observe0(ctx, tracer, "set-tx-rate-limit-fc", func(ctx context.Context) error {
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
	})
}

// setDriveRateLimit applies or clears a Firecracker VMM-level block device rate limit.
// Both buckets are disabled when their BucketSize < 0; if all are disabled an empty
// RateLimiter is sent to reset any limit persisted in a snapshot.
// This always sends a PATCH so snapshot-persisted limits are overwritten.
func (c *apiClient) setDriveRateLimit(ctx context.Context, driveID string, config RateLimiterConfig) error {
	return telemetry.Observe0(ctx, tracer, "set-drive-rate-limit-fc", func(ctx context.Context) error {
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
	})
}

func (c *apiClient) setNetworkInterface(ctx context.Context, ifaceID string, tapName string, tapMac string, txRateLimiter *models.RateLimiter) error {
	return telemetry.Observe0(ctx, tracer, "set-network-interface-fc", func(ctx context.Context) error {
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
	})
}

// newMachineConfigBody returns the PUT /machine-config body: the orchestrator's
// model, or, when the guest profile adds machine fields (Boot().MachineConfigExtra),
// the model's JSON object with those fields merged in; a key the model sets is refused.
// acpiPowerOff declares the ACPI power-off device; the field is left out when
// false, so a Firecracker without it accepts the body.
func newMachineConfigBody(vCPUCount int64, memoryMB int64, hugePages bool, extra map[string]json.RawMessage, acpiPowerOff bool) (any, error) {
	model := machineConfigModel(vCPUCount, memoryMB, hugePages, acpiPowerOff)
	if len(extra) == 0 {
		return model, nil
	}

	merged, err := mergeJSONFields(model, extra)
	if err != nil {
		return nil, err
	}

	return merged, nil
}

// setMachineConfig sends a body built by newMachineConfigBody.
func (c *apiClient) setMachineConfig(ctx context.Context, body any) error {
	return telemetry.Observe0(ctx, tracer, "set-machine-config-fc", func(ctx context.Context) error {
		params := operations.PutMachineConfigurationParams{Context: ctx}
		_, err := c.client.Operations.PutMachineConfiguration(&params, withJSONBody(body))
		if err != nil {
			return fmt.Errorf("error setting fc machine config: %w", err)
		}

		return nil
	})
}

// machineConfigModel is the machine configuration body the orchestrator decides.
func machineConfigModel(vCPUCount int64, memoryMB int64, hugePages bool, acpiPowerOff bool) *models.MachineConfiguration {
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
		AcpiPoweroff:    acpiPowerOff,
	}
	if hugePages {
		machineConfig.HugePages = models.MachineConfigurationHugePagesNr2M
	}

	return machineConfig
}

// mergeJSONFields returns the JSON object of model with the extra fields
// added. A field the model already sets is refused, so a profile cannot
// override what the orchestrator decides.
func mergeJSONFields(model any, extra map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	raw, err := json.Marshal(model)
	if err != nil {
		return nil, err
	}
	fields := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	for key, value := range extra {
		if _, ok := fields[key]; ok {
			return nil, fmt.Errorf("the guest profile's field %q is already set by the orchestrator", key)
		}
		if !json.Valid(value) {
			return nil, fmt.Errorf("the guest profile's field %q is not valid JSON", key)
		}
		fields[key] = value
	}

	return fields, nil
}

// withJSONBody replaces an operation's body with body, keeping its other
// parameters at their defaults.
func withJSONBody(body any) operations.ClientOption {
	return func(op *openapiruntime.ClientOperation) {
		op.Params = openapiruntime.ClientRequestWriterFunc(func(r openapiruntime.ClientRequest, _ strfmt.Registry) error {
			if err := r.SetTimeout(0); err != nil {
				return err
			}

			return r.SetBodyParam(body)
		})
	}
}

// setACPIPowerOff declares the ACPI power-off device before a snapshot load:
// the snapshot does not record it, and a PATCH carrying only this field
// leaves the load allowed.
func (c *apiClient) setACPIPowerOff(ctx context.Context) error {
	_, err := c.client.Operations.PatchMachineConfiguration(&operations.PatchMachineConfigurationParams{
		Context: ctx,
		Body:    &models.MachineConfiguration{AcpiPoweroff: true},
	})
	if err != nil {
		return fmt.Errorf("error setting fc acpi power-off: %w", err)
	}

	return nil
}

// cpuConfigBody renders the PUT /cpu-config body. Unset collections must be omitted:
// Firecracker accepts an absent field but rejects null.
func cpuConfigBody(tmpl cputemplate.Template) *models.CPUConfig {
	body := models.CPUConfig(tmpl)

	return &body
}

// setCPUConfig installs a custom CPU template. Firecracker accepts it only before boot,
// and in either order with PUT /machine-config as long as that omits cpu_template.
func (c *apiClient) setCPUConfig(ctx context.Context, tmpl cputemplate.Template) error {
	return telemetry.Observe0(ctx, tracer, "set-cpu-config-fc", func(ctx context.Context) error {
		cpuConfigParams := operations.PutCPUConfigurationParams{
			Context: ctx,
			Body:    cpuConfigBody(tmpl),
		}

		_, err := c.client.Operations.PutCPUConfiguration(&cpuConfigParams)
		if err != nil {
			return fmt.Errorf("error setting fc cpu config (template %s): %w", tmpl.Digest(), err)
		}

		return nil
	})
}

// https://github.com/firecracker-microvm/firecracker/blob/main/docs/entropy.md#firecracker-implementation
func (c *apiClient) setEntropyDevice(ctx context.Context) error {
	return telemetry.Observe0(ctx, tracer, "set-entropy-device-fc", func(ctx context.Context) error {
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
	})
}

func (c *apiClient) startVM(ctx context.Context) error {
	return telemetry.Observe0(ctx, tracer, "start-vm-fc", func(ctx context.Context) error {
		start := models.InstanceActionInfoActionTypeInstanceStart
		startActionParams := operations.CreateSyncActionParams{
			Context: ctx,
			Info: &models.InstanceActionInfo{
				ActionType: &start,
			},
		}

		began := time.Now()
		_, err := c.client.Operations.CreateSyncAction(&startActionParams)
		fcInstanceStartDuration.Record(ctx, time.Since(began).Milliseconds(),
			metric.WithAttributes(attribute.Bool("success", err == nil)))
		if err != nil {
			return fmt.Errorf("error starting fc: %w", err)
		}

		return nil
	})
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

// swaggerSuccess reports whether err is go-swagger's rendering of a 2xx the
// spec did not declare (FC answers 204 where the spec says 200/400): both the
// generic *openapiruntime.APIError and the typed default responses expose
// IsSuccess.
func swaggerSuccess(err error) bool {
	var ok interface{ IsSuccess() bool }

	return errors.As(err, &ok) && ok.IsSuccess()
}

func (c *apiClient) startBalloonHinting(ctx context.Context, acknowledgeOnStop bool) error {
	params := operations.StartBalloonHintingParams{
		Context: ctx,
		Body:    &models.BalloonStartCmd{AcknowledgeOnStop: acknowledgeOnStop},
	}
	_, err := c.client.Operations.StartBalloonHinting(&params)
	if err != nil {
		if swaggerSuccess(err) {
			return nil
		}

		return fmt.Errorf("error starting balloon hinting: %w", err)
	}

	return nil
}

// errHintingStopRefused is FC's 400 on stop: hinting not enabled on the
// balloon, or the device not yet active. Either way no cycle is running.
var errHintingStopRefused = errors.New("balloon hinting stop refused")

func (c *apiClient) stopBalloonHinting(ctx context.Context) error {
	params := operations.StopBalloonHintingParams{Context: ctx}
	if _, err := c.client.Operations.StopBalloonHinting(&params); err != nil {
		if swaggerSuccess(err) {
			return nil
		}
		if _, ok := errors.AsType[*operations.StopBalloonHintingBadRequest](err); ok {
			return fmt.Errorf("%w: %w", errHintingStopRefused, err)
		}

		return fmt.Errorf("error stopping balloon hinting: %w", err)
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
		if swaggerSuccess(err) {
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
		if swaggerSuccess(err) {
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
	cfg, err := c.describeBalloonConfig(ctx)
	if err != nil || cfg == nil {
		return false, err
	}

	return cfg.FreePageReporting, nil
}

// describeBalloonConfig returns the balloon device config, or nil when no
// balloon is installed (FC answers 400).
func (c *apiClient) describeBalloonConfig(ctx context.Context) (*models.Balloon, error) {
	params := operations.DescribeBalloonConfigParams{Context: ctx}
	res, err := c.client.Operations.DescribeBalloonConfig(&params)
	if err != nil {
		if _, ok := errors.AsType[*operations.DescribeBalloonConfigBadRequest](err); ok {
			return nil, nil
		}

		return nil, fmt.Errorf("error describing balloon config: %w", err)
	}

	return res.Payload, nil
}

// hintingStatus is FC's view of the hinting handshake: the last command the
// host issued and the last one the guest wrote back. An absent guest_cmd
// decodes as freePageHintStop (the guest has nothing in flight either way).
type hintingStatus struct {
	hostCmd  int64
	guestCmd int64
}

func (c *apiClient) describeBalloonHinting(ctx context.Context) (hintingStatus, error) {
	params := operations.DescribeBalloonHintingParams{Context: ctx}
	res, err := c.client.Operations.DescribeBalloonHinting(&params)
	if err != nil {
		return hintingStatus{}, err
	}
	st := hintingStatus{guestCmd: res.Payload.GuestCmd}
	if res.Payload.HostCmd != nil {
		st.hostCmd = *res.Payload.HostCmd
	}

	return st, nil
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
