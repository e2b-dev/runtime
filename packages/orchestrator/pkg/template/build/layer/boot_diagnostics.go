//go:build linux

package layer

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/fstab"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	causeFstabDeviceWait = "fstab_device_wait"
	causeUnknown         = "unknown"

	waitTimeout     = "timeout"
	waitGuestExited = "guest_exited"
	waitCanceled    = "canceled"
	waitOther       = "other"
)

// BootDiagnosticsOptions returns the option that inspects the rootfs before a
// cold boot, if the BuildBootDiagnostics feature flag is on. Returns nil otherwise.
func BootDiagnosticsOptions(ctx context.Context, featureFlags *featureflags.Client) []CreateSandboxOption {
	if !featureFlags.BoolFlag(ctx, featureflags.BuildBootDiagnosticsFlag) {
		return nil
	}

	return []CreateSandboxOption{withFstabInspector(rootfs.InspectFstab)}
}

// fstabInspector reads the fstab of the rootfs at rootfsPath before boot.
type fstabInspector func(ctx context.Context, rootfsPath string) (fstab.Report, error)

func withFstabInspector(inspect fstabInspector) CreateSandboxOption {
	return func(opts *createSandboxOptions) {
		opts.inspectFstab = inspect
	}
}

// bootDiagnostics is what the pre-boot hook learned about the rootfs, kept to
// explain a boot whose envd never answered.
type bootDiagnostics struct {
	report          fstab.Report
	inspectDuration time.Duration
}

// preBootHook returns the hook to hand to the sandbox factory and, when the
// rootfs is inspected, the diagnostics the hook fills in before it returns.
// The inspection fails the hook only if its debugfs may still hold the device,
// so the boot does not start on it.
func (cs *CreateSandbox) preBootHook() (sandbox.PreBootFn, *bootDiagnostics) {
	if cs.inspectFstab == nil {
		return cs.preBootFn, nil
	}

	diag := &bootDiagnostics{}
	inspect, inner := cs.inspectFstab, cs.preBootFn

	return func(ctx context.Context, rootfsPath string) (string, error) {
		start := time.Now()
		report, err := inspect(ctx, rootfsPath)
		diag.report = report
		diag.inspectDuration = time.Since(start)
		if err != nil {
			return "", err
		}

		if inner == nil {
			return "", nil
		}

		return inner(ctx, rootfsPath)
	}, diag
}

// annotate records why the boot may have failed with err on the log and the
// span in ctx. Every value it emits is a count or an enum.
func (d *bootDiagnostics) annotate(ctx context.Context, log logger.Logger, err error, fields ...zap.Field) {
	attrs := d.attributes(err)
	for _, attr := range attrs {
		if attr.Value.Type() == attribute.INT64 {
			fields = append(fields, zap.Int64(string(attr.Key), attr.Value.AsInt64()))
		} else {
			fields = append(fields, zap.String(string(attr.Key), attr.Value.Emit()))
		}
	}

	log.Error(ctx, "build boot failed waiting for envd", fields...)
	telemetry.SetAttributes(ctx, attrs...)
	telemetry.ReportEvent(ctx, "boot-failure-diagnostics", attrs...)
}

func (d *bootDiagnostics) attributes(err error) []attribute.KeyValue {
	cause, waitResult := classify(err, d.report)
	attrs := []attribute.KeyValue{
		attribute.String("boot_failure.cause", cause),
		attribute.String("boot_failure.wait_result", waitResult),
		attribute.String("fstab.inspect", string(d.report.Outcome)),
		attribute.Int64("fstab.inspect_ms", d.inspectDuration.Milliseconds()),
	}

	switch d.report.Outcome {
	case fstab.OutcomeUnknown:
		attrs = append(attrs, attribute.String("fstab.inspect_reason", string(d.report.Reason)))
	case fstab.OutcomeOK:
		attrs = append(attrs,
			attribute.Int("fstab.required_blockers", d.report.Blockers),
			attribute.Int("fstab.swap_missing", d.report.SwapMissing),
			attribute.Int("fstab.unresolved", d.report.Unresolved),
			attribute.Int("fstab.entries", d.report.Entries),
			attribute.Int("fstab.malformed", d.report.Malformed),
		)
		for _, class := range fstab.SourceClasses {
			attrs = append(attrs, attribute.Int("fstab.blockers_"+string(class), d.report.BySource[class]))
		}
	}

	return attrs
}

// classify attributes a failed envd wait to fstab only when the guest was
// still waiting at the deadline and the fstab has a required missing device.
func classify(err error, r fstab.Report) (cause, waitResult string) {
	switch {
	case errors.Is(err, sandbox.ErrWaitForEnvdTimeout):
		waitResult = waitTimeout
	case errors.Is(err, sandbox.ErrFcProcessExited):
		waitResult = waitGuestExited
	case errors.Is(err, context.Canceled):
		waitResult = waitCanceled
	default:
		waitResult = waitOther
	}

	if waitResult == waitTimeout && r.Outcome == fstab.OutcomeOK && r.Blockers > 0 {
		return causeFstabDeviceWait, waitResult
	}

	return causeUnknown, waitResult
}
