//go:build linux

package layer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/fstab"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func TestBootDiagnosticsOptions(t *testing.T) {
	t.Parallel()

	source := ldtestdata.DataSource()
	ff, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, ff.Close(context.WithoutCancel(t.Context()))) })

	assert.Nil(t, BootDiagnosticsOptions(t.Context(), ff))

	source.Update(source.Flag(featureflags.BuildBootDiagnosticsFlag.Key()).BooleanFlag().VariationForAll(true))
	opts := BootDiagnosticsOptions(t.Context(), ff)
	require.Len(t, opts, 1)
	assert.NotNil(t, NewCreateSandbox(nil, nil, 0, opts...).inspectFstab)
}

func TestPreBootHookOffIsPassThrough(t *testing.T) {
	t.Parallel()

	inner := func(context.Context, string) (string, error) { return "", nil }

	fn, diag := NewCreateSandbox(nil, nil, 0, WithPreBootFn(inner)).preBootHook()
	assert.Equal(t, reflect.ValueOf(inner).Pointer(), reflect.ValueOf(fn).Pointer())
	assert.Nil(t, diag)

	fn, diag = NewCreateSandbox(nil, nil, 0).preBootHook()
	assert.Nil(t, fn)
	assert.Nil(t, diag)
}

func TestPreBootHookOnRunsInner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		hasInner bool
		innerErr error
	}{
		{name: "inner hook succeeds", hasInner: true},
		{name: "inner hook error is returned verbatim", hasInner: true, innerErr: errors.New("reserved blocks failed")},
		{name: "no inner hook"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			calls := 0
			var inner sandbox.PreBootFn
			if tt.hasInner {
				inner = func(context.Context, string) (string, error) {
					calls++

					return "", tt.innerErr
				}
			}
			fn, diag := NewCreateSandbox(nil, nil, 0, WithPreBootFn(inner), withFstabInspector(rootfs.InspectFstab)).preBootHook()
			require.NotNil(t, fn)
			require.NotNil(t, diag)

			_, err := fn(t.Context(), "/nonexistent/rootfs.ext4")

			if tt.innerErr == nil {
				require.NoError(t, err)
			} else {
				assert.Same(t, tt.innerErr, err)
			}
			if tt.hasInner {
				assert.Equal(t, 1, calls)
			}
			assert.Equal(t, fstab.OutcomeUnknown, diag.report.Outcome)
		})
	}
}

func TestPreBootHookFailsWhileInspectorHoldsDevice(t *testing.T) {
	t.Parallel()

	held := fstab.Report{Outcome: fstab.OutcomeUnknown, Reason: fstab.ReasonTimeout}
	inspect := func(context.Context, string) (fstab.Report, error) {
		return held, rootfs.ErrInspectorRunning
	}
	innerCalled := false
	inner := func(context.Context, string) (string, error) {
		innerCalled = true

		return "", nil
	}

	fn, diag := NewCreateSandbox(nil, nil, 0, WithPreBootFn(inner), withFstabInspector(inspect)).preBootHook()
	_, err := fn(t.Context(), "/dev/nbd4095")

	require.ErrorIs(t, err, rootfs.ErrInspectorRunning)
	assert.False(t, innerCalled, "nothing else may touch the device")
	assert.Equal(t, held, diag.report)
}

// waitErr wraps cause the way the envd init loop reports a canceled wait.
func waitErr(cause error) error {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)

	return fmt.Errorf("failed to init new envd: %w", fmt.Errorf("%w with cause: %w", ctx.Err(), context.Cause(ctx)))
}

func TestClassify(t *testing.T) {
	t.Parallel()

	blocked := fstab.Report{Outcome: fstab.OutcomeOK, Entries: 1, Blockers: 1}
	clean := fstab.Report{Outcome: fstab.OutcomeOK, Entries: 1}
	unknown := fstab.Report{Outcome: fstab.OutcomeUnknown, Reason: fstab.ReasonTimeout}

	tests := []struct {
		name       string
		err        error
		report     fstab.Report
		wantCause  string
		wantResult string
	}{
		{"timeout with blockers", waitErr(sandbox.ErrWaitForEnvdTimeout), blocked, causeFstabDeviceWait, waitTimeout},
		{"timeout without blockers", waitErr(sandbox.ErrWaitForEnvdTimeout), clean, causeUnknown, waitTimeout},
		{"timeout with unknown inspection", waitErr(sandbox.ErrWaitForEnvdTimeout), unknown, causeUnknown, waitTimeout},
		{"guest exited with blockers", waitErr(fmt.Errorf("%w: %w", sandbox.ErrFcProcessExited, errors.New("exit status 1"))), blocked, causeUnknown, waitGuestExited},
		{"canceled", waitErr(context.Canceled), blocked, causeUnknown, waitCanceled},
		{"other", errors.New("connection reset"), blocked, causeUnknown, waitOther},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cause, result := classify(tt.err, tt.report)
			assert.Equal(t, tt.wantCause, cause)
			assert.Equal(t, tt.wantResult, result)
		})
	}
}

func TestAnnotateEmitsOnlyCountsAndEnums(t *testing.T) {
	t.Parallel()

	const sentinel = "SENTINEL-9f3a"
	target := fstab.DefaultTarget()
	target.IdentityKnown = true
	report := fstab.Parse([]byte(
		"/dev/"+sentinel+" /mnt/"+sentinel+" ext4 defaults 0 0\n"+
			"UUID="+sentinel+" /mnt/data ext4 defaults 0 0\n"+
			"/dev/"+sentinel+" none swap defaults 0 0\n",
	), target)
	require.Equal(t, 2, report.Blockers)
	diag := &bootDiagnostics{report: report, inspectDuration: 3 * time.Millisecond}

	core, logs := observer.New(zap.DebugLevel)
	rec := tracetest.NewSpanRecorder()
	ctx, span := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)).Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/layer").Start(t.Context(), "run-in-sandbox")

	diag.annotate(ctx, logger.NewTracedLoggerFromCore(core), waitErr(sandbox.ErrWaitForEnvdTimeout))
	span.End()

	enums := map[string][]string{
		"boot_failure.cause":       {causeFstabDeviceWait, causeUnknown},
		"boot_failure.wait_result": {waitTimeout, waitGuestExited, waitCanceled, waitOther},
		"fstab.inspect":            {string(fstab.OutcomeOK), string(fstab.OutcomeAbsent), string(fstab.OutcomeUnknown)},
	}
	wantKeys := []string{
		"boot_failure.cause", "boot_failure.wait_result", "fstab.inspect", "fstab.inspect_ms",
		"fstab.required_blockers", "fstab.swap_missing", "fstab.unresolved", "fstab.entries", "fstab.malformed",
		"fstab.blockers_dev", "fstab.blockers_uuid", "fstab.blockers_label",
		"fstab.blockers_partuuid", "fstab.blockers_partlabel",
	}
	slices.Sort(wantKeys)

	assertAttrs := func(t *testing.T, attrs []attribute.KeyValue) {
		t.Helper()

		keys := make([]string, 0, len(attrs))
		for _, kv := range attrs {
			key := string(kv.Key)
			keys = append(keys, key)
			value := kv.Value.Emit()
			assert.NotContains(t, value, sentinel, key)
			assert.NotContains(t, value, "/", key)
			if kv.Value.Type() == attribute.STRING {
				assert.Contains(t, enums[key], value, key)
			} else {
				assert.Equal(t, attribute.INT64, kv.Value.Type(), key)
			}
		}
		slices.Sort(keys)
		assert.Equal(t, wantKeys, keys)
	}

	ended := rec.Ended()
	require.Len(t, ended, 1)
	assertAttrs(t, ended[0].Attributes())
	require.Len(t, ended[0].Events(), 1)
	assert.Equal(t, "boot-failure-diagnostics", ended[0].Events()[0].Name)
	assertAttrs(t, ended[0].Events()[0].Attributes)

	entries := logs.All()
	require.Len(t, entries, 1)
	assert.NotContains(t, entries[0].Message, sentinel)
	var logAttrs []attribute.KeyValue
	for key, value := range entries[0].ContextMap() {
		// The logger adds the trace and span ids of ctx.
		if key == "trace_id" || key == "span_id" {
			continue
		}
		switch v := value.(type) {
		case int64:
			logAttrs = append(logAttrs, attribute.Int64(key, v))
		case string:
			logAttrs = append(logAttrs, attribute.String(key, v))
		default:
			t.Errorf("field %s has type %T", key, value)
		}
	}
	assertAttrs(t, logAttrs)
	assert.Equal(t, "fstab_device_wait", entries[0].ContextMap()["boot_failure.cause"])
	assert.Equal(t, int64(1), entries[0].ContextMap()["fstab.blockers_uuid"])
	assert.Equal(t, int64(1), entries[0].ContextMap()["fstab.swap_missing"])
}
