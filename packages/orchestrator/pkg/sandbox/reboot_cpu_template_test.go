//go:build linux

package sandbox

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc/cputemplate"
	templatemocks "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/mocks"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func TestCheckRebootCPUTemplate(t *testing.T) {
	t.Parallel()

	tsc := &cputemplate.Template{X86TscKhz: 3_200_000}
	kvm := &cputemplate.Template{KvmCapabilities: []string{"!121"}}

	// v1.14-0.2.0 predates x86_tsc_khz, so the reboot's version must reject it.
	err := checkRebootCPUTemplate(tsc, "v1.14-0.2.0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "v1.14-0.2.0")

	if runtime.GOARCH == "amd64" {
		require.NoError(t, checkRebootCPUTemplate(tsc, "v1.14-0.3.0"))
	}

	require.NoError(t, checkRebootCPUTemplate(kvm, "v1.14-0.3.0"))
	require.Error(t, checkRebootCPUTemplate(kvm, "not-a-version"))

	// No template needs no version.
	require.NoError(t, checkRebootCPUTemplate(nil, "not-a-version"))
	require.NoError(t, checkRebootCPUTemplate(&cputemplate.Template{}, "not-a-version"))
}

// The override replaces the build's template for one boot; with it cleared, the next boot is
// back on the build's template even though the last boot ran the override.
func TestRebootCPUTemplate(t *testing.T) {
	t.Parallel()

	a := &cputemplate.Template{KvmCapabilities: []string{"!121"}}
	b := &cputemplate.Template{KvmCapabilities: []string{"!122"}}
	// The last boot ran B under the override.
	meta := metadata.Template{BuildCPUTemplate: a, CPUTemplate: b}

	got, overridden, err := rebootCPUTemplate(meta, ldvalue.Null())
	require.NoError(t, err)
	assert.False(t, overridden)
	assert.Equal(t, a, got, "with no override the build's template returns")

	for _, none := range []string{`{}`, `{"template":null}`} {
		got, overridden, err = rebootCPUTemplate(meta, ldvalue.Parse([]byte(none)))
		require.NoError(t, err)
		assert.False(t, overridden)
		assert.Equal(t, a, got, "%s is no override", none)
	}

	got, overridden, err = rebootCPUTemplate(meta, ldvalue.Parse([]byte(`{"template":{"kvm_capabilities":["!122"]}}`)))
	require.NoError(t, err)
	assert.True(t, overridden)
	assert.Equal(t, b, got)

	got, overridden, err = rebootCPUTemplate(meta, ldvalue.Parse([]byte(`{"template":{}}`)))
	require.NoError(t, err)
	assert.True(t, overridden)
	assert.Nil(t, got, `{"template":{}} boots with no template`)

	for _, invalid := range []string{`{"template":{"bogus":1}}`, `{"kvm_capabilities":["!122"]}`, `{"template":{},"x":1}`, `[]`} {
		got, overridden, err = rebootCPUTemplate(meta, ldvalue.Parse([]byte(invalid)))
		require.Error(t, err, invalid)
		assert.False(t, overridden)
		assert.Equal(t, a, got, "an invalid override falls back to the build's template")
	}
}

// A rejected override boots the build's template, exactly as no override does, so the span and
// the log are the only places that tell the two apart.
func TestRebootSandboxMarksRejectedCPUTemplateOverride(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		override string
		invalid  bool
	}{
		{override: `{}`, invalid: false},
		{override: `{"kvm_capabilities":["!122"]}`, invalid: true},
	} {
		t.Run(tc.override, func(t *testing.T) {
			t.Parallel()

			td := ldtestdata.DataSource()
			td.Update(td.Flag(featureflags.RebootCPUTemplateOverride.Key()).ValueForAll(ldvalue.Parse([]byte(tc.override))))
			ff, err := featureflags.NewClientWithDatasource(td)
			require.NoError(t, err)
			t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

			tpl := templatemocks.NewMockTemplate(t)
			tpl.EXPECT().Files().Return(storage.CachePaths{Paths: storage.Paths{BuildID: uuid.NewString()}})
			tpl.EXPECT().Metadata().Return(metadata.Template{
				FilesystemOnly:   true,
				BuildCPUTemplate: &cputemplate.Template{KvmCapabilities: []string{"!121"}},
			}, nil)

			ctx, parent := otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox").Start(t.Context(), "test")
			// The build's template needs a Firecracker version to check against, so an
			// unparseable one stops the boot right after the template is picked.
			config := &Config{FirecrackerConfig: fc.Config{FirecrackerVersion: "not-a-version"}}
			sandboxID := "sbx-" + uuid.NewString()
			f := &Factory{featureFlags: ff}
			_, err = f.RebootSandbox(ctx, tpl, config, sandboxtypes.RuntimeMetadata{SandboxID: sandboxID},
				time.Now().Add(time.Minute), nil, false, false, nil)
			parent.End()
			require.ErrorContains(t, err, "not-a-version")

			got := map[attribute.Key]bool{}
			for _, sp := range testSpanRecorder.Ended() {
				if sp.Name() != "reboot sandbox" || sp.Parent().SpanID() != parent.SpanContext().SpanID() {
					continue
				}
				for _, kv := range sp.Attributes() {
					if kv.Value.Type() == attribute.BOOL {
						got[kv.Key] = kv.Value.AsBool()
					}
				}
			}
			require.Contains(t, got, attribute.Key("sandbox.cpu_template_override_invalid"))
			assert.Equal(t, tc.invalid, got["sandbox.cpu_template_override_invalid"])
			assert.False(t, got["sandbox.cpu_template_overridden"])

			if tc.invalid {
				entries := testLogObserver.FilterMessage("ignoring invalid reboot CPU template override").
					FilterField(zap.String("sandbox.id", sandboxID)).All()
				require.Len(t, entries, 1)
				assert.Equal(t, featureflags.RebootCPUTemplateOverride.Key(), entries[0].ContextMap()["flag"])
			}
		})
	}
}
