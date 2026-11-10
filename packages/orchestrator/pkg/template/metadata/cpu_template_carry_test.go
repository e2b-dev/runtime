package metadata

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc/cputemplate"
)

func testCPUTemplate() *cputemplate.Template {
	return &cputemplate.Template{X86TscKhz: 3_200_000, KvmCapabilities: []string{"!121"}}
}

func baseWithCPUTemplate() Template {
	return Template{
		Version:          CurrentVersion,
		Template:         TemplateMetadata{BuildID: "build-1"},
		Context:          Context{User: "root"},
		CPUTemplate:      testCPUTemplate(),
		BuildCPUTemplate: testCPUTemplate(),
	}
}

// Every copy-constructor must carry the template, or a filesystem-only cold boot silently
// presents a different CPU than the build did. Same contract as cmdline_args_carry_test.go.
func TestCopyConstructorsCarryCPUTemplate(t *testing.T) {
	t.Parallel()

	base := baseWithCPUTemplate()

	tests := map[string]Template{
		// The pause path, so the case that matters most.
		"SameVersionTemplate": base.SameVersionTemplate(TemplateMetadata{BuildID: "build-2"}),
		"NewVersionTemplate":  base.NewVersionTemplate(TemplateMetadata{BuildID: "build-2"}),
		"WithPrefetch":        base.WithPrefetch(&Prefetch{Memory: &MemoryPrefetchMapping{}}),
	}

	for name, got := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, testCPUTemplate(), got.CPUTemplate,
				"%s must carry the CPU template a cold boot replays", name)
			assert.Equal(t, testCPUTemplate(), got.BuildCPUTemplate,
				"%s must carry the build's CPU template, which a cleared override returns to", name)
		})
	}
}

// BasedOn is the one copy-constructor that must NOT carry it: a new build resolves the flag
// for its own team.
func TestBasedOnDropsCPUTemplate(t *testing.T) {
	t.Parallel()

	base := baseWithCPUTemplate()

	got := base.BasedOn(FromTemplate{Alias: "a", BuildID: "build-0"})

	assert.Nil(t, got.CPUTemplate, "a from-template build must not inherit the parent's CPU template")
	assert.Nil(t, got.BuildCPUTemplate, "a from-template build must not inherit the parent's CPU template")
	assert.Equal(t, base.Context, got.Context, "the rest of the lineage state still carries")
}

func TestCPUTemplateSurvivesFileRoundTrip(t *testing.T) {
	t.Parallel()

	base := Template{
		Version:     CurrentVersion,
		Template:    TemplateMetadata{BuildID: "build-rt", KernelVersion: "6.1", FirecrackerVersion: "1.14"},
		Context:     Context{User: "root"},
		CPUTemplate: testCPUTemplate(),
	}

	path := filepath.Join(t.TempDir(), "metadata.json")
	require.NoError(t, base.ToFile(path))

	got, err := FromFile(path)
	require.NoError(t, err)

	require.NotNil(t, got.CPUTemplate)
	assert.Equal(t, *testCPUTemplate(), *got.CPUTemplate)
}

// Metadata written before this field existed decodes to no template, so no migration is needed.
func TestAbsentCPUTemplateDecodesToNone(t *testing.T) {
	t.Parallel()

	raw := `{"version":2,"template":{"build_id":"b","kernel_version":"6.1","firecracker_version":"1.14"},"context":{"user":"root"}}`

	got, err := deserialize(bytes.NewReader([]byte(raw)))
	require.NoError(t, err)
	assert.Nil(t, got.CPUTemplate)
}
