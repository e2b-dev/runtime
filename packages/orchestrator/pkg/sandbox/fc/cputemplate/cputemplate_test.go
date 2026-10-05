package cputemplate

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/fc/models"
	"github.com/e2b-dev/infra/packages/shared/pkg/fcversion"
)

const cpuidTemplate = `{
	"cpuid_modifiers": [
		{
			"leaf": "0x1",
			"subleaf": "0x0",
			"flags": 0,
			"modifiers": [{"register": "ecx", "bitmap": "0bxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx0"}]
		}
	],
	"msr_modifiers": [{"addr": "0x10a", "bitmap": "0b0000000000000000000000000000000000000000000000000000000000000000"}],
	"x86_tsc_khz": 3200000
}`

func TestParse(t *testing.T) {
	t.Parallel()

	t.Run("an empty or null document is no template, not an error", func(t *testing.T) {
		t.Parallel()

		// The value of an unset flag must not be an error path.
		for _, raw := range []string{"", "   ", "null", " null\n", "{}", `{"kvm_capabilities": []}`} {
			got, err := Parse([]byte(raw))
			require.NoError(t, err)
			assert.Nil(t, got, "document %q", raw)
		}
	})

	t.Run("a TSC-only template sets only the TSC frequency", func(t *testing.T) {
		t.Parallel()

		got, err := Parse([]byte(`{"x86_tsc_khz": 3200000}`))
		require.NoError(t, err)
		require.NotNil(t, got)

		assert.Equal(t, Template{X86TscKhz: 3_200_000}, *got)
	})

	t.Run("the bounds are inclusive", func(t *testing.T) {
		t.Parallel()

		for _, khz := range []int64{100_000, 10_000_000} {
			got, err := Parse(fmt.Appendf(nil, `{"x86_tsc_khz": %d}`, khz))
			require.NoError(t, err, "%d kHz", khz)
			require.NotNil(t, got)
			assert.Equal(t, khz, got.X86TscKhz)
		}
	})

	t.Run("a full template keeps every collection", func(t *testing.T) {
		t.Parallel()

		got, err := Parse([]byte(cpuidTemplate))
		require.NoError(t, err)
		require.NotNil(t, got)

		require.Len(t, got.CpuidModifiers, 1)
		assert.Equal(t, "0x1", *got.CpuidModifiers[0].Leaf)
		require.Len(t, got.CpuidModifiers[0].Modifiers, 1)
		assert.Equal(t, "ecx", *got.CpuidModifiers[0].Modifiers[0].Register)
		require.Len(t, got.MsrModifiers, 1)
		assert.Equal(t, int64(3_200_000), got.X86TscKhz)
	})

	t.Run("String reads back as the same template", func(t *testing.T) {
		t.Parallel()

		// What metadata records must paste straight back into the flag.
		got, err := Parse([]byte(cpuidTemplate))
		require.NoError(t, err)

		again, err := Parse([]byte(got.String()))
		require.NoError(t, err)
		assert.Equal(t, got, again)
	})

	rejected := map[string]string{
		// Firecracker rejects unknown fields, so Parse must catch them before any boot.
		"an unknown field":               `{"tsc_khz": 3200000}`,
		"an unknown field in a modifier": `{"msr_modifiers": [{"addr": "0x10a", "bitmap": "0b0", "mask": "0b1"}]}`,
		"a fractional TSC frequency":     `{"x86_tsc_khz": 3200000.5}`,
		// Zero decodes like an absent field, but a document that spells it out is out of range.
		"a zero TSC frequency":     `{"x86_tsc_khz": 0}`,
		"a negative TSC frequency": `{"x86_tsc_khz": -1}`,
		// The bounds are 100 MHz to 10 GHz in kHz: a value in Hz or MHz by mistake must not boot.
		"a TSC frequency given in MHz":        `{"x86_tsc_khz": 3200}`,
		"a TSC frequency given in Hz":         `{"x86_tsc_khz": 3200000000}`,
		"a TSC frequency just below 100 MHz":  `{"x86_tsc_khz": 99999}`,
		"a TSC frequency just above 10 GHz":   `{"x86_tsc_khz": 10000001}`,
		"a TSC frequency beyond 32 bits":      `{"x86_tsc_khz": 4294967296}`,
		"a modifier missing a required field": `{"msr_modifiers": [{"addr": "0x10a"}]}`,
		// The model skips null entries, so Firecracker would be the first to reject them.
		"a null MSR modifier":            `{"msr_modifiers": [null]}`,
		"a null CPUID modifier":          `{"cpuid_modifiers": [null]}`,
		"a null CPUID register modifier": `{"cpuid_modifiers": [{"leaf": "0x1", "subleaf": "0x0", "flags": 0, "modifiers": [null]}]}`,
		"a null register modifier":       `{"reg_modifiers": [null]}`,
		"a null vCPU feature":            `{"vcpu_features": [null]}`,
		"a null entry after a valid one": `{"msr_modifiers": [{"addr": "0x10a", "bitmap": "0b0"}, null]}`,
		"a non-object document":          `["x86_tsc_khz"]`,
		"malformed JSON":                 `{"x86_tsc_khz": }`,
		"trailing data":                  `{"x86_tsc_khz": 3200000} {}`,
	}
	for name, raw := range rejected {
		t.Run("rejects "+name, func(t *testing.T) {
			t.Parallel()

			got, err := Parse([]byte(raw))
			require.Error(t, err)
			assert.Nil(t, got)
		})
	}
}

// Each architecture's Firecracker rejects the other's fields.
func TestValidateForArchitecture(t *testing.T) {
	t.Parallel()

	x86Only := map[string]Template{
		"x86_tsc_khz":     {X86TscKhz: 3_200_000},
		"cpuid_modifiers": {CpuidModifiers: []*models.CpuidLeafModifier{{Leaf: new("0x1"), Subleaf: new("0x0"), Flags: new(int32(0)), Modifiers: []*models.CpuidRegisterModifier{}}}},
		"msr_modifiers":   {MsrModifiers: []*models.MsrModifier{{Addr: new("0x10a"), Bitmap: new("0b0")}}},
	}
	armOnly := map[string]Template{
		"reg_modifiers": {RegModifiers: []*models.ArmRegisterModifier{{Addr: new("0x1"), Bitmap: new("0b0")}}},
		"vcpu_features": {VcpuFeatures: []*models.VcpuFeatures{{Index: new(int32(0)), Bitmap: new("0b0")}}},
	}

	for field, tmpl := range x86Only {
		t.Run(field+" is x86_64-only", func(t *testing.T) {
			t.Parallel()

			require.NoError(t, tmpl.validateFor("amd64"))

			err := tmpl.validateFor("arm64")
			require.Error(t, err)
			assert.Contains(t, err.Error(), field)
			assert.Contains(t, err.Error(), "arm64")
		})
	}

	for field, tmpl := range armOnly {
		t.Run(field+" is aarch64-only", func(t *testing.T) {
			t.Parallel()

			require.NoError(t, tmpl.validateFor("arm64"))

			err := tmpl.validateFor("amd64")
			require.Error(t, err)
			assert.Contains(t, err.Error(), field)
			assert.Contains(t, err.Error(), "amd64")
		})
	}

	t.Run("kvm_capabilities is valid on both", func(t *testing.T) {
		t.Parallel()

		tmpl := Template{KvmCapabilities: []string{"!121"}}
		assert.NoError(t, tmpl.validateFor("amd64"))
		assert.NoError(t, tmpl.validateFor("arm64"))
	})

	t.Run("an empty template is valid anywhere", func(t *testing.T) {
		t.Parallel()

		assert.NoError(t, Template{}.validateFor("amd64"))
		assert.NoError(t, Template{}.validateFor("arm64"))
	})

	t.Run("an out-of-range frequency from stored metadata is rejected", func(t *testing.T) {
		t.Parallel()

		// Parse never produces one, but a template also arrives from stored metadata.
		require.Error(t, Template{X86TscKhz: -1}.validateFor("amd64"))
		require.Error(t, Template{X86TscKhz: 99_999}.validateFor("amd64"))
		require.Error(t, Template{X86TscKhz: 10_000_001}.validateFor("amd64"))
	})
}

// Validate must agree with the helper the tests above exercise, or those tests pin behavior
// nothing in production runs.
func TestValidateUsesTheHostArchitecture(t *testing.T) {
	t.Parallel()

	fc, err := fcversion.New("v1.14-0.2.0")
	require.NoError(t, err)

	// x86-only but without x86_tsc_khz, so the version gate passes and only the arch decides.
	tmpl := Template{MsrModifiers: []*models.MsrModifier{{Addr: new("0x10a"), Bitmap: new("0b0")}}}

	assert.Equal(t, tmpl.validateFor(hostArch) == nil, tmpl.Validate(fc) == nil)
}

func TestIsEmpty(t *testing.T) {
	t.Parallel()

	assert.True(t, Template{}.IsEmpty(),
		"a template that changes nothing must be skippable, so no pre-boot request is sent")
	assert.False(t, Template{X86TscKhz: 3_200_000}.IsEmpty())
	assert.False(t, Template{KvmCapabilities: []string{"!121"}}.IsEmpty())
	assert.False(t, Template{VcpuFeatures: []*models.VcpuFeatures{{}}}.IsEmpty())
}

func TestCloneIsDeep(t *testing.T) {
	t.Parallel()

	orig, err := Parse([]byte(cpuidTemplate))
	require.NoError(t, err)

	clone := orig.Clone()
	*clone.CpuidModifiers[0].Leaf = "0x7"

	assert.Equal(t, "0x1", *orig.CpuidModifiers[0].Leaf,
		"a template recorded in metadata must not be reachable through the config it was copied from")
}

// String is what the base layer cache key is built from, so it must be canonical: the same
// settings render the same way however the flag spelled them.
func TestStringIsCanonical(t *testing.T) {
	t.Parallel()

	a, err := Parse([]byte(`{"x86_tsc_khz": 3200000, "kvm_capabilities": ["!121"]}`))
	require.NoError(t, err)
	b, err := Parse([]byte("{\n  \"kvm_capabilities\": [\"!121\"],\n  \"x86_tsc_khz\": 3200000\n}"))
	require.NoError(t, err)

	assert.Equal(t, a.String(), b.String())
	assert.Equal(t, a.Digest(), b.Digest())

	assert.JSONEq(t, `{"x86_tsc_khz":3200000}`, Template{X86TscKhz: 3_200_000}.String())
	assert.NotEqual(t, Template{X86TscKhz: 3_200_000}.String(), Template{X86TscKhz: 2_400_000}.String(),
		"two frequencies are two different guests and must not share a cached layer")
	assert.NotEqual(t, Template{X86TscKhz: 3_200_000}.Digest(), Template{X86TscKhz: 2_400_000}.Digest())
	assert.Len(t, Template{X86TscKhz: 3_200_000}.Digest(), 12)
}

// x86_tsc_khz is an e2b extension first served by v1.14-0.3.0; the upstream fields need no
// version gate.
func TestValidateGatesTscKhzOnVersion(t *testing.T) {
	t.Parallel()

	old, err := fcversion.New("v1.14-0.2.0")
	require.NoError(t, err)

	err = Template{X86TscKhz: 3_200_000}.Validate(old)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "x86_tsc_khz")

	assert.NoError(t, Template{KvmCapabilities: []string{"!121"}}.Validate(old))
	assert.NoError(t, Template{}.Validate(old))

	if hostArch == "amd64" {
		floor, err := fcversion.New("v1.14-0.3.0")
		require.NoError(t, err)
		assert.NoError(t, Template{X86TscKhz: 3_200_000}.Validate(floor))
	}
}

func TestAppliedDigest(t *testing.T) {
	t.Parallel()

	assert.Empty(t, AppliedDigest(nil), "nil is no template")
	assert.Empty(t, AppliedDigest(&Template{}), "an empty template is never sent, so it is none")

	tmpl, err := Parse([]byte(cpuidTemplate))
	require.NoError(t, err)
	assert.Equal(t, tmpl.Digest(), AppliedDigest(tmpl))
}
