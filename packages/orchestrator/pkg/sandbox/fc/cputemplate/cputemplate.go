// Package cputemplate parses and validates the custom Firecracker CPU template.
package cputemplate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"

	"github.com/go-openapi/strfmt"

	"github.com/e2b-dev/infra/packages/shared/pkg/fc/models"
	"github.com/e2b-dev/infra/packages/shared/pkg/fcversion"
)

// hostArch is the architecture the guest boots on since Firecracker never emulates.
var hostArch = runtime.GOARCH

// Template is a custom Firecracker CPU template in the format accepted by the PUT /cpu-config body.
type Template models.CPUConfig

// Parse parses a CPU template document. Empty, null and a template that sets nothing are no
// template (nil). Validate checks whether a host can apply the result.
func Parse(raw []byte) (*Template, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}

	// Firecracker rejects unknown fields, so catch them before any boot does.
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()

	var tmpl Template
	if err := dec.Decode(&tmpl); err != nil {
		return nil, fmt.Errorf("invalid CPU template: %w", err)
	}

	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("invalid CPU template: trailing data after the JSON object")
	}

	model := models.CPUConfig(tmpl)
	if err := model.Validate(strfmt.Default); err != nil {
		return nil, fmt.Errorf("invalid CPU template: %w", err)
	}

	// The model skips null entries, which Firecracker then rejects along with the whole request.
	hasNull := slices.Contains(tmpl.MsrModifiers, nil) ||
		slices.Contains(tmpl.RegModifiers, nil) ||
		slices.Contains(tmpl.VcpuFeatures, nil) ||
		slices.ContainsFunc(tmpl.CpuidModifiers, func(m *models.CpuidLeafModifier) bool {
			return m == nil || slices.Contains(m.Modifiers, nil)
		})
	if hasNull {
		return nil, errors.New("invalid CPU template: a modifier collection contains a null entry")
	}

	// The model skips its range check at zero, because for an optional field zero is how
	// "absent" decodes. A zero the document spells out is a value like any other, and it is
	// below the minimum; omitting the field is how to leave the host frequency.
	var present struct {
		X86TscKhz *int64 `json:"x86_tsc_khz"`
	}
	if err := json.Unmarshal(raw, &present); err == nil && present.X86TscKhz != nil && *present.X86TscKhz == 0 {
		return nil, errors.New("invalid CPU template: x86_tsc_khz 0 is below the minimum of 100000 kHz; omit the field to keep the host TSC frequency")
	}

	if tmpl.IsEmpty() {
		return nil, nil
	}

	return &tmpl, nil
}

// Validate reports whether fc, running on this host, can apply this template: each
// architecture's Firecracker rejects the other's fields, and older versions reject x86_tsc_khz.
func (t Template) Validate(fc fcversion.Info) error {
	if t.X86TscKhz != 0 && !fc.HasTscKhzTemplate() {
		return errors.New("CPU template sets x86_tsc_khz, which this Firecracker version does not support")
	}

	return t.validateFor(hostArch)
}

func (t Template) validateFor(arch string) error {
	// Re-run here, not only in Parse, because a template also arrives from stored metadata.
	model := models.CPUConfig(t)
	if err := model.Validate(strfmt.Default); err != nil {
		return fmt.Errorf("invalid CPU template: %w", err)
	}

	if arch != "amd64" {
		switch {
		case t.X86TscKhz != 0:
			return fmt.Errorf("CPU template sets x86_tsc_khz, which Firecracker supports only on x86_64 (host is %s)", arch)
		case len(t.CpuidModifiers) > 0:
			return fmt.Errorf("CPU template sets cpuid_modifiers, which Firecracker supports only on x86_64 (host is %s)", arch)
		case len(t.MsrModifiers) > 0:
			return fmt.Errorf("CPU template sets msr_modifiers, which Firecracker supports only on x86_64 (host is %s)", arch)
		}
	}

	if arch != "arm64" {
		switch {
		case len(t.RegModifiers) > 0:
			return fmt.Errorf("CPU template sets reg_modifiers, which Firecracker supports only on aarch64 (host is %s)", arch)
		case len(t.VcpuFeatures) > 0:
			return fmt.Errorf("CPU template sets vcpu_features, which Firecracker supports only on aarch64 (host is %s)", arch)
		}
	}

	return nil
}

// IsEmpty reports whether this template changes nothing about the guest CPU.
func (t Template) IsEmpty() bool {
	return t.X86TscKhz == 0 &&
		len(t.CpuidModifiers) == 0 &&
		len(t.KvmCapabilities) == 0 &&
		len(t.MsrModifiers) == 0 &&
		len(t.RegModifiers) == 0 &&
		len(t.VcpuFeatures) == 0
}

// Clone returns a deep copy; the modifier collections are slices of pointers.
func (t Template) Clone() Template {
	var out Template
	// Cannot fail: the value was produced by this type's own marshaling.
	_ = json.Unmarshal([]byte(t.String()), &out)

	return out
}

// String renders the template as canonical JSON: fixed field order, unset fields omitted.
func (t Template) String() string {
	raw, err := json.Marshal(t)
	if err != nil {
		// Unreachable for a struct of strings, integers and slices of the same.
		return ""
	}

	return string(raw)
}

// Digest is a short, stable label for the template's settings.
func (t Template) Digest() string {
	sum := sha256.Sum256([]byte(t.String()))

	return hex.EncodeToString(sum[:6])
}

// AppliedDigest labels what a guest boots with: the digest of tmpl, or empty when tmpl is nil
// or empty, since Firecracker is then sent no template at all.
func AppliedDigest(tmpl *Template) string {
	if tmpl == nil || tmpl.IsEmpty() {
		return ""
	}

	return tmpl.Digest()
}
