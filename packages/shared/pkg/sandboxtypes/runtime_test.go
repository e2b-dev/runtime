package sandboxtypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// fieldValues flattens the fields into key -> string value. Every field
// LogFields emits is a zap.StringType, so a missing key means the field was
// omitted rather than emitted blank.
func fieldValues(t *testing.T, fields []zap.Field) map[string]string {
	t.Helper()

	out := make(map[string]string, len(fields))
	for _, f := range fields {
		require.Equal(t, zapcore.StringType, f.Type, "unexpected field type for %q", f.Key)
		_, dup := out[f.Key]
		require.False(t, dup, "duplicate field key %q", f.Key)
		out[f.Key] = f.String
	}

	return out
}

func fullRuntime() RuntimeMetadata {
	return RuntimeMetadata{
		TemplateID:  "tmpl-1",
		SandboxID:   "sbx-1",
		ExecutionID: "exec-1",
		TeamID:      "team-1",
		BuildID:     "build-1",
		// Set so the exact-match assertions below also prove the type stays
		// out of the log fields.
		SandboxType: SandboxTypeBuild,
	}
}

func TestLogFieldsAllPopulated(t *testing.T) {
	t.Parallel()

	got := fieldValues(t, fullRuntime().LogFields())

	assert.Equal(t, map[string]string{
		"sandbox.id":   "sbx-1",
		"template.id":  "tmpl-1",
		"team.id":      "team-1",
		"build.id":     "build-1",
		"execution.id": "exec-1",
	}, got)
}

// An unset id must be absent, not present and empty: the resume-build CLI
// leaves BuildID unset, and a blank build.id in Loki reads as a real value.
func TestLogFieldsOmitsEmptyIDs(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		blank   func(*RuntimeMetadata)
		absent  string
		present []string
	}{
		{
			name:    "build id",
			blank:   func(r *RuntimeMetadata) { r.BuildID = "" },
			absent:  "build.id",
			present: []string{"sandbox.id", "template.id", "team.id", "execution.id"},
		},
		{
			name:    "team id",
			blank:   func(r *RuntimeMetadata) { r.TeamID = "" },
			absent:  "team.id",
			present: []string{"sandbox.id", "template.id", "build.id", "execution.id"},
		},
		{
			name:    "sandbox id",
			blank:   func(r *RuntimeMetadata) { r.SandboxID = "" },
			absent:  "sandbox.id",
			present: []string{"template.id", "team.id", "build.id", "execution.id"},
		},
		{
			name:    "template id",
			blank:   func(r *RuntimeMetadata) { r.TemplateID = "" },
			absent:  "template.id",
			present: []string{"sandbox.id", "team.id", "build.id", "execution.id"},
		},
		{
			name:    "execution id",
			blank:   func(r *RuntimeMetadata) { r.ExecutionID = "" },
			absent:  "execution.id",
			present: []string{"sandbox.id", "template.id", "team.id", "build.id"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			runtime := fullRuntime()
			tc.blank(&runtime)

			got := fieldValues(t, runtime.LogFields())

			assert.NotContains(t, got, tc.absent)
			assert.Len(t, got, len(tc.present))
			for _, key := range tc.present {
				assert.Contains(t, got, key)
			}
		})
	}
}

func TestSandboxTypeString(t *testing.T) {
	t.Parallel()
	for input, want := range map[SandboxType]string{"": "sandbox", SandboxTypeSandbox: "sandbox", SandboxTypeBuild: "build", "custom": "custom"} {
		require.Equal(t, want, input.String())
	}
}

// The whole per-class DSCP split hangs off this mapping: if a build sandbox
// stops reporting EgressClassBuild, build egress silently reverts to the
// sandbox class and nothing else in the stack notices.
func TestSandboxType_EgressClass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sandboxType SandboxType
		want        EgressClass
	}{
		{name: "build sandboxes get the build class", sandboxType: SandboxTypeBuild, want: EgressClassBuild},
		{name: "regular sandboxes get the sandbox class", sandboxType: SandboxTypeSandbox, want: EgressClassSandbox},
		{name: "the empty type is a regular sandbox", sandboxType: "", want: EgressClassSandbox},
		{name: "an unknown type falls back to the sandbox class", sandboxType: SandboxType("something-else"), want: EgressClassSandbox},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, tt.sandboxType.EgressClass())
		})
	}
}
