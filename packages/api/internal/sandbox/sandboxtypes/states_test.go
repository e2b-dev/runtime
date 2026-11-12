package sandboxtypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKillReasonValues(t *testing.T) {
	t.Parallel()

	for reason, want := range map[KillReason]string{
		KillReasonUnknown:             "unknown",
		KillReasonRequest:             "request",
		KillReasonTimeout:             "timeout",
		KillReasonAdmin:               "admin",
		KillReasonOrphaned:            "orphaned",
		KillReasonBaseTemplateMissing: "base_template_missing",
		KillReasonTeamDeleted:         "team_deleted",
	} {
		assert.Equal(t, want, string(reason))
	}
}

func TestKillReasonString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   KillReason
		want string
	}{
		{
			name: "empty",
			in:   "",
			want: "unknown",
		},
		{
			name: "unknown",
			in:   KillReasonUnknown,
			want: "unknown",
		},
		{
			name: "request",
			in:   KillReasonRequest,
			want: "request",
		},
		{
			name: "timeout",
			in:   KillReasonTimeout,
			want: "timeout",
		},
		{
			name: "admin",
			in:   KillReasonAdmin,
			want: "admin",
		},
		{
			name: "orphaned",
			in:   KillReasonOrphaned,
			want: "orphaned",
		},
		{
			name: "base template missing",
			in:   KillReasonBaseTemplateMissing,
			want: "base_template_missing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.in.String())
		})
	}
}
