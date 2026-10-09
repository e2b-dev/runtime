package sandboxtypes

import "testing"

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
		if string(reason) != want {
			t.Errorf("KillReason = %q, want %q", string(reason), want)
		}
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

			if got := tt.in.String(); got != tt.want {
				t.Errorf("(%q).String() = %q, want %q", string(tt.in), got, tt.want)
			}
		})
	}
}
