package telemetry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseEdgeTraceID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		gcpHeader string
		awsHeader string
		want      string
		wantOK    bool
	}{
		{
			name:      "prefers gcp trace header",
			gcpHeader: "0123456789abcdef0123456789abcdef/123;o=1",
			awsHeader: "Root=1-01234567-89abcdef0123456789abcdef",
			want:      "0123456789abcdef0123456789abcdef",
			wantOK:    true,
		},
		{
			name:      "falls back to aws trace header",
			awsHeader: "Self=ignored; Root=1-01234567-89abcdef0123456789abcdef; Parent=123",
			want:      "0123456789abcdef0123456789abcdef",
			wantOK:    true,
		},
		{
			name:      "rejects malformed gcp trace id",
			gcpHeader: "not-hex/123;o=1",
			wantOK:    false,
		},
		{
			name:      "rejects malformed aws trace id",
			awsHeader: "Root=1-xyz-89abcdef0123456789abcdef",
			wantOK:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, gotOK := ParseEdgeTraceID(tt.gcpHeader, tt.awsHeader)
			require.Equal(t, tt.wantOK, gotOK)
			require.Equal(t, tt.want, got)
		})
	}
}
