package filesystem

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateMetadata(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		metadata map[string]string
		wantErr  string
	}{
		{
			name:     "nil is ok",
			metadata: nil,
		},
		{
			name:     "empty is ok",
			metadata: map[string]string{},
		},
		{
			name:     "valid pair",
			metadata: map[string]string{"author": "mish"},
		},
		{
			name:     "empty key rejected",
			metadata: map[string]string{"": "v"},
			wantErr:  "must not be empty",
		},
		{
			name:     "oversized key rejected",
			metadata: map[string]string{strings.Repeat("k", MaxMetadataKeyLen+1): "v"},
			wantErr:  "exceeds",
		},
		{
			name:     "NUL in key rejected",
			metadata: map[string]string{"bad\x00key": "v"},
			wantErr:  "non-printable-ASCII",
		},
		{
			name:     "non-ASCII key rejected",
			metadata: map[string]string{"naïve": "v"},
			wantErr:  "non-printable-ASCII",
		},
		{
			name:     "non-ASCII value rejected",
			metadata: map[string]string{"k": "naïve"},
			wantErr:  "non-printable-ASCII",
		},
		{
			name:     "large single value within budget ok",
			metadata: map[string]string{"k": strings.Repeat("v", 2048)},
		},
		{
			name:     "oversized total rejected",
			metadata: map[string]string{"k": strings.Repeat("v", MaxMetadataTotalLen)},
			wantErr:  "total metadata size",
		},
		{
			name: "total across multiple keys rejected",
			metadata: map[string]string{
				"a": strings.Repeat("v", MaxMetadataTotalLen/2),
				"b": strings.Repeat("v", MaxMetadataTotalLen/2),
			},
			wantErr: "total metadata size",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateMetadata(tc.metadata)
			if tc.wantErr == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}
