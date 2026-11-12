package secretsstore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeNameCanonicalizesAllowedName(t *testing.T) {
	t.Parallel()

	got, err := NormalizeName("  API-Key_1  ")
	require.NoError(t, err)
	require.Equal(t, "api-key_1", got)
}

func TestNormalizeNameRejectsEachInvalidBranchWithAFixedError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
	}{
		{name: "empty", input: "   "},
		{name: "too long", input: strings.Repeat("a", maxNameBytes+1)},
		{name: "reserved ID prefix", input: "  SEC_private  "},
		{name: "non-ASCII case alias", input: "KEY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := NormalizeName(tt.input)
			require.Empty(t, got)
			require.ErrorIs(t, err, ErrInvalidName)
			require.EqualError(t, err, "secret name is invalid")
			require.NotContains(t, err.Error(), tt.input, "error contains rejected input")
		})
	}
}
