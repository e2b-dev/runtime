package logs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlatJsonLogLineParser(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    string
		expected map[string]string
		hasError bool
	}{
		{
			name:  "string values",
			input: `{"message": "test", "level": "info"}`,
			expected: map[string]string{
				"message": "test",
				"level":   "info",
			},
			hasError: false,
		},
		{
			name:  "float64 values",
			input: `{"temperature": 25.5, "count": 42.0}`,
			expected: map[string]string{
				"temperature": "25.5",
				"count":       "42",
			},
			hasError: false,
		},
		{
			name:  "boolean values",
			input: `{"success": true, "debug": false}`,
			expected: map[string]string{
				"success": "true",
				"debug":   "false",
			},
			hasError: false,
		},
		{
			name:  "mixed types",
			input: `{"message": "hello", "count": 123, "active": true}`,
			expected: map[string]string{
				"message": "hello",
				"count":   "123",
				"active":  "true",
			},
			hasError: false,
		},
		{
			name:     "empty object",
			input:    `{}`,
			expected: map[string]string{},
			hasError: false,
		},
		{
			name:  "null values ignored",
			input: `{"message": "test", "nullField": null, "level": "info"}`,
			expected: map[string]string{
				"message": "test",
				"level":   "info",
			},
			hasError: false,
		},
		{
			name:  "array values ignored",
			input: `{"message": "test", "tags": ["tag1", "tag2"], "level": "info"}`,
			expected: map[string]string{
				"message": "test",
				"level":   "info",
			},
			hasError: false,
		},
		{
			name:  "nested object ignored",
			input: `{"message": "test", "metadata": {"key": "value"}, "level": "info"}`,
			expected: map[string]string{
				"message": "test",
				"level":   "info",
			},
			hasError: false,
		},
		{
			name:     "invalid JSON",
			input:    `{"message": "test", "level":}`,
			expected: nil,
			hasError: true,
		},
		{
			name:     "malformed JSON",
			input:    `not json at all`,
			expected: nil,
			hasError: true,
		},
		{
			name:  "integer as float64",
			input: `{"port": 8080, "timeout": 30}`,
			expected: map[string]string{
				"port":    "8080",
				"timeout": "30",
			},
			hasError: false,
		},
		{
			name:  "zero values",
			input: `{"count": 0, "enabled": false, "name": ""}`,
			expected: map[string]string{
				"count":   "0",
				"enabled": "false",
				"name":    "",
			},
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := FlatJsonLogLineParser(tt.input)

			if tt.hasError {
				assert.Error(t, err)

				return
			}

			require.NoError(t, err)

			assert.Equal(t, tt.expected, result)
		})
	}
}
