package api

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractMetadataHeaders(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		headers map[string]string
		want    map[string]string
	}{
		{
			name:    "no metadata headers",
			headers: map[string]string{"X-Access-Token": "abc"},
			want:    nil,
		},
		{
			name:    "single header",
			headers: map[string]string{"X-Metadata-Author": "mish"},
			want:    map[string]string{"author": "mish"},
		},
		{
			name: "multiple headers, mixed case",
			headers: map[string]string{
				"X-Metadata-Author":  "mish",
				"x-metadata-purpose": "upload",
				"Content-Type":       "application/octet-stream",
			},
			want: map[string]string{"author": "mish", "purpose": "upload"},
		},
		{
			name:    "empty key dropped",
			headers: map[string]string{"X-Metadata-": "ignored"},
			want:    nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := http.Header{}
			for k, v := range tc.headers {
				h.Set(k, v)
			}
			got := extractMetadataHeaders(h)
			require.Len(t, got, len(tc.want), "got %v, want %v", got, tc.want)
			for k, v := range tc.want {
				assert.Equal(t, v, got[k], "key %q", k)
			}
		})
	}

	t.Run("multiple values for one header uses first value", func(t *testing.T) {
		t.Parallel()

		h := http.Header{}
		h.Add("X-Metadata-Author", "first")
		h.Add("X-Metadata-Author", "second")

		got := extractMetadataHeaders(h)
		require.Equal(t, "first", got["author"], "got %v", got)
	})
}
