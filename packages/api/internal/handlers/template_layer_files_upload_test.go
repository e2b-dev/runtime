package handlers

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
)

// Pre-headers clients must keep parsing the new response; header-less providers must keep producing the old bytes exactly.
func TestTemplateBuildFileUploadHeadersAreAdditive(t *testing.T) {
	t.Parallel()

	t.Run("no headers keeps the response bytes unchanged", func(t *testing.T) {
		t.Parallel()

		body, err := json.Marshal(api.TemplateBuildFileUpload{Present: false, Url: new("https://bucket.example/signed")})
		require.NoError(t, err)
		assert.JSONEq(t, `{"present":false,"url":"https://bucket.example/signed"}`, string(body))
	})

	t.Run("a client ignoring headers still parses the response", func(t *testing.T) {
		t.Parallel()

		var legacy struct {
			Present bool    `json:"present"`
			Url     *string `json:"url"`
		}
		require.NoError(t, json.Unmarshal(
			[]byte(`{"present":false,"url":"https://account.example/signed","headers":{"x-ms-blob-type":"BlockBlob"}}`),
			&legacy))

		assert.False(t, legacy.Present)
		require.NotNil(t, legacy.Url)
		assert.Equal(t, "https://account.example/signed", *legacy.Url)
	})
}
