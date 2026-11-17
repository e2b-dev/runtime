package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONBStringMap_MarshalJSON_Nil(t *testing.T) {
	t.Parallel()

	var m JSONBStringMap
	data, err := json.Marshal(m)
	require.NoError(t, err)
	assert.Equal(t, "{}", string(data))
}

func TestJSONBStringMap_MarshalJSON_Empty(t *testing.T) {
	t.Parallel()

	m := JSONBStringMap{}
	data, err := json.Marshal(m)
	require.NoError(t, err)
	assert.Equal(t, "{}", string(data))
}

func TestJSONBStringMap_MarshalJSON_WithValues(t *testing.T) {
	t.Parallel()

	m := JSONBStringMap{"key": "value"}
	data, err := json.Marshal(m)
	require.NoError(t, err)
	assert.JSONEq(t, `{"key":"value"}`, string(data))
}

func TestJSONBStringMap_UnmarshalJSON_Null(t *testing.T) {
	t.Parallel()

	var m JSONBStringMap
	err := json.Unmarshal([]byte("null"), &m)
	require.NoError(t, err)
	assert.NotNil(t, m)
	assert.Empty(t, m)
}

func TestJSONBStringMap_UnmarshalJSON_EmptyObject(t *testing.T) {
	t.Parallel()

	var m JSONBStringMap
	err := json.Unmarshal([]byte("{}"), &m)
	require.NoError(t, err)
	assert.NotNil(t, m)
	assert.Empty(t, m)
}

func TestJSONBStringMap_UnmarshalJSON_WithValues(t *testing.T) {
	t.Parallel()

	var m JSONBStringMap
	err := json.Unmarshal([]byte(`{"key":"value"}`), &m)
	require.NoError(t, err)
	assert.Equal(t, JSONBStringMap{"key": "value"}, m)
}

func TestJSONBStringMap_RoundTrip(t *testing.T) {
	t.Parallel()

	original := JSONBStringMap{"foo": "bar", "baz": "qux"}
	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded JSONBStringMap
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}

func TestJSONBStringMap_NilRoundTrip(t *testing.T) {
	t.Parallel()

	var original JSONBStringMap // nil
	data, err := json.Marshal(original)
	require.NoError(t, err)
	assert.Equal(t, "{}", string(data))

	var decoded JSONBStringMap
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)
	assert.NotNil(t, decoded)
	assert.Empty(t, decoded)
}

func TestPausedSandboxConfig_AutoPauseFilesystemOnlyRoundTrip(t *testing.T) {
	t.Parallel()

	// The policy must survive the column write path (Value -> JSONB) and read
	// back identically for both kinds.
	for _, want := range []bool{true, false} {
		v, err := PausedSandboxConfig{
			Version:                 PausedSandboxConfigVersion,
			AutoPauseFilesystemOnly: want,
		}.Value()
		require.NoError(t, err)

		raw, ok := v.(string)
		require.True(t, ok)

		var decoded PausedSandboxConfig
		require.NoError(t, json.Unmarshal([]byte(raw), &decoded))
		assert.Equal(t, want, decoded.AutoPauseFilesystemOnly)
	}
}

func TestPausedSandboxConfig_LegacyRowDefaultsToMemoryAutoPause(t *testing.T) {
	t.Parallel()

	// A snapshot row written before the field existed omits the key; it must
	// decode to false (a memory auto-pause), not error.
	var decoded PausedSandboxConfig
	require.NoError(t, json.Unmarshal([]byte(`{"filesystemOnly":true}`), &decoded))

	assert.False(t, decoded.AutoPauseFilesystemOnly, "legacy rows must default to a memory auto-pause")
	assert.True(t, decoded.FilesystemOnly, "unrelated fields must still decode")
}

func TestPausedSandboxConfigHTTPSPortsRoundTrip(t *testing.T) {
	t.Parallel()

	httpsPorts := []uint32{443, 8443}
	v, err := PausedSandboxConfig{
		Version: PausedSandboxConfigVersion,
		Network: &SandboxNetworkConfig{
			Ingress: &SandboxNetworkIngressConfig{HTTPSPorts: httpsPorts},
		},
	}.Value()
	require.NoError(t, err)

	raw, ok := v.(string)
	require.True(t, ok)

	var decoded PausedSandboxConfig
	require.NoError(t, json.Unmarshal([]byte(raw), &decoded))
	require.NotNil(t, decoded.Network)
	require.NotNil(t, decoded.Network.Ingress)
	assert.Equal(t, httpsPorts, decoded.Network.Ingress.HTTPSPorts)
}
