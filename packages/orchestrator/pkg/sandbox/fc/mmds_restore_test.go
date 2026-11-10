//go:build linux

package fc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A restore's metadata put tolerates the 400 naming an uninitialised store only
// under a profile that declares the store optional, and remembers it for the
// pause; any other 400 fails, and a store-bearing restore records nothing.
func TestSetRestoredMmdsRemembersAMissingStore(t *testing.T) {
	t.Parallel()

	meta := &MmdsMetadata{SandboxID: "sbx", TemplateID: "tmpl", LogsCollectorAddress: "http://169.254.0.22/logs"}
	const missing = `{"fault_message":"The MMDS data store is not initialized."}`

	client, rec := newWireRecorder(t)
	rec.refuseMmds(missing)
	optional := &Process{Versions: Config{Guest: firmwareProfile{}}, client: client}
	require.NoError(t, optional.setRestoredMmds(t.Context(), meta))
	assert.True(t, optional.MmdsStoreMissing())

	client, rec = newWireRecorder(t)
	rec.refuseMmds(missing)
	linux := &Process{client: client}
	require.Error(t, linux.setRestoredMmds(t.Context(), meta), "the default profile requires the store")
	assert.False(t, linux.MmdsStoreMissing())

	client, rec = newWireRecorder(t)
	rec.refuseMmds(`{"fault_message":"The MMDS data store cannot exceed 51200 bytes."}`)
	other := &Process{Versions: Config{Guest: firmwareProfile{}}, client: client}
	require.Error(t, other.setRestoredMmds(t.Context(), meta))
	assert.False(t, other.MmdsStoreMissing())

	client, rec = newWireRecorder(t)
	stored := &Process{Versions: Config{Guest: firmwareProfile{}}, client: client}
	require.NoError(t, stored.setRestoredMmds(t.Context(), meta))
	assert.False(t, stored.MmdsStoreMissing())
	assert.Contains(t, rec.body(t, "PUT /mmds"), `"instanceID":"sbx"`, "the put reaches a store-bearing microVM")

	assert.False(t, (*Process)(nil).MmdsStoreMissing())
	assert.False(t, mmdsStoreMissing(nil))
}
