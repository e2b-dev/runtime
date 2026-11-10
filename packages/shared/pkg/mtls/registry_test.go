package mtls

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Two open connections sharing a remote address cannot be told apart by a
// lookup, so neither may be answered with the other's verdict.
func TestRegistryTreatsASharedRemoteAddressAsUnverified(t *testing.T) {
	t.Parallel()

	first, firstPeer := net.Pipe()
	second, secondPeer := net.Pipe()
	t.Cleanup(func() {
		for _, c := range []net.Conn{first, firstPeer, second, secondPeer} {
			_ = c.Close()
		}
	})
	key := remoteKey(first)
	require.Equal(t, key, remoteKey(second), "pipe connections share an address")

	r := newConnRegistry()
	verified := ConnState{TLS: true, ChainVerified: true}
	assert.True(t, r.add(key, first, verified, false))
	state, known := r.lookup(key)
	require.True(t, known)
	assert.True(t, state.ChainVerified)

	assert.False(t, r.add(key, second, verified, false), "a second live connection on the key")
	state, known = r.lookup(key)
	require.True(t, known)
	assert.Equal(t, ConnState{}, state, "a second live connection on the key leaves neither verdict attributable")

	assert.False(t, r.add("", first, verified, false), "an address-less connection cannot be attributed either")
	state, known = r.lookup("")
	require.True(t, known)
	assert.Equal(t, ConnState{}, state)
}
