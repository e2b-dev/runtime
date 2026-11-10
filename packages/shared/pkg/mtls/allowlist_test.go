package mtls

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseAllowListAcceptsFullNamesAndTrailingWildcards(t *testing.T) {
	t.Parallel()

	list, err := ParseAllowList([]string{
		serverID,
		"  " + clientID + "  ",
		"",
		"spiffe://cluster-a.example.internal/ns/jobs/sa/worker-*",
		"spiffe://cluster-a.example.internal/ns/jobs/sa/*",
	})
	require.NoError(t, err)

	assert.Equal(t, 4, list.Len())
	assert.Equal(t, []string{
		serverID,
		clientID,
		"spiffe://cluster-a.example.internal/ns/jobs/sa/worker-*",
		"spiffe://cluster-a.example.internal/ns/jobs/sa/*",
	}, list.Entries())

	assert.True(t, list.Allows(serverID))
	assert.True(t, list.Allows(clientID))
	assert.True(t, list.Allows("spiffe://cluster-a.example.internal/ns/jobs/sa/worker-7"))
	assert.True(t, list.Allows("spiffe://cluster-a.example.internal/ns/jobs/sa/anything"))
	assert.False(t, list.Allows(strangerID))
	assert.False(t, list.Allows(otherDomain), "the same path under another trust domain is another name")
	assert.False(t, list.Allows("spiffe://cluster-a.example.internal/ns/other/sa/worker-7"), "a wildcard covers one namespace")
	assert.False(t, list.Allows(serverID+"/"), "a trailing slash is a different path")
	assert.False(t, list.Allows(""))
}

func TestParseAllowListCanonicalisesTheTrustDomainOnly(t *testing.T) {
	t.Parallel()

	list, err := ParseAllowList([]string{"spiffe://Cluster-A.Example.Internal/ns/platform/sa/Server"})
	require.NoError(t, err)

	assert.Equal(t, []string{"spiffe://cluster-a.example.internal/ns/platform/sa/Server"}, list.Entries())
	assert.True(t, list.Allows("spiffe://cluster-a.example.internal/ns/platform/sa/Server"))
	assert.False(t, list.Allows(serverID), "the path is case-sensitive")
}

func TestParseAllowListRejectsMalformedEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		entry string
	}{
		{name: "not a URI", entry: "server"},
		{name: "wrong scheme", entry: "https://cluster-a.example.internal/ns/platform/sa/server"},
		{name: "no trust domain", entry: "spiffe:///ns/platform/sa/server"},
		{name: "no path", entry: "spiffe://cluster-a.example.internal"},
		{name: "root path", entry: "spiffe://cluster-a.example.internal/"},
		{name: "query", entry: serverID + "?x=1"},
		{name: "fragment", entry: serverID + "#x"},
		{name: "user info", entry: "spiffe://user@cluster-a.example.internal/ns/platform/sa/server"},
		{name: "port", entry: "spiffe://cluster-a.example.internal:443/ns/platform/sa/server"},
		{name: "bare host colon", entry: "spiffe://cluster-a.example.internal:/ns/platform/sa/server"},
		{name: "bare query", entry: serverID + "?"},
		{name: "bare fragment", entry: serverID + "#"},
		{name: "percent-encoded path", entry: "spiffe://cluster-a.example.internal/ns/platform/sa/serv%65r"},
		{name: "wildcard in the middle", entry: "spiffe://cluster-a.example.internal/ns/*/sa/server"},
		{name: "wildcard in the namespace segment", entry: "spiffe://cluster-a.example.internal/ns/plat*"},
		{name: "wildcard outside ns/sa", entry: "spiffe://cluster-a.example.internal/workload-*"},
		{name: "wildcard trust domain", entry: "spiffe://*.example.internal/ns/platform/sa/server"},
		{name: "two wildcards", entry: "spiffe://cluster-a.example.internal/ns/platform/sa/*-*"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseAllowList([]string{tt.entry})
			require.ErrorIs(t, err, ErrInvalidAllowList)
		})
	}
}

func TestParseAllowListWithoutWildcards(t *testing.T) {
	t.Parallel()

	_, err := ParseAllowList([]string{"spiffe://cluster-a.example.internal/ns/jobs/sa/worker-*"}, WithoutWildcards())
	require.ErrorIs(t, err, ErrInvalidAllowList)

	list, err := ParseAllowList([]string{serverID}, WithoutWildcards())
	require.NoError(t, err)
	assert.True(t, list.Allows(serverID))

	require.ErrorIs(t, list.Replace([]string{"spiffe://cluster-a.example.internal/ns/jobs/sa/worker-*"}), ErrInvalidAllowList,
		"a list parsed without wildcards keeps refusing them on Replace")
	assert.True(t, list.Allows(serverID), "and keeps its entries")
}

func TestAllowListEmptyAndNilAdmitNobody(t *testing.T) {
	t.Parallel()

	empty, err := ParseAllowList(nil)
	require.NoError(t, err)
	assert.Equal(t, 0, empty.Len())
	assert.False(t, empty.Allows(serverID))

	var nilList *AllowList
	assert.Equal(t, 0, nilList.Len())
	assert.False(t, nilList.Allows(serverID))
	assert.Empty(t, nilList.Entries())
}

func TestAllowListReplaceKeepsTheLastGoodListOnError(t *testing.T) {
	t.Parallel()

	list, err := ParseAllowList([]string{serverID})
	require.NoError(t, err)

	require.ErrorIs(t, list.Replace([]string{"garbage"}), ErrInvalidAllowList)
	assert.True(t, list.Allows(serverID), "a bad list keeps the last good one")

	require.NoError(t, list.Replace([]string{clientID}))
	assert.False(t, list.Allows(serverID), "a removed name is gone")
	assert.True(t, list.Allows(clientID))
}

func TestAllowListWildcardStaysInTheServiceAccountSegment(t *testing.T) {
	t.Parallel()

	list, err := ParseAllowList([]string{
		"spiffe://cluster-a.example.internal/ns/jobs/sa/worker-*",
		"spiffe://cluster-a.example.internal/ns/jobs/sa/*",
	})
	require.NoError(t, err)

	assert.True(t, list.Allows("spiffe://cluster-a.example.internal/ns/jobs/sa/worker-7"))
	assert.False(t, list.Allows("spiffe://cluster-a.example.internal/ns/jobs/sa/worker-7/admin"),
		"a name that runs past the ServiceAccount segment is not a ServiceAccount")
}
