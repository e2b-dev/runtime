package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
)

func TestNewTeamPreservesAPIRates(t *testing.T) {
	t.Parallel()

	team := NewTeam(authqueries.GetTeamByTeamIDRow{ApiTeamRpsList: 1 << 32, ApiTeamRpsDelete: 1<<32 + 1})
	assert.Equal(t, int64(1<<32), team.Limits.APITeamRPSList)
	assert.Equal(t, int64(1<<32+1), team.Limits.APITeamRPSDelete)
}

func TestNewTeamPreservesTheSecretLimit(t *testing.T) {
	t.Parallel()

	team := NewTeam(authqueries.GetTeamByTeamIDRow{MaxSecrets: 1 << 32})
	assert.Equal(t, int64(1<<32), team.Limits.MaxSecrets)
}

// Pods of adjacent releases share the cached teams. An entry written while the
// team still carried its tier must decode whole, and the entry written back
// must hold the same keys without it. An entry written before the member role
// was cached decodes with none.
func TestTeamKeepsTheCachedShape(t *testing.T) {
	t.Parallel()

	const cached = `{
		"ID": "0b5e7f0c-6f3a-4a59-9c0e-2f1f7a1d2c01", "CreatedAt": "2026-01-02T03:04:05Z",
		"IsBlocked": true, "Name": "Acme", "Tier": "base_v1", "Email": "ops@acme.test",
		"IsBanned": true, "BlockedReason": "unpaid", "ClusterID": "5d3c2b1a-0f9e-4d8c-8b7a-6f5e4d3c2b1a",
		"SandboxSchedulingLabels": ["gpu"], "SsoOrganizationID": "1a2b3c4d-5e6f-4a8b-9c0d-1e2f3a4b5c6d",
		"SsoAutoJoin": true, "DeletedAt": "2026-02-03T04:05:06Z", "Slug": "acme",
		"Limits": {
			"SandboxConcurrency": 1, "BuildConcurrency": 2, "MaxLengthHours": 3, "MaxVcpu": 4,
			"MaxRamMb": 5, "DiskMb": 6, "EventsTTLDays": 7, "DefaultFreeDiskSizeMb": 8,
			"MaxFreeDiskSizeMb": 9, "APITeamRPSList": 10, "APITeamRPSDelete": 11, "MaxSecrets": 12
		},
		"ProjectSettings": {"SandboxSettings": {"DefaultAllowPublicTraffic": false}}
	}`

	var team *Team
	require.NoError(t, json.Unmarshal([]byte(cached), &team)) //nolint:musttag // The cache keys are the untagged field names.

	written, err := json.Marshal(team) //nolint:musttag // The cache keys are the untagged field names.
	require.NoError(t, err)

	var want map[string]any
	require.NoError(t, json.Unmarshal([]byte(cached), &want))
	delete(want, "Tier")
	want["Role"] = ""
	wantJSON, err := json.Marshal(want)
	require.NoError(t, err)
	assert.JSONEq(t, string(wantJSON), string(written))
}
