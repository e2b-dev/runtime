package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/api/internal/db/pkg/types"
	"github.com/e2b-dev/infra/packages/api/internal/db/readmodel"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func TestSandboxNetworkConfigLocalhostForwarding(t *testing.T) {
	t.Parallel()

	t.Run("off is persisted and survives the stored form", func(t *testing.T) {
		t.Parallel()

		off := false
		got := sandboxNetworkConfig(&api.SandboxNetworkConfig{ExposeLocalhostPorts: &off}, dbtypes.SandboxSettings{})
		require.NotNil(t, got)
		require.NotNil(t, got.Ingress)

		stored, err := json.Marshal(got)
		require.NoError(t, err)
		var resumed dbtypes.SandboxNetworkConfig
		require.NoError(t, json.Unmarshal(stored, &resumed))
		require.NotNil(t, resumed.Ingress)
		require.NotNil(t, resumed.Ingress.ExposeLocalhostPorts, "a resume must read back what the create stored")
		assert.False(t, *resumed.Ingress.ExposeLocalhostPorts)
	})

	t.Run("absent stays absent", func(t *testing.T) {
		t.Parallel()

		got := sandboxNetworkConfig(&api.SandboxNetworkConfig{}, dbtypes.SandboxSettings{})
		require.NotNil(t, got)
		require.NotNil(t, got.Ingress)
		assert.Nil(t, got.Ingress.ExposeLocalhostPorts)
	})
}

func TestDBNetworkConfigToAPILocalhostForwarding(t *testing.T) {
	t.Parallel()

	off := false
	result := dbNetworkConfigToAPI(&dbtypes.SandboxNetworkConfig{
		Ingress: &dbtypes.SandboxNetworkIngressConfig{ExposeLocalhostPorts: &off},
	})

	require.NotNil(t, result)
	require.NotNil(t, result.ExposeLocalhostPorts)
	assert.False(t, *result.ExposeLocalhostPorts)
}

func TestGateLocalhostForwarding(t *testing.T) {
	t.Parallel()

	off, on := false, true
	withForwarding := func(v *bool) *dbtypes.SandboxNetworkConfig {
		return &dbtypes.SandboxNetworkConfig{Ingress: &dbtypes.SandboxNetworkIngressConfig{ExposeLocalhostPorts: v}}
	}

	require.Nil(t, gateLocalhostForwarding(withForwarding(&off), true), "enabled keeps an asked-for off")
	require.Nil(t, gateLocalhostForwarding(nil, false))

	apiErr := gateLocalhostForwarding(withForwarding(&off), false)
	require.NotNil(t, apiErr, "a disabled control refuses off rather than store a request a node may ignore")
	assert.Equal(t, http.StatusBadRequest, apiErr.Code)

	dropped := withForwarding(&on)
	require.Nil(t, gateLocalhostForwarding(dropped, false))
	assert.Nil(t, dropped.Ingress.ExposeLocalhostPorts, "a disabled control sends nothing new")
}

func TestSandboxNetworkConfigProjectLocalhostForwarding(t *testing.T) {
	t.Parallel()

	// An API binary that predates the field caches the team without it, which forwards.
	var oldCachedTeam authtypes.Team
	require.NoError(t, json.Unmarshal([]byte(`{"ID":"`+uuid.NewString()+`","Slug":"team"}`), &oldCachedTeam))
	require.Nil(t, oldCachedTeam.ProjectSettings.SandboxSettings.DefaultExposeLocalhostPorts)

	off, on := false, true
	forwarding := func(n *dbtypes.SandboxNetworkConfig) *bool {
		if n == nil || n.Ingress == nil {
			return nil
		}

		return n.Ingress.ExposeLocalhostPorts
	}

	for name, tc := range map[string]struct {
		request        *api.SandboxNetworkConfig
		projectDefault *bool
		public         *bool
		want           *bool
		wantNil        bool
	}{
		"no network, no project default keeps the legacy nil network": {wantNil: true},
		"no network, project off":                                     {projectDefault: &off, want: &off},
		"no network, project off, private":                            {projectDefault: &off, public: &off, want: &off},
		"no network, project on":                                      {projectDefault: &on, want: &on},
		"network without the field, project off":                      {request: &api.SandboxNetworkConfig{}, projectDefault: &off, want: &off},
		"request on overrides project off":                            {request: &api.SandboxNetworkConfig{ExposeLocalhostPorts: &on}, projectDefault: &off, want: &on},
		"request off without project default":                         {request: &api.SandboxNetworkConfig{ExposeLocalhostPorts: &off}, want: &off},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			settings := dbtypes.SandboxSettings{DefaultAllowPublicTraffic: tc.public, DefaultExposeLocalhostPorts: tc.projectDefault}
			got := sandboxNetworkConfig(tc.request, settings)
			if tc.wantNil {
				assert.Nil(t, got)

				return
			}
			require.NotNil(t, forwarding(got))
			assert.Equal(t, *tc.want, *forwarding(got))
			if tc.projectDefault != nil && tc.request == nil || tc.request != nil && tc.request.ExposeLocalhostPorts == nil {
				assert.NotSame(t, tc.projectDefault, forwarding(got), "the sandbox must not share the cached team's pointer")
			}
			if tc.public != nil && !*tc.public {
				require.NotNil(t, got.Ingress.AllowPublicAccess)
				assert.False(t, *got.Ingress.AllowPublicAccess, "the private project default still applies")
			}
		})
	}
}

// A delivered default reaches sandbox creation through the cached team, and a later
// revision without it clears it.
func TestProjectSettingsApplyLocalhostForwarding(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, _ := newProjectSettingsStore(db)

	for _, tc := range []struct {
		revision int64
		settings string
		want     *bool
	}{
		{1, `{"sandboxSettings":{"defaultAllowPublicTraffic":true,"defaultExposeLocalhostPorts":false}}`, new(false)},
		{2, `{"sandboxSettings":{"defaultAllowPublicTraffic":true}}`, nil},
	} {
		response := callProjectSettings(t, store, teamID, fmt.Sprintf(`{"revision":%d,"settings":%s}`, tc.revision, tc.settings))
		require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())

		row, err := db.AuthDB.GetTeamByTeamID(t.Context(), teamID)
		require.NoError(t, err)
		cached, err := json.Marshal(readmodel.NewTeam(row))
		require.NoError(t, err)
		var team authtypes.Team
		require.NoError(t, json.Unmarshal(cached, &team))
		assert.Equal(t, tc.want, team.ProjectSettings.SandboxSettings.DefaultExposeLocalhostPorts)
		assert.Equal(t, new(true), team.ProjectSettings.SandboxSettings.DefaultAllowPublicTraffic, "the other setting is kept")
	}
}

func TestSandboxSettingsForDropsADefaultThatCannotApply(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		control     bool
		envdVersion string
		applies     bool
	}{
		"control on, envd that applies it":  {control: true, envdVersion: sharedUtils.MinEnvdVersionForExposeLocalhostPorts, applies: true},
		"control on, older envd":            {control: true, envdVersion: "0.11.1"},
		"control on, no recorded envd":      {control: true, envdVersion: ""},
		"control on, unparsable envd":       {control: true, envdVersion: "not-a-version"},
		"control off, envd that applies it": {control: false, envdVersion: sharedUtils.MinEnvdVersionForExposeLocalhostPorts},
		"control off, older envd":           {control: false, envdVersion: "0.11.1"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			off := false
			settings := sandboxSettingsFor(dbtypes.SandboxSettings{DefaultExposeLocalhostPorts: &off}, test.control, test.envdVersion)

			if !test.applies {
				assert.Nil(t, settings.DefaultExposeLocalhostPorts, "a saved off that cannot apply must not reach the sandbox")
				assert.Nil(t, sandboxNetworkConfig(nil, settings),
					"a public create stores no network, so reads report the ports exposed and placement is unrestricted")

				return
			}
			require.NotNil(t, settings.DefaultExposeLocalhostPorts)
			assert.False(t, *settings.DefaultExposeLocalhostPorts)
			network := sandboxNetworkConfig(nil, settings)
			require.NotNil(t, network)
			require.NotNil(t, network.Ingress)
			require.NotNil(t, network.Ingress.ExposeLocalhostPorts)
			assert.False(t, *network.Ingress.ExposeLocalhostPorts)
		})
	}
}

func TestRequireEnvdForExposeLocalhostPortsOff(t *testing.T) {
	t.Parallel()

	off, on := false, true
	tests := map[string]struct {
		requested   *api.SandboxNetworkConfig
		envdVersion string
		refused     bool
	}{
		"off on an envd that applies it":               {requested: &api.SandboxNetworkConfig{ExposeLocalhostPorts: &off}, envdVersion: sharedUtils.MinEnvdVersionForExposeLocalhostPorts},
		"off on an older envd is refused":              {requested: &api.SandboxNetworkConfig{ExposeLocalhostPorts: &off}, envdVersion: "0.11.1", refused: true},
		"off on a build without an envd version":       {requested: &api.SandboxNetworkConfig{ExposeLocalhostPorts: &off}, envdVersion: "", refused: true},
		"on on an older envd carries on":               {requested: &api.SandboxNetworkConfig{ExposeLocalhostPorts: &on}, envdVersion: "0.11.1"},
		"nothing asked on an older envd carries on":    {requested: &api.SandboxNetworkConfig{}, envdVersion: "0.11.1"},
		"no network block on an older envd carries on": {requested: nil, envdVersion: "0.11.1"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			apiErr := requireEnvdForExposeLocalhostPortsOff(test.requested, test.envdVersion)
			if !test.refused {
				assert.Nil(t, apiErr)

				return
			}
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Code)
			assert.Contains(t, apiErr.ClientMsg, "template must be rebuilt")
		})
	}
}
