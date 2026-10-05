package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	handlersmocks "github.com/e2b-dev/infra/packages/api/internal/handlers/mocks"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	sandbox_network "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-network"
)

func TestBuildAutoResumeConfig(t *testing.T) {
	t.Parallel()

	configPtr := func(v bool) *api.SandboxAutoResumeConfig {
		return &api.SandboxAutoResumeConfig{Enabled: v}
	}

	tests := []struct {
		name       string
		in         *api.SandboxAutoResumeConfig
		wantNil    bool
		wantPolicy dbtypes.SandboxAutoResumePolicy
	}{
		{
			name:    "nil config returns nil",
			in:      nil,
			wantNil: true,
		},
		{
			name:       "true maps to any policy",
			in:         configPtr(true),
			wantPolicy: dbtypes.SandboxAutoResumeAny,
		},
		{
			name:       "false maps to off policy",
			in:         configPtr(false),
			wantPolicy: dbtypes.SandboxAutoResumeOff,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := buildAutoResumeConfig(tt.in)

			if tt.wantNil {
				if got != nil {
					t.Fatalf("buildAutoResumeConfig() = %#v, want nil", got)
				}

				return
			}

			if got == nil {
				t.Fatalf("buildAutoResumeConfig() = nil, want non-nil config")
			}

			if got.Policy != tt.wantPolicy {
				t.Fatalf("buildAutoResumeConfig().Policy = %v, want %v", got.Policy, tt.wantPolicy)
			}
		})
	}
}

func TestBuildSandboxIam(t *testing.T) {
	t.Parallel()

	tokens := func(defs api.SandboxIamTokens) *api.SandboxIam { return &api.SandboxIam{Tokens: &defs} }
	valid := api.SandboxIamToken{Audience: "sts.amazonaws.com", TokenType: "JWT-SVID"}

	tooMany := api.SandboxIamTokens{}
	for i := range maxIamTokens + 1 {
		tooMany[fmt.Sprintf("t%d", i)] = valid
	}

	tests := []struct {
		name    string
		iam     *api.SandboxIam
		want    *dbtypes.SandboxIam
		wantErr bool
	}{
		{name: "nil iam is disabled", iam: nil},
		{name: "nil tokens is disabled", iam: &api.SandboxIam{}},
		{name: "empty tokens is disabled", iam: tokens(api.SandboxIamTokens{})},
		{
			name: "valid token is preserved exactly",
			iam:  tokens(api.SandboxIamTokens{"aws": valid}),
			want: &dbtypes.SandboxIam{Tokens: map[string]dbtypes.SandboxIamToken{"aws": {Audience: "sts.amazonaws.com", TokenType: "JWT-SVID"}}},
		},
		{
			name:    "empty name is rejected",
			iam:     tokens(api.SandboxIamTokens{"": valid}),
			wantErr: true,
		},
		{
			name:    "empty audience is rejected",
			iam:     tokens(api.SandboxIamTokens{"aws": {Audience: "", TokenType: "JWT-SVID"}}),
			wantErr: true,
		},
		{
			name:    "unsupported token type is rejected",
			iam:     tokens(api.SandboxIamTokens{"aws": {Audience: "sts.amazonaws.com", TokenType: "OIDC"}}),
			wantErr: true,
		},
		{
			name:    "too many tokens is rejected",
			iam:     tokens(tooMany),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, apiErr := buildSandboxIam(tt.iam)
			if tt.wantErr {
				require.NotNil(t, apiErr)
				assert.Equal(t, http.StatusBadRequest, apiErr.Code)
				assert.Nil(t, got)

				return
			}

			require.Nil(t, apiErr)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValidateNetworkConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		network    *api.SandboxNetworkConfig
		wantErr    bool
		wantCode   int
		wantErrMsg string
	}{
		{
			name:    "nil network config is valid",
			network: nil,
			wantErr: false,
		},
		{
			name:    "empty network config is valid",
			network: &api.SandboxNetworkConfig{},
			wantErr: false,
		},
		{
			name: "valid HTTPS ports",
			network: &api.SandboxNetworkConfig{
				HttpsPorts: &[]uint32{443, 8443},
			},
			wantErr: false,
		},
		{
			name: "too many HTTPS ports",
			network: &api.SandboxNetworkConfig{
				HttpsPorts: func() *[]uint32 {
					ports := make([]uint32, maxHTTPSPorts+1)
					for i := range ports {
						ports[i] = uint32(i + 1)
					}

					return &ports
				}(),
			},
			wantErr:    true,
			wantCode:   http.StatusBadRequest,
			wantErrMsg: fmt.Sprintf("HTTPS ports can have at most %d entries.", maxHTTPSPorts),
		},
		{
			name: "duplicate HTTPS port is invalid",
			network: &api.SandboxNetworkConfig{
				HttpsPorts: &[]uint32{443, 443},
			},
			wantErr:    true,
			wantCode:   http.StatusBadRequest,
			wantErrMsg: "HTTPS port 443 is specified more than once.",
		},
		{
			name: "zero HTTPS port is invalid",
			network: &api.SandboxNetworkConfig{
				HttpsPorts: &[]uint32{0},
			},
			wantErr:    true,
			wantCode:   http.StatusBadRequest,
			wantErrMsg: "HTTPS port must be between 1 and 65535: 0",
		},
		{
			name: "out-of-range HTTPS port is invalid",
			network: &api.SandboxNetworkConfig{
				HttpsPorts: &[]uint32{65536},
			},
			wantErr:    true,
			wantCode:   http.StatusBadRequest,
			wantErrMsg: "HTTPS port must be between 1 and 65535: 65536",
		},
		{
			name: "envd HTTPS port is invalid",
			network: &api.SandboxNetworkConfig{
				HttpsPorts: &[]uint32{uint32(consts.DefaultEnvdServerPort)},
			},
			wantErr:    true,
			wantCode:   http.StatusBadRequest,
			wantErrMsg: fmt.Sprintf("HTTPS backend routing is not supported for reserved port %d", consts.DefaultEnvdServerPort),
		},
		{
			name: "valid deny_out with CIDR",
			network: &api.SandboxNetworkConfig{
				DenyOut: &[]string{"10.0.0.0/8"},
			},
			wantErr: false,
		},
		{
			name: "invalid deny_out entry",
			network: &api.SandboxNetworkConfig{
				DenyOut: &[]string{"not-a-cidr"},
			},
			wantErr:    true,
			wantCode:   http.StatusBadRequest,
			wantErrMsg: "invalid denied CIDR not-a-cidr",
		},
		// Domain validation tests
		{
			name: "allow_out with domain requires deny_out block-all",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"example.com"},
			},
			wantErr:    true,
			wantCode:   http.StatusBadRequest,
			wantErrMsg: ErrMsgDomainsRequireBlockAll,
		},
		{
			name: "allow_out with domain and block-all deny_out is valid",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"example.com"},
				DenyOut:  &[]string{sandbox_network.AllInternetTrafficCIDR},
			},
			wantErr: false,
		},
		{
			name: "allow_out with domain and partial deny_out is invalid",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"example.com"},
				DenyOut:  &[]string{"10.0.0.0/8"},
			},
			wantErr:    true,
			wantCode:   http.StatusBadRequest,
			wantErrMsg: ErrMsgDomainsRequireBlockAll,
		},
		{
			name: "allow_out with wildcard domain requires deny_out block-all",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"*.example.com"},
			},
			wantErr:    true,
			wantCode:   http.StatusBadRequest,
			wantErrMsg: ErrMsgDomainsRequireBlockAll,
		},
		{
			name: "allow_out with wildcard domain and block-all deny_out is valid",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"*.example.com"},
				DenyOut:  &[]string{sandbox_network.AllInternetTrafficCIDR},
			},
			wantErr: false,
		},
		// CIDR validation tests
		{
			name: "allow_out with CIDR without deny_out is valid",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"10.0.0.0/8"},
			},
			wantErr: false,
		},
		{
			name: "allow_out with CIDR and deny_out block-all is valid",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"10.0.0.0/8"},
				DenyOut:  &[]string{sandbox_network.AllInternetTrafficCIDR},
			},
			wantErr: false,
		},
		{
			name: "allow_out with IP without deny_out is valid",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"8.8.8.8"},
			},
			wantErr: false,
		},
		{
			name: "allow_out with IP and deny_out block-all is valid",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"8.8.8.8"},
				DenyOut:  &[]string{sandbox_network.AllInternetTrafficCIDR},
			},
			wantErr: false,
		},
		// CIDR intersection validation tests
		{
			name: "allow_out CIDR not covered by deny_out CIDR is valid (no intersection check)",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"10.0.0.0/8"},
				DenyOut:  &[]string{"192.168.0.0/16"}, // No intersection, but still valid
			},
			wantErr: false,
		},
		{
			name: "allow_out CIDR covered by intersecting deny_out CIDR is valid",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"10.1.0.0/16"},
				DenyOut:  &[]string{"10.0.0.0/8"}, // Deny covers allow
			},
			wantErr: false,
		},
		{
			name: "allow_out CIDR covers deny_out CIDR is valid (intersection exists)",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"10.0.0.0/8"},
				DenyOut:  &[]string{"10.1.0.0/16"}, // Allow covers deny - still valid intersection
			},
			wantErr: false,
		},
		{
			name: "allow_out IP covered by deny_out CIDR is valid",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"10.1.2.3"},
				DenyOut:  &[]string{"10.0.0.0/8"},
			},
			wantErr: false,
		},
		{
			name: "allow_out IP not covered by deny_out CIDR is valid (no intersection check)",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"8.8.8.8"},
				DenyOut:  &[]string{"10.0.0.0/8"},
			},
			wantErr: false,
		},
		{
			name: "multiple allow_out CIDRs partial deny_out coverage is valid (no intersection check)",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"10.0.0.0/8", "192.168.0.0/16"},
				DenyOut:  &[]string{"10.0.0.0/8"}, // Only covers first, but still valid
			},
			wantErr: false,
		},
		{
			name: "multiple allow_out CIDRs covered by multiple deny_out CIDRs is valid",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"10.0.0.0/8", "192.168.0.0/16"},
				DenyOut:  &[]string{"10.0.0.0/8", "192.168.0.0/16"},
			},
			wantErr: false,
		},
		// Mixed domain and CIDR tests
		{
			name: "allow_out with domain and CIDR without deny_out block-all is invalid",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"example.com", "8.8.8.8"},
				DenyOut:  &[]string{"10.0.0.0/8"},
			},
			wantErr:    true,
			wantCode:   http.StatusBadRequest,
			wantErrMsg: ErrMsgDomainsRequireBlockAll,
		},
		{
			name: "allow_out with domain and CIDR with deny_out block-all is valid",
			network: &api.SandboxNetworkConfig{
				AllowOut: &[]string{"example.com", "8.8.8.8"},
				DenyOut:  &[]string{sandbox_network.AllInternetTrafficCIDR},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mockFF := handlersmocks.NewMockFeatureFlagsClient(t)
			err := validateNetworkConfig(t.Context(), mockFF, uuid.Nil, "", 10, tt.network)

			if tt.wantErr {
				if err == nil {
					t.Errorf("validateNetworkConfig() expected error, got nil")

					return
				}

				if err.Code != tt.wantCode {
					t.Errorf("validateNetworkConfig() error code = %v, want %v", err.Code, tt.wantCode)
				}

				if err.ClientMsg != tt.wantErrMsg {
					t.Errorf("validateNetworkConfig() error message = %q, want %q", err.ClientMsg, tt.wantErrMsg)
				}
			} else if err != nil {
				t.Errorf("validateNetworkConfig() unexpected error: %v", err)
			}
		})
	}
}

func TestDBNetworkConfigToAPIHTTPSPorts(t *testing.T) {
	t.Parallel()

	httpsPorts := []uint32{443, 8443}
	result := dbNetworkConfigToAPI(&dbtypes.SandboxNetworkConfig{
		Ingress: &dbtypes.SandboxNetworkIngressConfig{HTTPSPorts: httpsPorts},
	})

	require.NotNil(t, result)
	require.NotNil(t, result.HttpsPorts)
	assert.Equal(t, httpsPorts, *result.HttpsPorts)
}

func TestOrchestrator_convertVolumeMounts(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)

	t.Run("InvalidVolumeMountsError.Error() returns expected string", func(t *testing.T) {
		t.Parallel()

		err := InvalidVolumeMountsError{[]InvalidMount{
			{0, "reason1"},
			{2, "reason2"},
		}}
		expected := "invalid mounts:\n\t- volume mount #0: reason1\n\t- volume mount #2: reason2"
		assert.Equal(t, expected, err.Error())
	})

	testCases := map[string]struct {
		expectFeatureFlag bool
		expectResources   bool
		volumesEnabled    bool
		input             []api.SandboxVolumeMount
		database          []queries.CreateVolumeParams
		volumeTypes       []string
		expected          []*orchestrator.SandboxVolumeMount
		err               error
	}{
		"missing volume reports correct error": {
			expectFeatureFlag: true,
			expectResources:   true,
			volumesEnabled:    true,
			input: []api.SandboxVolumeMount{
				{Name: "vol1"},
			},
			volumeTypes: []string{},
			err:         InvalidVolumeMountsError{[]InvalidMount{{0, "volume 'vol1' not found"}}},
		},
		"partial success returns error": {
			expectFeatureFlag: true,
			expectResources:   true,
			volumesEnabled:    true,
			input: []api.SandboxVolumeMount{
				{Name: "vol1", Path: "/vol1"},
				{Name: "vol2", Path: "/vol2"},
			},
			database: []queries.CreateVolumeParams{
				{Name: "vol1", VolumeType: "local"},
			},
			volumeTypes: []string{"local"},
			err:         InvalidVolumeMountsError{[]InvalidMount{{1, "volume 'vol2' not found"}}},
		},
		"empty volume mounts": {
			input:    []api.SandboxVolumeMount{},
			expected: []*orchestrator.SandboxVolumeMount{},
		},
		"feature flag disabled": {
			expectFeatureFlag: true,
			volumesEnabled:    false,
			input: []api.SandboxVolumeMount{
				{Name: "vol1", Path: "/vol1"},
			},
			err: ErrVolumeMountsDisabled,
		},
		"empty path reports error": {
			expectFeatureFlag: true,
			expectResources:   true,
			volumesEnabled:    true,
			input: []api.SandboxVolumeMount{
				{Name: "vol1", Path: ""},
			},
			database: []queries.CreateVolumeParams{
				{Name: "vol1", VolumeType: "local"},
			},
			volumeTypes: []string{"local"},
			err:         InvalidVolumeMountsError{[]InvalidMount{{0, "path cannot be empty"}}},
		},
		"non-absolute path reports error": {
			expectFeatureFlag: true,
			expectResources:   true,
			volumesEnabled:    true,
			input: []api.SandboxVolumeMount{
				{Name: "vol1", Path: "relative/path"},
			},
			database: []queries.CreateVolumeParams{
				{Name: "vol1", VolumeType: "local"},
			},
			volumeTypes: []string{"local"},
			err:         InvalidVolumeMountsError{[]InvalidMount{{0, "path must be absolute"}}},
		},
		"non-clean path reports error": {
			expectFeatureFlag: true,
			expectResources:   true,
			volumesEnabled:    true,
			input: []api.SandboxVolumeMount{
				{Name: "vol1", Path: "/path/./to/somewhere"},
			},
			database: []queries.CreateVolumeParams{
				{Name: "vol1", VolumeType: "local"},
			},
			volumeTypes: []string{"local"},
			err:         InvalidVolumeMountsError{[]InvalidMount{{0, "path must not contain any '.' or '..' components"}}},
		},
		"duplicate paths report error": {
			expectFeatureFlag: true,
			expectResources:   true,
			volumesEnabled:    true,
			input: []api.SandboxVolumeMount{
				{Name: "vol1", Path: "/path"},
				{Name: "vol2", Path: "/path"},
			},
			database: []queries.CreateVolumeParams{
				{Name: "vol1", VolumeType: "local"},
				{Name: "vol2", VolumeType: "local"},
			},
			volumeTypes: []string{"local"},
			err:         InvalidVolumeMountsError{[]InvalidMount{{1, "path '/path' is already used"}}},
		},
		"multiple invalid mounts report all errors": {
			expectFeatureFlag: true,
			expectResources:   true,
			volumesEnabled:    true,
			input: []api.SandboxVolumeMount{
				{Name: "missing", Path: "/path1"},
				{Name: "vol1", Path: "relative"},
				{Name: "vol2", Path: "/path2"},
				{Name: "vol3", Path: "/path2"},
			},
			database: []queries.CreateVolumeParams{
				{Name: "vol1", VolumeType: "local"},
				{Name: "vol2", VolumeType: "local"},
				{Name: "vol3", VolumeType: "local"},
			},
			volumeTypes: []string{"local"},
			err: InvalidVolumeMountsError{[]InvalidMount{
				{0, "volume 'missing' not found"},
				{1, "path must be absolute"},
				{3, "path '/path2' is already used"},
			}},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			teamID := testutils.CreateTestTeam(t, db)

			for _, v := range tc.database {
				_, err := db.SqlcClient.CreateVolume(t.Context(),
					queries.CreateVolumeParams{
						ID:         uuid.New(),
						Name:       v.Name,
						TeamID:     teamID,
						VolumeType: v.VolumeType,
					},
				)
				require.NoError(t, err)
			}

			ffClient := handlersmocks.NewMockFeatureFlagsClient(t)
			if tc.expectFeatureFlag {
				ffClient.EXPECT().
					BoolFlag(mock.Anything, mock.Anything).
					Return(tc.volumesEnabled)
			}

			actual, err := convertAPIVolumesToOrchestratorVolumes(
				t.Context(), db.SqlcClient, ffClient,
				teamID, tc.input, &queries.EnvBuild{EnvdVersion: new(minEnvdVersionForVolumes)},
			)
			assert.Equal(t, tc.err, err)
			assert.Equal(t, tc.expected, actual)
		})
	}

	t.Run("existing volumes are returned", func(t *testing.T) {
		t.Parallel()

		teamID := testutils.CreateTestTeam(t, db)

		dbVolume, err := db.SqlcClient.CreateVolume(t.Context(),
			queries.CreateVolumeParams{
				ID:         uuid.New(),
				Name:       "vol1",
				TeamID:     teamID,
				VolumeType: "local",
			},
		)
		require.NoError(t, err)

		ffClient := handlersmocks.NewMockFeatureFlagsClient(t)
		ffClient.EXPECT().
			BoolFlag(mock.Anything, mock.Anything).
			Return(true)

		actual, err := convertAPIVolumesToOrchestratorVolumes(
			t.Context(), db.SqlcClient, ffClient,
			teamID, []api.SandboxVolumeMount{
				{Name: "vol1", Path: "/vol1"},
			}, &queries.EnvBuild{EnvdVersion: new(minEnvdVersionForVolumes)},
		)
		require.NoError(t, err)
		assert.Equal(t, []*orchestrator.SandboxVolumeMount{
			{Id: dbVolume.ID.String(), Name: "vol1", Path: "/vol1", Type: "local"},
		}, actual)
	})
}

func TestNewSandboxFromV2(t *testing.T) {
	t.Parallel()

	t.Run("v2 request has every NewSandbox field except secure", func(t *testing.T) {
		t.Parallel()

		v1Fields := map[string]reflect.Type{}
		for f := range reflect.TypeFor[api.NewSandbox]().Fields() {
			v1Fields[f.Name] = f.Type
		}
		delete(v1Fields, "Secure")

		v2Fields := map[string]reflect.Type{}
		for f := range reflect.TypeFor[api.NewSandboxV2]().Fields() {
			v2Fields[f.Name] = f.Type
		}

		assert.Equal(t, v1Fields, v2Fields)
	})

	t.Run("forces secure and copies every field", func(t *testing.T) {
		t.Parallel()

		timeout := int32(60)
		autoPause := true
		autoPauseMemory := false
		allowInternet := false
		allowPublicTraffic := false

		v2 := api.NewSandboxV2{
			TemplateID:          "base",
			Timeout:             &timeout,
			AutoPause:           &autoPause,
			AutoPauseMemory:     &autoPauseMemory,
			AutoResume:          &api.SandboxAutoResumeConfig{Enabled: true},
			AllowInternetAccess: &allowInternet,
			Network:             &api.SandboxNetworkConfig{AllowPublicTraffic: &allowPublicTraffic},
			Metadata:            &api.SandboxMetadata{"k": "v"},
			EnvVars:             &api.EnvVars{"A": "1"},
			Mcp:                 &api.Mcp{},
			Iam:                 &api.SandboxIam{},
			VolumeMounts:        &[]api.SandboxVolumeMount{{Name: "vol", Path: "/vol"}},
		}

		got := newSandboxFromV2(v2)

		require.NotNil(t, got.Secure)
		assert.True(t, *got.Secure)

		got.Secure = nil
		var want api.NewSandbox
		raw, err := json.Marshal(v2)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, &want))
		assert.Equal(t, want, got)
	})
}

func TestPostSandboxes_MissingTagDisclosure(t *testing.T) {
	t.Parallel()

	t.Run("public template returns tag-specific not found", func(t *testing.T) {
		t.Parallel()
		assertMissingTagDisclosure(t, true, "public-missing-tag")
	})

	t.Run("private template stays generic not found", func(t *testing.T) {
		t.Parallel()
		assertMissingTagDisclosure(t, false, "private-missing-tag")
	})

	t.Run("public template without default tag names default", func(t *testing.T) {
		t.Parallel()
		assertMissingDefaultTagDisclosure(t)
	})
}

func TestPostSandboxes_MissingBareAliasUsesPromotedFallbackKey(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	requesterTeamID := testutils.CreateTestTeam(t, db)
	requesterTeamSlug := testutils.GetTeamSlug(t, ctx, db, requesterTeamID)

	store := &APIStore{
		templateCache: templatecache.NewTemplateCache(db.SqlcClient, redis),
	}
	defer func() {
		require.NoError(t, store.templateCache.Close(ctx))
	}()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)

	body, err := json.Marshal(api.PostSandboxesJSONRequestBody{TemplateID: "desktop"})
	require.NoError(t, err)

	ginCtx.Request = httptest.NewRequestWithContext(ctx, http.MethodPost, "/sandboxes", bytes.NewReader(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
		Team: &authqueries.Team{
			ID:   requesterTeamID,
			Slug: requesterTeamSlug,
		},
		Limits: &authtypes.TeamLimits{MaxLengthHours: 24},
	})

	store.PostSandboxes(ginCtx)

	require.Equal(t, http.StatusNotFound, recorder.Code)

	var apiErr api.Error
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &apiErr))

	assert.Equal(t, int32(http.StatusNotFound), apiErr.Code)
	assert.Equal(t, "template 'desktop' not found", apiErr.Message)
}

// Valid tag exists on a private template owned by another team. A non-owner
// requester must receive the same generic 404 used for missing tags so access
// denials don't leak template existence.
func TestPostSandboxes_PrivateTemplateHidesAccessDenied(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	ownerTeamID := testutils.CreateTestTeam(t, db)
	ownerTeamSlug := testutils.GetTeamSlug(t, ctx, db, ownerTeamID)
	requesterTeamID := testutils.CreateTestTeam(t, db)
	requesterTeamSlug := testutils.GetTeamSlug(t, ctx, db, requesterTeamID)

	store := &APIStore{
		templateCache: templatecache.NewTemplateCache(db.SqlcClient, redis),
	}
	defer func() {
		require.NoError(t, store.templateCache.Close(ctx))
	}()

	alias := "private-valid-tag"
	tag := "v2"

	templateID := createTestTemplate(ctx, t, db, ownerTeamID)
	setTemplatePublic(ctx, t, db, templateID, false)
	createTestTemplateAliasWithName(ctx, t, db, templateID, alias, &ownerTeamSlug)

	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "ready")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, tag)

	templateRef := id.WithTag(id.WithNamespace(ownerTeamSlug, alias), tag)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)

	body, err := json.Marshal(api.PostSandboxesJSONRequestBody{TemplateID: templateRef})
	require.NoError(t, err)

	ginCtx.Request = httptest.NewRequestWithContext(ctx, http.MethodPost, "/sandboxes", bytes.NewReader(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
		Team: &authqueries.Team{
			ID:   requesterTeamID,
			Slug: requesterTeamSlug,
		},
		Limits: &authtypes.TeamLimits{MaxLengthHours: 24},
	})

	store.PostSandboxes(ginCtx)

	require.Equal(t, http.StatusNotFound, recorder.Code)

	var apiErr api.Error
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &apiErr))

	assert.Equal(t, int32(http.StatusNotFound), apiErr.Code)
	assert.Equal(t, fmt.Sprintf("template '%s' not found", id.WithNamespace(ownerTeamSlug, alias)), apiErr.Message)
}

func assertMissingTagDisclosure(t *testing.T, public bool, alias string) {
	t.Helper()

	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	ownerTeamID := testutils.CreateTestTeam(t, db)
	ownerTeamSlug := testutils.GetTeamSlug(t, ctx, db, ownerTeamID)
	requesterTeamID := testutils.CreateTestTeam(t, db)
	requesterTeamSlug := testutils.GetTeamSlug(t, ctx, db, requesterTeamID)

	store := &APIStore{
		templateCache: templatecache.NewTemplateCache(db.SqlcClient, redis),
	}
	defer func() {
		require.NoError(t, store.templateCache.Close(ctx))
	}()

	templateID := createTestTemplate(ctx, t, db, ownerTeamID)
	setTemplatePublic(ctx, t, db, templateID, public)
	createTestTemplateAliasWithName(ctx, t, db, templateID, alias, &ownerTeamSlug)

	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "ready")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, id.DefaultTag)

	templateRef := id.WithTag(id.WithNamespace(ownerTeamSlug, alias), "v2")
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)

	body, err := json.Marshal(api.PostSandboxesJSONRequestBody{TemplateID: templateRef})
	require.NoError(t, err)

	ginCtx.Request = httptest.NewRequestWithContext(ctx, http.MethodPost, "/sandboxes", bytes.NewReader(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
		Team: &authqueries.Team{
			ID:   requesterTeamID,
			Slug: requesterTeamSlug,
		},
		Limits: &authtypes.TeamLimits{MaxLengthHours: 24},
	})

	store.PostSandboxes(ginCtx)

	require.Equal(t, http.StatusNotFound, recorder.Code)

	var apiErr api.Error
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &apiErr))

	var wantMessage string
	if public {
		wantMessage = fmt.Sprintf("tag 'v2' does not exist for template '%s'", id.WithNamespace(ownerTeamSlug, alias))
	} else {
		wantMessage = fmt.Sprintf("template '%s' not found", id.WithNamespace(ownerTeamSlug, alias))
	}

	assert.Equal(t, int32(http.StatusNotFound), apiErr.Code)
	assert.Equal(t, wantMessage, apiErr.Message)
}

func assertMissingDefaultTagDisclosure(t *testing.T) {
	t.Helper()

	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	ownerTeamID := testutils.CreateTestTeam(t, db)
	ownerTeamSlug := testutils.GetTeamSlug(t, ctx, db, ownerTeamID)
	requesterTeamID := testutils.CreateTestTeam(t, db)
	requesterTeamSlug := testutils.GetTeamSlug(t, ctx, db, requesterTeamID)

	store := &APIStore{
		templateCache: templatecache.NewTemplateCache(db.SqlcClient, redis),
	}
	defer func() {
		require.NoError(t, store.templateCache.Close(ctx))
	}()

	alias := "public-missing-default-tag"
	templateID := createTestTemplate(ctx, t, db, ownerTeamID)
	createTestTemplateAliasWithName(ctx, t, db, templateID, alias, &ownerTeamSlug)

	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "ready")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, "dev")

	templateRef := id.WithNamespace(ownerTeamSlug, alias)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)

	body, err := json.Marshal(api.PostSandboxesJSONRequestBody{TemplateID: templateRef})
	require.NoError(t, err)

	ginCtx.Request = httptest.NewRequestWithContext(ctx, http.MethodPost, "/sandboxes", bytes.NewReader(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")
	auth.SetTeamInfoForTest(t, ginCtx, &authtypes.Team{
		Team: &authqueries.Team{
			ID:   requesterTeamID,
			Slug: requesterTeamSlug,
		},
		Limits: &authtypes.TeamLimits{MaxLengthHours: 24},
	})

	store.PostSandboxes(ginCtx)

	require.Equal(t, http.StatusNotFound, recorder.Code)

	var apiErr api.Error
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &apiErr))

	assert.Equal(t, int32(http.StatusNotFound), apiErr.Code)
	assert.Equal(t, fmt.Sprintf("tag 'default' does not exist for template '%s'", templateRef), apiErr.Message)
}

func setTemplatePublic(ctx context.Context, t *testing.T, db *testutils.Database, templateID string, public bool) {
	t.Helper()

	err := db.SqlcClient.TestsRawSQL(
		ctx,
		"UPDATE public.envs SET public = $2 WHERE id = $1",
		templateID,
		public,
	)
	require.NoError(t, err)
}

func createTestTemplate(ctx context.Context, t *testing.T, db *testutils.Database, teamID uuid.UUID) string {
	t.Helper()

	templateID := "base-env-" + uuid.New().String()

	err := db.SqlcClient.TestsRawSQL(
		ctx,
		"INSERT INTO public.envs (id, team_id, public, updated_at, source) VALUES ($1, $2, $3, NOW(), 'template')",
		templateID,
		teamID,
		true,
	)
	require.NoError(t, err)

	return templateID
}

func ffEnabled(t *testing.T) *handlersmocks.MockFeatureFlagsClient {
	t.Helper()
	ff := handlersmocks.NewMockFeatureFlagsClient(t)
	ff.EXPECT().BoolFlag(mock.Anything, mock.Anything, mock.Anything).Return(true)

	return ff
}

func ffDisabled(t *testing.T) *handlersmocks.MockFeatureFlagsClient {
	t.Helper()
	ff := handlersmocks.NewMockFeatureFlagsClient(t)
	ff.EXPECT().BoolFlag(mock.Anything, mock.Anything, mock.Anything).Return(false)

	return ff
}

func ffUnused(t *testing.T) *handlersmocks.MockFeatureFlagsClient {
	t.Helper()

	return handlersmocks.NewMockFeatureFlagsClient(t) // no expectations — must not be called
}

func simpleRule(headers map[string]string) api.SandboxNetworkRule {
	h := headers

	return api.SandboxNetworkRule{
		Transform: &api.SandboxNetworkTransform{Headers: &h},
	}
}

// markerRun concatenates count unique secret markers starting at index from.
func markerRun(from, count int) string {
	var builder strings.Builder
	for i := from; i < from+count; i++ {
		fmt.Fprintf(&builder, "${e2b.secrets.name-%d}", i)
	}

	return builder.String()
}

func TestAPIRulesToDBRulesNormalizesDomains(t *testing.T) {
	t.Parallel()

	rules := map[string][]api.SandboxNetworkRule{
		"API.Example.COM": {simpleRule(map[string]string{"X-Exact": "value"})},
		"*.GitHub.COM":    {simpleRule(map[string]string{"X-Test": "value"})},
	}

	dbRules := apiRulesToDBRules(&rules)
	require.NotContains(t, dbRules, "API.Example.COM")
	require.Contains(t, dbRules, "api.example.com")
	require.Len(t, dbRules["api.example.com"], 1)
	assert.Equal(t, "value", dbRules["api.example.com"][0].Transform.Headers["X-Exact"])
	require.NotContains(t, dbRules, "*.GitHub.COM")
	require.Contains(t, dbRules, "*.github.com")
	require.Len(t, dbRules["*.github.com"], 1)
	assert.Equal(t, "value", dbRules["*.github.com"][0].Transform.Headers["X-Test"])
}

func TestValidateNetworkRules(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	const defaultMaxDomains = 10

	rulesWithDomains := func(count int) *map[string][]api.SandboxNetworkRule {
		rules := make(map[string][]api.SandboxNetworkRule, count)
		for i := range count {
			rules[fmt.Sprintf("domain%d.example.com", i)] = nil
		}

		return &rules
	}

	tests := []struct {
		name        string
		envdVersion string
		rules       *map[string][]api.SandboxNetworkRule
		setupFF     func(t *testing.T) *handlersmocks.MockFeatureFlagsClient
		wantCode    int
		wantMsg     string // substring of ClientMsg; empty means expect no error
		notWant     []string
	}{
		// ── nil / empty ──────────────────────────────────────────────────────────
		{
			name:    "nil rules are valid",
			rules:   nil,
			setupFF: ffUnused,
		},
		{
			name:        "empty rules map is valid",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{}),
			setupFF:     ffEnabled,
		},
		// ── feature flag ─────────────────────────────────────────────────────────
		{
			name:     "feature flag disabled returns 400",
			rules:    new(map[string][]api.SandboxNetworkRule{"api.openai.com": {}}),
			setupFF:  ffDisabled,
			wantCode: http.StatusBadRequest,
			wantMsg:  "not available for your team",
		},
		// ── envd version ─────────────────────────────────────────────────────────
		{
			name:     "missing envd version returns 400",
			rules:    new(map[string][]api.SandboxNetworkRule{"api.openai.com": {}}),
			setupFF:  ffEnabled,
			wantCode: http.StatusBadRequest,
			wantMsg:  "template must be rebuilt: envd version is not set",
		},
		{
			name:        "envd version below minimum returns 400",
			envdVersion: "0.5.12",
			rules:       new(map[string][]api.SandboxNetworkRule{"api.openai.com": {}}),
			setupFF:     ffEnabled,
			wantCode:    http.StatusBadRequest,
			wantMsg:     "template must be rebuilt",
		},
		{
			name:        "envd version at minimum is valid",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"api.openai.com": {}}),
			setupFF:     ffEnabled,
		},
		// ── domain count ─────────────────────────────────────────────────────────
		{
			name:        "exactly max domains is valid",
			rules:       rulesWithDomains(defaultMaxDomains),
			envdVersion: minEnvdVersionForNetworkRules,
			setupFF:     ffEnabled,
		},
		{
			name:        "one over max domains returns 400",
			rules:       rulesWithDomains(defaultMaxDomains + 1),
			envdVersion: minEnvdVersionForNetworkRules,
			setupFF:     ffEnabled,
			wantCode:    http.StatusBadRequest,
			wantMsg:     fmt.Sprintf("at most %d domains", defaultMaxDomains),
		},
		// ── domain key validation ─────────────────────────────────────────────────
		{
			name:        "empty domain key returns 400",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"": {}}),
			setupFF:     ffEnabled,
			wantCode:    http.StatusBadRequest,
			wantMsg:     "must not be empty",
		},
		{
			name:        "domain exceeding max length returns 400",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{strings.Repeat("a", maxNetworkRuleDomainLen+1): {}}),
			setupFF:     ffEnabled,
			wantCode:    http.StatusBadRequest,
			wantMsg:     "maximum length",
		},
		{
			name:        "invalid domain returns 400",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"not a valid domain!": {}}),
			setupFF:     ffEnabled,
			wantCode:    http.StatusBadRequest,
			wantMsg:     "not a valid domain name",
		},
		{
			name:        "valid plain domain is accepted",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"api.openai.com": {}}),
			setupFF:     ffEnabled,
		},
		{
			name:        "wildcard domain is accepted",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"*.github.com": {}}),
			setupFF:     ffEnabled,
		},
		{
			name:        "uppercase wildcard domain is accepted",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"*.GitHub.COM": {}}),
			setupFF:     ffEnabled,
		},
		{
			name:        "exact and wildcard domains are accepted together",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"api.github.com": {},
				"*.github.com":   {},
			}),
			setupFF: ffEnabled,
		},
		{
			name:        "top-level wildcard is accepted",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"*.com": {}}),
			setupFF:     ffEnabled,
		},
		{
			name:        "country-code public suffix wildcard is accepted",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"*.co.uk": {}}),
			setupFF:     ffEnabled,
		},
		{
			name:        "private public suffix wildcard is accepted",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"*.github.io": {}}),
			setupFF:     ffEnabled,
		},
		{
			name:        "shared service wildcard is accepted",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"*.s3.amazonaws.com": {}}),
			setupFF:     ffEnabled,
		},
		{
			name:        "internal wildcard is accepted",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"*.internal": {}}),
			setupFF:     ffEnabled,
		},
		{
			name:        "bare wildcard is rejected",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"*": {}}),
			setupFF:     ffEnabled,
			wantCode:    http.StatusBadRequest,
			wantMsg:     "not a valid domain name",
		},
		{
			name:        "wildcard without a suffix is rejected",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"*.": {}}),
			setupFF:     ffEnabled,
			wantCode:    http.StatusBadRequest,
			wantMsg:     "not a valid domain name",
		},
		{
			name:        "embedded wildcard is rejected",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"api.*.example.com": {}}),
			setupFF:     ffEnabled,
			wantCode:    http.StatusBadRequest,
			wantMsg:     "not a valid domain name",
		},
		{
			name:        "repeated wildcard is rejected",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"*.*.example.com": {}}),
			setupFF:     ffEnabled,
			wantCode:    http.StatusBadRequest,
			wantMsg:     "not a valid domain name",
		},
		{
			name:        "non-ASCII wildcard is rejected",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"*.münich.example": {}}),
			setupFF:     ffEnabled,
			wantCode:    http.StatusBadRequest,
			wantMsg:     "not a valid domain name",
		},
		{
			name:        "trailing-dot wildcard is rejected",
			envdVersion: minEnvdVersionForNetworkRules,
			rules:       new(map[string][]api.SandboxNetworkRule{"*.github.com.": {}}),
			setupFF:     ffEnabled,
			wantCode:    http.StatusBadRequest,
			wantMsg:     "not a valid domain name",
		},
		{
			name:        "case-insensitive exact duplicates are rejected",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"api.example.com": {},
				"API.EXAMPLE.COM": {},
			}),
			setupFF:  ffEnabled,
			wantCode: http.StatusBadRequest,
			wantMsg:  "unique ignoring case",
		},
		{
			name:        "case-insensitive wildcard duplicates are rejected",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"*.example.com": {},
				"*.EXAMPLE.COM": {},
			}),
			setupFF:  ffEnabled,
			wantCode: http.StatusBadRequest,
			wantMsg:  "unique ignoring case",
		},
		{
			name:        "wildcard domain at full key limit is accepted",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"*." + strings.Repeat("a", 63) + "." + strings.Repeat("b", 58) + ".com": {},
			}),
			setupFF: ffEnabled,
		},
		{
			name:        "wildcard domain over full key limit is rejected",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"*." + strings.Repeat("a", 63) + "." + strings.Repeat("b", 59) + ".com": {},
			}),
			setupFF:  ffEnabled,
			wantCode: http.StatusBadRequest,
			wantMsg:  "maximum length",
		},
		// ── transform count ───────────────────────────────────────────────────────
		{
			name:        "one transform per domain is valid",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"api.openai.com": {simpleRule(map[string]string{"Authorization": "Bearer token"})},
			}),
			setupFF: ffEnabled,
		},
		{
			name:        "two transforms for one domain returns 400",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"api.openai.com": {
					simpleRule(map[string]string{"Authorization": "Bearer token"}),
					simpleRule(map[string]string{"X-Custom": "value"}),
				},
			}),
			setupFF:  ffEnabled,
			wantCode: http.StatusBadRequest,
			wantMsg:  fmt.Sprintf("at most %d transform rule", maxNetworkRuleTransformsPerDomain),
		},
		// ── secret marker count ────────────────────────────────────────────────────
		{
			name:        "thirty-two unique secret markers in one domain is valid",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"api.openai.com": {simpleRule(map[string]string{"X-Secrets": markerRun(0, 32)})},
			}),
			setupFF: ffEnabled,
		},
		{
			name:        "thirty-three unique secret markers returns 400",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"api.openai.com": {simpleRule(map[string]string{"X-Secrets": markerRun(0, 33)})},
			}),
			setupFF:  ffEnabled,
			wantCode: http.StatusBadRequest,
			wantMsg:  "references more than 32 secrets",
		},
		{
			name:        "case-variant marker spellings count once",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"api.openai.com": {simpleRule(map[string]string{
					"X-Secrets": markerRun(0, 32) + "${e2b.secrets.NAME-0}",
				})},
			}),
			setupFF: ffEnabled,
		},
		// ── nil transform (no headers to check) ───────────────────────────────────
		{
			name:        "nil transform in rule is valid",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"api.openai.com": {{Transform: nil}},
			}),
			setupFF: ffEnabled,
		},
		// ── header name ───────────────────────────────────────────────────────────
		{
			name:        "empty header name returns 400",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"api.openai.com": {simpleRule(map[string]string{"": "value"})},
			}),
			setupFF:  ffEnabled,
			wantCode: http.StatusBadRequest,
			wantMsg:  "must not be empty",
		},
		{
			name:        "header name at max length is valid",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"api.openai.com": {simpleRule(map[string]string{
					strings.Repeat("X", maxNetworkRuleHeaderNameLen): "value",
				})},
			}),
			setupFF: ffEnabled,
		},
		{
			name:        "header name exceeding max length returns 400",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"api.openai.com": {simpleRule(map[string]string{
					strings.Repeat("X", maxNetworkRuleHeaderNameLen+1): "value",
				})},
			}),
			setupFF:  ffEnabled,
			wantCode: http.StatusBadRequest,
			wantMsg:  "maximum length",
		},
		// ── header value ──────────────────────────────────────────────────────────
		{
			name:        "header value at max length is valid",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"api.openai.com": {simpleRule(map[string]string{
					"Authorization": strings.Repeat("x", maxNetworkRuleHeaderValueLen),
				})},
			}),
			setupFF: ffEnabled,
		},
		{
			name:        "header value exceeding max length returns 400",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"api.openai.com": {simpleRule(map[string]string{
					"Authorization": strings.Repeat("x", maxNetworkRuleHeaderValueLen+1),
				})},
			}),
			setupFF:  ffEnabled,
			wantCode: http.StatusBadRequest,
			wantMsg:  "maximum length",
		},
		{
			name:        "malformed known placeholder returns generic 400",
			envdVersion: minEnvdVersionForNetworkRules,
			rules: new(map[string][]api.SandboxNetworkRule{
				"api.openai.com": {simpleRule(map[string]string{
					"Authorization": "Bearer ${e2b.secrets.private-canary",
				})},
			}),
			setupFF:  ffEnabled,
			wantCode: http.StatusBadRequest,
			wantMsg:  "Network transform header contains a malformed E2B placeholder.",
			notWant:  []string{"${e2b.secrets.", "private-canary"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ff := tt.setupFF(t)
			apiErr := validateNetworkRules(t.Context(), ff, teamID, tt.envdVersion, defaultMaxDomains, tt.rules)

			if tt.wantMsg == "" {
				assert.Nil(t, apiErr)

				return
			}

			if assert.NotNil(t, apiErr) {
				assert.Equal(t, tt.wantCode, apiErr.Code)
				assert.Contains(t, apiErr.ClientMsg, tt.wantMsg)
				for _, forbidden := range tt.notWant {
					assert.NotContains(t, apiErr.ClientMsg, forbidden)
					assert.NotContains(t, apiErr.Err.Error(), forbidden)
				}
			}
		})
	}

	assert.Nil(t, validateNetworkRules(t.Context(), ffEnabled(t), teamID, minEnvdVersionForNetworkRules, defaultMaxDomains+1, rulesWithDomains(defaultMaxDomains+1)), "custom max domains should override the boundary")
}

func createTestTemplateAliasWithName(ctx context.Context, t *testing.T, db *testutils.Database, templateID, aliasName string, namespace *string) {
	t.Helper()

	err := db.SqlcClient.TestsRawSQL(
		ctx,
		"INSERT INTO public.env_aliases (alias, env_id, is_renamable, namespace) VALUES ($1, $2, $3, $4)",
		aliasName,
		templateID,
		true,
		namespace,
	)
	require.NoError(t, err)
}
