package cfg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
)

func setBaseEnv(t *testing.T) {
	t.Helper()

	t.Setenv("POSTGRES_CONNECTION_STRING", "postgres://example")
	t.Setenv("ADMIN_TOKEN", "admin-token")
	t.Setenv("REDIS_URL", "redis://example")
	t.Setenv("ORY_SDK_URL", "https://tenant.projects.oryapis.com")
	t.Setenv("ORY_PROJECT_API_TOKEN", "pat")
}

func TestParseAuthProviderConfig(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("AUTH_PROVIDER_CONFIG", `{
		"jwt": [
			{
				"issuer": {
					"url": "https://auth.example.com",
					"audiences": ["dashboard-api", "other"],
					"audienceMatchPolicy": "MatchAny"
				},
				"cacheDuration": "30m"
			}
		]
	}`)

	config, err := Parse()
	require.NoError(t, err)
	require.Len(t, config.AuthProvider.JWT, 1)

	entry := config.AuthProvider.JWT[0]
	require.Equal(t, "https://auth.example.com", entry.Issuer.URL)
	require.Equal(t, []string{"dashboard-api", "other"}, entry.Issuer.Audiences)
	require.Equal(t, sharedauth.AudienceMatchAny, entry.Issuer.AudienceMatchPolicy)
	require.Equal(t, 30*time.Minute, entry.CacheDuration)
}

func TestParseRequiresOryEnv(t *testing.T) {
	tests := []struct {
		name          string
		sdkURL        string
		token         string
		wantErrSubstr string
	}{
		{
			name:          "without sdk url errors",
			token:         "pat",
			wantErrSubstr: "ORY_SDK_URL",
		},
		{
			name:          "without token errors",
			sdkURL:        "https://tenant.projects.oryapis.com",
			wantErrSubstr: "ORY_PROJECT_API_TOKEN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("POSTGRES_CONNECTION_STRING", "postgres://example")
			t.Setenv("ADMIN_TOKEN", "admin-token")
			t.Setenv("REDIS_URL", "redis://example")
			t.Setenv("ORY_SDK_URL", tt.sdkURL)
			t.Setenv("ORY_PROJECT_API_TOKEN", tt.token)

			_, err := Parse()
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErrSubstr)
		})
	}
}

func TestParseOryHappyPath(t *testing.T) { //nolint:paralleltest // t.Setenv cannot be used with t.Parallel.
	setBaseEnv(t)

	config, err := Parse()
	require.NoError(t, err)
	require.Equal(t, "https://tenant.projects.oryapis.com", config.OrySDKURL)
	require.Equal(t, "pat", config.OryProjectAPIToken)
	require.Empty(t, config.AuthProvider.JWT)
}

func TestParseOryWithAuthProviderConfig(t *testing.T) {
	t.Setenv("POSTGRES_CONNECTION_STRING", "postgres://example")
	t.Setenv("ADMIN_TOKEN", "admin-token")
	t.Setenv("REDIS_URL", "redis://example")
	t.Setenv("ORY_SDK_URL", "https://tenant.projects.oryapis.com")
	t.Setenv("ORY_PROJECT_API_TOKEN", "pat")
	t.Setenv("AUTH_PROVIDER_CONFIG", `{
		"jwt": [
			{"issuer": {"url": "https://auth.mycompany.com", "audiences": ["dashboard-api"]}}
		]
	}`)

	config, err := Parse()
	require.NoError(t, err)
	require.Len(t, config.AuthProvider.JWT, 1)
	require.Equal(t, "https://auth.mycompany.com", config.AuthProvider.JWT[0].Issuer.URL)
}

func TestParseAdminAuthProviderConfig(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("ADMIN_AUTH_PROVIDER_CONFIG", `{"jwt":[{"issuer":{"url":"https://workspace.example.com","audiences":["fx1"]}}]}`)

	config, err := Parse()
	require.NoError(t, err)
	require.Len(t, config.AdminAuthProvider.JWT, 1)
	require.Equal(t, "https://workspace.example.com", config.AdminAuthProvider.JWT[0].Issuer.URL)
}

func TestParseFailureCondition(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want FailureCondition
	}{
		{
			name: "missing redis connection",
			env: map[string]string{
				"POSTGRES_CONNECTION_STRING": "postgres://example",
				"ADMIN_TOKEN":                "admin-token",
			},
			want: FailureConditionMissingRedisConnection,
		},
		{
			name: "missing ory sdk url",
			env: map[string]string{
				"POSTGRES_CONNECTION_STRING": "postgres://example",
				"ADMIN_TOKEN":                "admin-token",
				"REDIS_URL":                  "redis://example",
				"ORY_PROJECT_API_TOKEN":      "pat",
			},
			want: FailureConditionMissingOrySDKURL,
		},
		{
			name: "missing ory project token",
			env: map[string]string{
				"POSTGRES_CONNECTION_STRING": "postgres://example",
				"ADMIN_TOKEN":                "admin-token",
				"REDIS_URL":                  "redis://example",
				"ORY_SDK_URL":                "https://tenant.projects.oryapis.com",
			},
			want: FailureConditionMissingOryProjectToken,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for key, value := range tt.env {
				t.Setenv(key, value)
			}

			_, err := Parse()
			require.Error(t, err)

			got, ok := ParseFailureCondition(err)
			require.True(t, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseWithoutOryStartsWithoutIdentityProvider(t *testing.T) {
	t.Setenv("POSTGRES_CONNECTION_STRING", "postgres://example")
	t.Setenv("ADMIN_TOKEN", "admin-token")
	t.Setenv("REDIS_URL", "redis://example")
	t.Setenv("ORY_SDK_URL", "")
	t.Setenv("ORY_PROJECT_API_TOKEN", "")

	config, err := Parse()
	require.NoError(t, err)
	require.False(t, config.IdentityProviderConfigured())
}

func TestParseWithOryReportsIdentityProviderConfigured(t *testing.T) {
	t.Setenv("POSTGRES_CONNECTION_STRING", "postgres://example")
	t.Setenv("ADMIN_TOKEN", "admin-token")
	t.Setenv("REDIS_URL", "redis://example")
	t.Setenv("ORY_SDK_URL", "https://tenant.projects.oryapis.com")
	t.Setenv("ORY_PROJECT_API_TOKEN", "pat")

	config, err := Parse()
	require.NoError(t, err)
	require.True(t, config.IdentityProviderConfigured())
}
