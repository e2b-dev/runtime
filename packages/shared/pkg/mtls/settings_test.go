package mtls

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnvPrefixFollowsTheChartsKeyRule(t *testing.T) {
	t.Parallel()

	for key, want := range map[string]string{
		"http":          "E2B_TLS_HTTP_",
		"internal_grpc": "E2B_TLS_INTERNAL_GRPC_",
		"secrets-store": "E2B_TLS_SECRETS_STORE_",
		"peers":         "E2B_TLS_PEERS_",
	} {
		assert.Equal(t, want, EnvPrefix(key), key)
	}
}

func TestListenerSettingsFromEnvReadsTheChartsVariables(t *testing.T) { //nolint:paralleltest // cannot call t.Setenv and t.Parallel
	t.Setenv("E2B_TLS_INTERNAL_GRPC_MODE", "permissive")
	t.Setenv("E2B_TLS_INTERNAL_GRPC_ALLOW", clientID+", ,"+serverID)
	t.Setenv("E2B_TLS_INTERNAL_GRPC_HEALTH_PATHS", "/health,/ready")

	assert.Equal(t, ListenerSettings{
		Mode:        "permissive",
		Allow:       []string{clientID, serverID},
		HealthPaths: []string{"/health", "/ready"},
	}, ListenerSettingsFromEnv("internal_grpc"))
}

func TestHopSettingsFromEnvReadsTheChartsVariables(t *testing.T) { //nolint:paralleltest // cannot call t.Setenv and t.Parallel
	t.Setenv("E2B_TLS_PEERS_CLIENT_MODE", "on")
	t.Setenv("E2B_TLS_PEERS_EXPECT", serverID)
	t.Setenv("E2B_TLS_PEERS_SERVER_NAME", " "+serverDNS+" ")

	assert.Equal(t, HopSettings{Mode: "on", Expect: []string{serverID}, ServerName: serverDNS}, HopSettingsFromEnv("peers"))
}

func TestSettingsFromEnvAreEmptyWhenNothingIsSet(t *testing.T) {
	t.Parallel()

	assert.Equal(t, ListenerSettings{}, ListenerSettingsFromEnv("never_rendered"))
	assert.Equal(t, HopSettings{}, HopSettingsFromEnv("never_rendered"))
}

// An env-struct configuration reads the settings through their tags and a
// service without one through the FromEnv readers; both must read the
// names the chart renders.
func TestSettingsTagsNameTheVariablesTheReadersRead(t *testing.T) { //nolint:paralleltest // cannot call t.Setenv and t.Parallel
	prefix := EnvPrefix("tagged")
	for _, typ := range []reflect.Type{reflect.TypeFor[ListenerSettings](), reflect.TypeFor[HopSettings]()} {
		for field := range typ.Fields() {
			t.Setenv(prefix+field.Tag.Get("env"), "set")
		}
	}

	for _, read := range []any{ListenerSettingsFromEnv("tagged"), HopSettingsFromEnv("tagged")} {
		value := reflect.ValueOf(read)
		for i := range value.NumField() {
			assert.False(t, value.Field(i).IsZero(), "%s.%s is not read from the variable its tag names", value.Type().Name(), value.Type().Field(i).Name)
		}
	}
}
