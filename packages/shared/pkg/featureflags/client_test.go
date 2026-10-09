package featureflags

import (
	"context"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	ldclient "github.com/launchdarkly/go-server-sdk/v7"
	"github.com/launchdarkly/go-server-sdk/v7/interfaces"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	flagName = "demo-feature-flag"
)

// Not parallel: it updates launchDarklyOfflineStore, which parallel tests read
// through NewClient and NewClientWithDatasource.
//
//nolint:paralleltest
func TestOfflineDatastore(t *testing.T) {
	clientCtx := ldcontext.NewBuilder(flagName).Build()
	client, err := NewClient("", "", "")
	require.NoError(t, err)

	t.Cleanup(func() {
		err = client.Close(context.WithoutCancel(t.Context()))
		assert.NoError(t, err)
	})

	// value is not set so it should be default (false)
	flagValue, _ := client.ld.BoolVariation(flagName, clientCtx, false)
	assert.False(t, flagValue)

	launchDarklyOfflineStore.Update(
		launchDarklyOfflineStore.Flag(flagName).VariationForAll(true),
	)
	// Restore so the package survives -count=N reruns in one process.
	t.Cleanup(func() {
		launchDarklyOfflineStore.Update(
			launchDarklyOfflineStore.Flag(flagName).VariationForAll(false),
		)
	})

	// value is set manually in datastore and should be taken from there
	flagValue, _ = client.ld.BoolVariation(flagName, clientCtx, false)
	assert.True(t, flagValue)
}

func TestAllContextsIncludesServiceAndDeployment(t *testing.T) {
	t.Parallel()

	client := newClient(nil, true, "staging", "orchestration-api")

	merged := mergeContexts(t.Context(), client.allContexts(t.Context(), nil))
	contexts := merged.GetAllIndividualContexts(nil)

	seen := map[ldcontext.Kind]string{}
	for _, item := range contexts {
		seen[item.Kind()] = item.Key()
	}

	require.Equal(t, "staging", seen[DeploymentEnvironmentKind])
	require.Equal(t, "orchestration-api", seen[ServiceKind])
}

// A combined orchestrator host evaluates some flags once per service it runs,
// so the service a caller names must win over the process-wide one.
func TestAllContextsCallerServiceOverridesProcessService(t *testing.T) {
	t.Parallel()

	client := newClient(nil, true, "staging", "orchestrator_template-manager")

	merged := mergeContexts(t.Context(), client.allContexts(t.Context(), []ldcontext.Context{ServiceContext("orchestrator")}))

	seen := map[ldcontext.Kind]string{}
	for _, item := range merged.GetAllIndividualContexts(nil) {
		seen[item.Kind()] = item.Key()
	}

	require.Equal(t, "orchestrator", seen[ServiceKind])
	require.Equal(t, "staging", seen[DeploymentEnvironmentKind])
}

// newClient is exercised directly: a real client on launchDarklyOfflineStore
// would race with the updates in TestOfflineDatastore.
func TestNewClientPlaceKey(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		value string
		want  string
	}{
		{name: "set", value: "staging", want: "staging"},
		{name: "empty is undefined", value: "", want: undefinedDeploymentEnvironment},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newClient(nil, true, tc.value, "")

			merged := mergeContexts(t.Context(), client.allContexts(t.Context(), nil))
			seen := map[ldcontext.Kind]string{}
			for _, item := range merged.GetAllIndividualContexts(nil) {
				seen[item.Kind()] = item.Key()
			}

			require.Equal(t, tc.want, seen[DeploymentEnvironmentKind])
			require.NotContains(t, seen, ServiceKind)
		})
	}
}

func TestApplicationInfo(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		environment string
		service     string
		version     string
		want        interfaces.ApplicationInfo
	}{
		{
			name: "service and environment", environment: "e2b-staging", service: "orchestration-api", version: "0.14.1",
			want: interfaces.ApplicationInfo{ApplicationID: "orchestration-api-e2b-staging", ApplicationVersion: "0.14.1"},
		},
		{
			name: "empty environment is undefined", environment: "", service: "orchestration-api", version: "0.14.1",
			want: interfaces.ApplicationInfo{ApplicationID: "orchestration-api-" + undefinedDeploymentEnvironment, ApplicationVersion: "0.14.1"},
		},
		{
			name: "empty service sends nothing", environment: "e2b-staging", service: "", version: "0.14.1",
			want: interfaces.ApplicationInfo{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.want, applicationInfo(tc.environment, tc.service, tc.version))
		})
	}
}

func TestAllContextsIncludesRegisteredProviders(t *testing.T) {
	t.Parallel()

	client := &Client{}
	client.RegisterContextProvider(func(context.Context) ldcontext.Context {
		return ldcontext.NewWithKind("node", "node-1")
	})

	merged := mergeContexts(t.Context(), client.allContexts(t.Context(), nil))
	contexts := merged.GetAllIndividualContexts(nil)

	seen := map[ldcontext.Kind]string{}
	for _, item := range contexts {
		seen[item.Kind()] = item.Key()
	}

	require.Equal(t, "node-1", seen["node"])
}

func TestClickhouseAsyncOfflineFallback(t *testing.T) {
	t.Parallel()
	client, err := NewClientWithDatasource(launchDarklyOfflineStore)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close(context.WithoutCancel(t.Context()))) })
	_, ok := client.BoolFlagOverride(t.Context(), ClickhouseAsyncInsertFlag, BatcherContext("sandbox-events"))
	require.False(t, ok, "offline defaults must not override writer-specific async behavior")
	require.True(t, client.BoolFlag(t.Context(), ClickhouseAsyncInsertFlag))
	require.True(t, client.BoolFlag(t.Context(), ClickhouseWaitForAsyncInsertFlag))
}

func TestIntFlagOverride(t *testing.T) {
	t.Parallel()

	const fallback = -1

	tests := []struct {
		name       string
		serve      *ldvalue.Value
		wantValue  int
		wantServed bool
	}{
		{name: "served value", serve: new(ldvalue.Int(25)), wantValue: 25, wantServed: true},
		{name: "served value equal to the fallback", serve: new(ldvalue.Int(fallback)), wantValue: fallback, wantServed: true},
		{name: "key the environment does not define", serve: nil, wantValue: fallback, wantServed: true},
		{name: "wrong type", serve: new(ldvalue.String("25")), wantValue: fallback, wantServed: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source := ldtestdata.DataSource()
			flag := IntFlag{name: "int-flag-override-test", fallback: fallback}
			if tt.serve != nil {
				source.Update(source.Flag(flag.Key()).ValueForAll(*tt.serve))
			}

			client, err := NewClientWithDatasource(source)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Close(context.WithoutCancel(t.Context()))) })

			value, served := client.IntFlagOverride(t.Context(), flag)
			assert.Equal(t, tt.wantValue, value)
			assert.Equal(t, tt.wantServed, served)
		})
	}
}

func TestIntFlagOverrideNilClient(t *testing.T) {
	t.Parallel()

	value, served := (&Client{}).IntFlagOverride(t.Context(), IntFlag{name: "int-flag-override-test", fallback: 7})
	assert.Equal(t, 7, value)
	assert.False(t, served)
}

func TestIntFlagWithError(t *testing.T) {
	t.Parallel()
	flag := IntFlag{name: "checked-int-flag", fallback: -1}
	for _, tc := range []struct {
		name    string
		value   *ldvalue.Value
		want    int
		wantErr bool
	}{
		{"missing flag", nil, -1, false},
		{"configured value", new(ldvalue.Int(25)), 25, false},
		{"configured fallback", new(ldvalue.Int(-1)), -1, false},
		{"zero is a served value", new(ldvalue.Int(0)), 0, false},
		{"large integer", new(ldvalue.Int(1 << 34)), 1 << 34, false},
		{"whole number", new(ldvalue.Float64(2.0)), 2, false},
		{"fraction", new(ldvalue.Float64(1.5)), 0, true},
		{"wrong type", new(ldvalue.String("25")), 0, true},
		{"null", new(ldvalue.Null()), 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := ldtestdata.DataSource()
			if tc.value != nil {
				source.Update(source.Flag(flag.Key()).ValueForAll(*tc.value))
			}
			client, err := NewClientWithDatasource(source)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Close(context.WithoutCancel(t.Context()))) })
			value, err := client.IntFlagWithError(t.Context(), flag)
			if tc.wantErr {
				require.ErrorContains(t, err, flag.Key())
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, value)
		})
	}
}

func TestIntFlagWithErrorUnavailableClient(t *testing.T) {
	t.Parallel()
	flag := IntFlag{name: "checked-int-flag", fallback: 7}
	value, err := (&Client{}).IntFlagWithError(t.Context(), flag)
	require.NoError(t, err)
	require.Equal(t, 7, value)

	ld, err := ldclient.MakeCustomClient("", ldclient.Config{Offline: true}, 0)
	require.NoError(t, err)
	client := &Client{ld: ld}
	t.Cleanup(func() { require.NoError(t, client.Close(context.WithoutCancel(t.Context()))) })
	value, err = client.IntFlagWithError(t.Context(), flag)
	require.NoError(t, err)
	require.Equal(t, 7, value)
}
