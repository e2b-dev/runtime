package sandbox

import (
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

// A profile's /init attempts get at least its floor whatever the flag says
// below it; the default profile and a flag above the floor keep the flag.
func TestEnvdInitRequestTimeoutForRaisesAProfileToItsFloor(t *testing.T) {
	t.Parallel()

	factoryWith := func(t *testing.T, ms int) *Factory {
		t.Helper()
		td := ldtestdata.DataSource()
		td.Update(td.Flag(featureflags.EnvdInitTimeoutMilliseconds.Key()).ValueForAll(ldvalue.Int(ms)))
		ff, err := featureflags.NewClientWithDatasource(td)
		require.NoError(t, err)
		t.Cleanup(func() { _ = ff.Close(t.Context()) })

		return &Factory{featureFlags: ff}
	}
	linux := &Config{}
	floored := &Config{FirecrackerConfig: fc.Config{Guest: firmwareProfile{}}}

	low := factoryWith(t, 50)
	require.Equal(t, 50*time.Millisecond, low.envdInitRequestTimeoutFor(t.Context(), linux))
	require.Equal(t, firmwareProfile{}.EnvdInit().RequestTimeoutFloor, low.envdInitRequestTimeoutFor(t.Context(), floored))

	high := factoryWith(t, 5000)
	require.Equal(t, 5*time.Second, high.envdInitRequestTimeoutFor(t.Context(), linux))
	require.Equal(t, 5*time.Second, high.envdInitRequestTimeoutFor(t.Context(), floored))

	require.Equal(t, 50*time.Millisecond, low.envdInitRequestTimeoutFor(t.Context(), nil), "no config is the flag")
}
