package orchestrator

import (
	"context"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk-evaluation/v3/ldmodel"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// newWriteFormTestOrchestrator builds the minimal Orchestrator the write form
// update needs: a flag client on source and an unstarted store.
func newWriteFormTestOrchestrator(t *testing.T, source *ldtestdata.TestDataSource) *Orchestrator {
	t.Helper()

	storage, err := sandboxredis.NewStorage(redis_utils.SetupInstance(t), noop.NewMeterProvider())
	require.NoError(t, err)
	ff, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ff.Close(context.WithoutCancel(t.Context()))) })

	return &Orchestrator{featureFlagsClient: ff, redisStorage: storage}
}

func setPerSandboxKeysFlag(source *ldtestdata.TestDataSource, value ldvalue.Value) {
	source.Update(source.Flag(featureflags.SandboxStorePerSandboxKeysFlag.Key()).ValueForAll(value))
}

func TestWriteFormFollowsFlag(t *testing.T) {
	t.Parallel()

	source := ldtestdata.DataSource()
	o := newWriteFormTestOrchestrator(t, source)

	setPerSandboxKeysFlag(source, ldvalue.Bool(true))
	o.updateStoreWriteForm(t.Context())
	require.True(t, o.redisStorage.WritesPerSandbox())

	setPerSandboxKeysFlag(source, ldvalue.Bool(false))
	o.updateStoreWriteForm(t.Context())
	require.False(t, o.redisStorage.WritesPerSandbox())
}

func TestWriteFormKeepsValueOnError(t *testing.T) {
	t.Parallel()

	source := ldtestdata.DataSource()
	o := newWriteFormTestOrchestrator(t, source)

	setPerSandboxKeysFlag(source, ldvalue.Bool(true))
	o.updateStoreWriteForm(t.Context())
	require.True(t, o.redisStorage.WritesPerSandbox())

	// A value of the wrong type is not served; the fallback must not win
	// over the form already selected.
	setPerSandboxKeysFlag(source, ldvalue.String("on"))
	o.updateStoreWriteForm(t.Context())
	require.True(t, o.redisStorage.WritesPerSandbox())
}

// A flag turned off reaches every replica only through its off variation:
// off with the variation of false, the UI's default, puts a replica back on
// the team form; off with no off variation is not served and keeps the form
// already selected, so the flag must carry one for a flip off to be the
// rollback lever.
func TestWriteFormOffFlagRevertsOnlyWithAnOffVariation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		offVariation bool
		perSandbox   bool
	}{
		{"off variation false", true, false},
		{"no off variation", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			source := ldtestdata.DataSource()
			o := newWriteFormTestOrchestrator(t, source)
			setPerSandboxKeysFlag(source, ldvalue.Bool(true))
			o.updateStoreWriteForm(t.Context())
			require.True(t, o.redisStorage.WritesPerSandbox())

			flag := ldmodel.FeatureFlag{
				Key:         featureflags.SandboxStorePerSandboxKeysFlag.Key(),
				On:          false,
				Variations:  []ldvalue.Value{ldvalue.Bool(true), ldvalue.Bool(false)},
				Fallthrough: ldmodel.VariationOrRollout{Variation: ldvalue.NewOptionalInt(0)},
			}
			if tc.offVariation {
				flag.OffVariation = ldvalue.NewOptionalInt(1)
			}
			source.UsePreconfiguredFlag(flag)
			o.updateStoreWriteForm(t.Context())
			require.Equal(t, tc.perSandbox, o.redisStorage.WritesPerSandbox())
		})
	}
}
