//go:build linux

package build

import (
	"context"
	"testing"

	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

func newFlagClient(t *testing.T, source *ldtestdata.TestDataSource) *featureflags.Client {
	t.Helper()

	client, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, client.Close(context.WithoutCancel(t.Context())))
	})

	return client
}

func TestResolveRootfsOptions(t *testing.T) {
	t.Parallel()

	cfg := config.TemplateConfig{TemplateID: "template-1", TeamID: "team-1"}
	flag := featureflags.BuildEnvdMemoryProtection

	t.Run("the flag's fallback leaves the protection off", func(t *testing.T) {
		t.Parallel()

		// A cluster with no flag data resolves to the fallback; that is the
		// state the whole fleet ships in, so it has to be off.
		assert.False(t, flag.Fallback())

		client := newFlagClient(t, ldtestdata.DataSource())
		assert.False(t, resolveRootfsOptions(t.Context(), client, cfg).EnvdMemoryProtection)
	})

	t.Run("the flag on turns the protection on", func(t *testing.T) {
		t.Parallel()

		source := ldtestdata.DataSource()
		source.Update(source.Flag(flag.Key()).BooleanFlag().VariationForAll(true))

		client := newFlagClient(t, source)
		assert.True(t, resolveRootfsOptions(t.Context(), client, cfg).EnvdMemoryProtection)
	})

	t.Run("the flag is evaluated on the build's team", func(t *testing.T) {
		t.Parallel()

		// The rollout is a percentage of teams, so a rule targeting this
		// build's team must reach the evaluation. Build also adds the team
		// context to ctx; this pins that the evaluation carries it on its own,
		// as the provision-version evaluation does, with a bare ctx.
		source := ldtestdata.DataSource()
		source.Update(source.Flag(flag.Key()).BooleanFlag().
			VariationForAll(false).
			VariationForKey(featureflags.TeamKind, cfg.TeamID, true))

		client := newFlagClient(t, source)
		assert.True(t, resolveRootfsOptions(t.Context(), client, cfg).EnvdMemoryProtection)

		other := cfg
		other.TeamID = "team-2"
		assert.False(t, resolveRootfsOptions(t.Context(), client, other).EnvdMemoryProtection)
	})
}
