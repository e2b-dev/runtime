package handlers

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
)

// readySource is a source template row with a usable ready build.
func readySource() queries.GetTeamTemplateRow {
	envd := "0.4.0"

	return queries.GetTeamTemplateRow{
		BuildStatus:      types.BuildStatusGroupReady,
		BuildVcpu:        4,
		BuildRamMb:       2048,
		BuildEnvdVersion: &envd,
		Aliases:          []string{"my-template"},
	}
}

func TestBuildRefreshEnvdPlan_Success(t *testing.T) {
	plan, apiErr := buildRefreshEnvdPlan("tmpl-1", readySource())
	require.Nil(t, apiErr)

	// FROM the template itself, resolved to its latest ready build upstream.
	assert.Equal(t, "tmpl-1", plan.FromTemplate)

	// Exactly one trivial RUN step -- what guarantees the step phase (and thus
	// the envd swap) runs instead of leaving it to finalize.
	require.Len(t, plan.Steps, 1)
	assert.Equal(t, "RUN", plan.Steps[0].Type)
	require.NotNil(t, plan.Steps[0].Args)
	assert.Equal(t, []string{"true"}, *plan.Steps[0].Args)

	// Specs inherited from the source, not dropped to RegisterBuild defaults.
	assert.Equal(t, int32(4), plan.CPU)
	assert.Equal(t, int32(2048), plan.RAM)

	// Alias inherited so the in-place rebuild keeps the same name.
	require.NotNil(t, plan.Alias)
	assert.Equal(t, "my-template", *plan.Alias)

	assert.Equal(t, "0.4.0", plan.FromEnvdVersion)
}

func TestBuildRefreshEnvdPlan_NoReadyBuild(t *testing.T) {
	src := readySource()
	src.BuildStatus = types.BuildStatusGroupInProgress

	_, apiErr := buildRefreshEnvdPlan("tmpl-1", src)
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.Code)
}

func TestBuildRefreshEnvdPlan_MissingSpecs(t *testing.T) {
	for _, tc := range []struct {
		name string
		cpu  int64
		ram  int64
	}{
		{"zero cpu", 0, 2048},
		{"zero ram", 4, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := readySource()
			src.BuildVcpu = tc.cpu
			src.BuildRamMb = tc.ram

			_, apiErr := buildRefreshEnvdPlan("tmpl-1", src)
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Code)
		})
	}
}

func TestBuildRefreshEnvdPlan_NoAliasNoEnvdVersion(t *testing.T) {
	src := readySource()
	src.Aliases = nil
	src.BuildEnvdVersion = nil

	plan, apiErr := buildRefreshEnvdPlan("tmpl-1", src)
	require.Nil(t, apiErr)
	assert.Nil(t, plan.Alias, "no source alias -> nil, RegisterBuild leaves aliases untouched")
	assert.Equal(t, "unknown", plan.FromEnvdVersion)
}
