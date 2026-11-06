package api_templates

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
)

func TestRequestTemplateBuild(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()
	db := setup.GetTestDBClient(t)
	teamID, err := uuid.Parse(setup.TeamID)
	require.NoError(t, err)
	limits, err := db.AuthDb.GetTeamByTeamID(t.Context(), teamID)
	require.NoError(t, err)
	defaultFreeDisk, maxFreeDisk := limits.DefaultFreeDiskSizeMb, limits.MaxFreeDiskSizeMb

	for _, test := range []struct {
		name      string
		preferred *int32
		legacy    *int32
		want      int64
		wantError string
	}{
		{name: "default", want: defaultFreeDisk},
		{name: "positive", preferred: new(int32(1024)), want: 1024},
		{name: "zero", preferred: new(int32(0))},
		{name: "maximum", preferred: new(int32(maxFreeDisk)), want: maxFreeDisk},
		{name: "retired-zero", legacy: new(int32(0)), want: defaultFreeDisk},
		{name: "retired-positive", legacy: new(int32(1024)), want: defaultFreeDisk},
		{name: "negative", preferred: new(int32(-1)), wantError: "validation error"},
		{name: "over-limit", preferred: new(int32(maxFreeDisk + 1)), wantError: "Minimum free disk can't be higher than"},
		{name: "retired-over-limit", legacy: new(int32(maxFreeDisk + 1)), want: defaultFreeDisk},
		{name: "new-zero-wins", preferred: new(int32(0)), legacy: new(int32(1024))},
		{name: "new-positive-wins", preferred: new(int32(1024)), legacy: new(int32(0)), want: 1024},
		{name: "invalid-new", preferred: new(int32(maxFreeDisk + 1)), legacy: new(int32(0)), wantError: "Minimum free disk can't be higher than"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			body := map[string]any{
				"name":     "test-request-build-" + test.name,
				"cpuCount": 2,
				"memoryMB": 1024,
			}
			if test.preferred != nil {
				body["minFreeDiskMb"] = *test.preferred
			}
			if test.legacy != nil {
				body["freeDiskSpaceMB"] = *test.legacy
			}
			encoded, err := json.Marshal(body)
			require.NoError(t, err)
			resp, err := c.PostV3TemplatesWithBodyWithResponse(t.Context(), "application/json", bytes.NewReader(encoded), setup.WithAPIKey())
			require.NoError(t, err)
			if test.wantError != "" {
				require.Equal(t, http.StatusBadRequest, resp.StatusCode())
				require.NotNil(t, resp.JSON400)
				assert.Contains(t, resp.JSON400.Message, test.wantError)

				return
			}
			require.Equal(t, http.StatusAccepted, resp.StatusCode())
			require.NotNil(t, resp.JSON202)
			t.Cleanup(func() {
				deleted, err := c.DeleteTemplatesTemplateIDWithResponse(context.WithoutCancel(t.Context()), resp.JSON202.TemplateID, setup.WithAPIKey())
				require.NoError(t, err)
				require.Equal(t, http.StatusNoContent, deleted.StatusCode())
			})
			assert.Equal(t, test.want, readFreeDiskSize(t, db,
				`SELECT free_disk_size_mb FROM public.env_builds WHERE id = $1`, resp.JSON202.BuildID))
		})
	}
}

func readFreeDiskSize(t *testing.T, db *setup.Database, query string, args ...any) int64 {
	t.Helper()

	var value int64
	err := db.Db.TestsRawSQLQuery(t.Context(), query, func(rows pgx.Rows) error {
		rows.Next()

		return rows.Scan(&value)
	}, args...)
	require.NoError(t, err)

	return value
}

func TestRequestTemplateTooLowCPU(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	resp, err := c.PostV3TemplatesWithResponse(t.Context(), api.TemplateBuildRequestV3{
		Name:     new("test-request-build-too-low-cpu"),
		CpuCount: new(api.CPUCount(0)),
		MemoryMB: new(api.MemoryMB(1024)),
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode())
	require.NotNil(t, resp.JSON400)
	assert.True(t, strings.HasPrefix(resp.JSON400.Message, "validation error"), "error should have prefix 'validation error', the error is '%s'", resp.JSON400.Message)
}

func TestRequestTemplateTooLowRAM(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	resp, err := c.PostV3TemplatesWithResponse(t.Context(), api.TemplateBuildRequestV3{
		Name:     new("test-request-build-too-low-ram"),
		CpuCount: new(api.CPUCount(2)),
		MemoryMB: new(api.MemoryMB(32)),
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode())
	require.NotNil(t, resp.JSON400)
	assert.True(t, strings.HasPrefix(resp.JSON400.Message, "validation error"), "error should have prefix 'validation error', the error is '%s'", resp.JSON400.Message)
}

func TestRequestTemplateTooHighCPU(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	resp, err := c.PostV3TemplatesWithResponse(t.Context(), api.TemplateBuildRequestV3{
		Name:     new("test-request-build-too-high-cpu"),
		CpuCount: new(api.CPUCount(1024)),
		MemoryMB: new(api.MemoryMB(1024)),
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode())
	require.NotNil(t, resp.JSON400)
	assert.Equal(t, "CPU count must be at most 32", resp.JSON400.Message)
}

func TestRequestTemplateOddCPU(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	resp, err := c.PostV3TemplatesWithResponse(t.Context(), api.TemplateBuildRequestV3{
		Name:     new("test-request-build-odd-cpu"),
		CpuCount: new(api.CPUCount(3)),
		MemoryMB: new(api.MemoryMB(1024)),
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode())
	require.NotNil(t, resp.JSON400)
	assert.Equal(t, "CPU count must be 1 or an even number", resp.JSON400.Message)
}

func TestRequestTemplateTooHighMemory(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	resp, err := c.PostV3TemplatesWithResponse(t.Context(), api.TemplateBuildRequestV3{
		Name:     new("test-request-build-too-high-memory"),
		CpuCount: new(api.CPUCount(2)),
		MemoryMB: new(api.MemoryMB(1024 * 1024)),
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode())
	require.NotNil(t, resp.JSON400)
	assert.True(t, strings.HasPrefix(resp.JSON400.Message, "Memory can't be higher than"), "error should have prefix 'Memory can't be higher than', the error is '%s'", resp.JSON400.Message)
}

func TestRequestTemplateMemoryNonDivisibleBy2(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	resp, err := c.PostV3TemplatesWithResponse(t.Context(), api.TemplateBuildRequestV3{
		Name:     new("test-request-build-memory-odd"),
		CpuCount: new(api.CPUCount(2)),
		MemoryMB: new(api.MemoryMB(1001)),
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode())
	require.NotNil(t, resp.JSON400)
	assert.Equal(t, "Memory must be divisible by 2", resp.JSON400.Message)
}

func TestRequestTemplateBuildMaxCPU(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()
	db := setup.GetTestDBClient(t)

	for _, test := range []struct {
		name string
		max  *api.CPUCount
		want *int64
	}{
		{name: "above-cpu", max: new(api.CPUCount(4)), want: new(int64(4))},
		{name: "equal-cpu", max: new(api.CPUCount(2)), want: new(int64(2))},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			resp, err := c.PostV3TemplatesWithResponse(t.Context(), api.TemplateBuildRequestV3{
				Name:        new("test-request-build-max-cpu-" + test.name + "-" + uuid.NewString()),
				CpuCount:    new(api.CPUCount(2)),
				MaxCpuCount: test.max,
				MemoryMB:    new(api.MemoryMB(1024)),
			}, setup.WithAPIKey())
			require.NoError(t, err)
			require.Equal(t, http.StatusAccepted, resp.StatusCode())
			require.NotNil(t, resp.JSON202)
			t.Cleanup(func() {
				deleted, err := c.DeleteTemplatesTemplateIDWithResponse(context.WithoutCancel(t.Context()), resp.JSON202.TemplateID, setup.WithAPIKey())
				require.NoError(t, err)
				require.Equal(t, http.StatusNoContent, deleted.StatusCode())
			})

			var got *int64
			err = db.Db.TestsRawSQLQuery(t.Context(), `SELECT max_vcpu FROM public.env_builds WHERE id = $1`, func(rows pgx.Rows) error {
				require.True(t, rows.Next())

				return rows.Scan(&got)
			}, resp.JSON202.BuildID)
			require.NoError(t, err)
			assert.Equal(t, test.want, got)

			tmpl, err := c.GetTemplatesTemplateIDWithResponse(t.Context(), resp.JSON202.TemplateID, &api.GetTemplatesTemplateIDParams{}, setup.WithAPIKey())
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, tmpl.StatusCode())
			require.NotNil(t, tmpl.JSON200)

			var found bool
			for _, b := range tmpl.JSON200.Builds {
				if b.BuildID.String() != resp.JSON202.BuildID {
					continue
				}
				found = true
				if test.want == nil {
					assert.Nil(t, b.MaxCpuCount)
				} else {
					require.NotNil(t, b.MaxCpuCount)
					assert.Equal(t, *test.want, int64(*b.MaxCpuCount))
				}
			}
			assert.True(t, found, "build not listed on template")
		})
	}
}

func TestRequestTemplateBuildMaxCPUInvalid(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	for _, test := range []struct {
		name string
		max  api.CPUCount
		want string
	}{
		{name: "below-cpu", max: 1, want: "Max CPU count must be at least the CPU count (2)"},
		{name: "odd", max: 3, want: "Max CPU count must be 1 or an even number"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			resp, err := c.PostV3TemplatesWithResponse(t.Context(), api.TemplateBuildRequestV3{
				Name:        new("test-request-build-max-cpu-" + test.name + "-" + uuid.NewString()),
				CpuCount:    new(api.CPUCount(2)),
				MaxCpuCount: new(test.max),
				MemoryMB:    new(api.MemoryMB(1024)),
			}, setup.WithAPIKey())
			require.NoError(t, err)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode())
			require.NotNil(t, resp.JSON400)
			assert.Equal(t, test.want, resp.JSON400.Message)
		})
	}
}
