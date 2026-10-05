package api_templates

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

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
	defaultFreeDisk := readFreeDiskSize(t, db,
		`SELECT default_free_disk_size_mb FROM public.team_limits WHERE id = $1`, setup.TeamID)
	maxFreeDisk := readFreeDiskSize(t, db,
		`SELECT max_free_disk_size_mb FROM public.team_limits WHERE id = $1`, setup.TeamID)

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
