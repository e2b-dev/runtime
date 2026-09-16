package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	handlersmocks "github.com/e2b-dev/infra/packages/api/internal/handlers/mocks"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func TestResolveFilesystemBoot(t *testing.T) {
	t.Parallel()

	memorySnapshot := queries.Snapshot{
		SandboxID: "sbx",
		TeamID:    uuid.New(),
		Config:    &dbtypes.PausedSandboxConfig{},
	}
	fsOnlySnapshot := queries.Snapshot{
		SandboxID: "sbx",
		TeamID:    uuid.New(),
		Config:    &dbtypes.PausedSandboxConfig{FilesystemOnly: true},
	}

	tests := []struct {
		name     string
		memory   *bool
		snapshot queries.Snapshot
		flag     *bool
		want     bool
		wantCode int
	}{
		// nil memory is what the implicit paths (auto-resume, fork) pass; a
		// nil flag expectation makes the mock fail on any consultation.
		{"absent field: memory resume", nil, memorySnapshot, nil, false, 0},
		{"memory true: memory resume", new(true), memorySnapshot, nil, false, 0},
		{"memory false on fs-only snapshot: no-op, flag not consulted", new(false), fsOnlySnapshot, nil, false, 0},
		{"memory false, flag off: rejected, never downgraded", new(false), memorySnapshot, new(false), false, http.StatusBadRequest},
		{"memory false, flag on: cold boot demanded", new(false), memorySnapshot, new(true), true, 0},
		{"legacy snapshot row without config: flag-gated like memory", new(false), queries.Snapshot{SandboxID: "sbx"}, new(false), false, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			flags := handlersmocks.NewMockFeatureFlagsClient(t)
			if tt.flag != nil {
				flags.EXPECT().BoolFlag(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(*tt.flag)
			}

			got, apiErr := resolveFilesystemBoot(t.Context(), flags, tt.memory, tt.snapshot)

			assert.Equal(t, tt.want, got)
			if tt.wantCode == 0 {
				assert.Nil(t, apiErr)
			} else {
				assert.NotNil(t, apiErr)
				assert.Equal(t, tt.wantCode, apiErr.Code)
				assert.Contains(t, apiErr.ClientMsg, "memory: false")
			}
		})
	}
}

func TestSnapshotIsFilesystemOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		snapshot queries.Snapshot
		want     bool
	}{
		{"filesystem-only snapshot", queries.Snapshot{Config: &dbtypes.PausedSandboxConfig{FilesystemOnly: true}}, true},
		{"memory snapshot", queries.Snapshot{Config: &dbtypes.PausedSandboxConfig{}}, false},
		// Rows written before the kind was recorded: a memory snapshot, and so
		// not subject to the resume CPU pin.
		{"row without config", queries.Snapshot{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, snapshotIsFilesystemOnly(tt.snapshot))
		})
	}
}

func TestFrozenSnapshotLifetimeDistinguishesLegacyAndExhausted(t *testing.T) {
	t.Parallel()

	zero := uint64(0)
	seconds := uint64(37)
	tests := []struct {
		name   string
		snap   queries.Snapshot
		want   time.Duration
		frozen bool
	}{
		{name: "legacy no config", snap: queries.Snapshot{}},
		{name: "legacy no field", snap: queries.Snapshot{Config: &dbtypes.PausedSandboxConfig{}}},
		{name: "exhausted", snap: queries.Snapshot{Config: &dbtypes.PausedSandboxConfig{RemainingLifetimeSeconds: &zero}}, frozen: true},
		{name: "remaining", snap: queries.Snapshot{Config: &dbtypes.PausedSandboxConfig{RemainingLifetimeSeconds: &seconds}}, want: 37 * time.Second, frozen: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, frozen := frozenSnapshotLifetime(tt.snap)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.frozen, frozen)
		})
	}
}

func TestClampToFrozenSnapshotLifetime(t *testing.T) {
	t.Parallel()

	zero := uint64(0)
	remaining := uint64(30)
	tests := []struct {
		name      string
		snap      queries.Snapshot
		requested time.Duration
		want      time.Duration
		exhausted bool
	}{
		{name: "legacy unchanged", snap: queries.Snapshot{}, requested: time.Minute, want: time.Minute},
		{name: "shorter request unchanged", snap: queries.Snapshot{Config: &dbtypes.PausedSandboxConfig{RemainingLifetimeSeconds: &remaining}}, requested: 10 * time.Second, want: 10 * time.Second},
		{name: "implicit request capped", snap: queries.Snapshot{Config: &dbtypes.PausedSandboxConfig{RemainingLifetimeSeconds: &remaining}}, requested: time.Minute, want: 30 * time.Second},
		{name: "zero remains exhausted", snap: queries.Snapshot{Config: &dbtypes.PausedSandboxConfig{RemainingLifetimeSeconds: &zero}}, requested: time.Minute, exhausted: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, exhausted := clampToFrozenSnapshotLifetime(tt.requested, tt.snap)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.exhausted, exhausted)
		})
	}
}

func TestSetMemoryOverrideOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		memory  *bool
		err     *api.APIError
		outcome string
	}{
		{"served", new(false), nil, "served"},
		{"flag off", new(false), &api.APIError{Code: http.StatusBadRequest, ErrorCode: errCodeMemoryOverrideDisabled}, "rejected_flag_off"},
		{"in-flight start", new(false), &api.APIError{Code: http.StatusConflict, ErrorCode: orchestrator.ErrCodeStartInFlight}, "rejected_in_flight_start"},
		{"unconfirmed echo", new(false), &api.APIError{Code: http.StatusServiceUnavailable, ErrorCode: orchestrator.ErrCodeFilesystemBootUnconfirmed}, "rejected_unconfirmed"},
		{"capacity 503 is not the echo bucket", new(false), &api.APIError{Code: http.StatusServiceUnavailable, ErrorCode: "sandbox_capacity_unavailable"}, "error"},
		{"plain resume is unlabeled", nil, nil, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			setMemoryOverrideOutcome(c, tt.memory, tt.err)

			got, ok := c.Get(metricMemoryOverride)
			if tt.outcome == "" {
				assert.False(t, ok)

				return
			}
			assert.True(t, ok)
			assert.Equal(t, tt.outcome, got)
		})
	}
}
