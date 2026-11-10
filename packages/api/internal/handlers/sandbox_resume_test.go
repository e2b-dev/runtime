package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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

	markedBuild := queries.EnvBuild{EnvdUnresponsive: new(true)}
	healthyBuild := queries.EnvBuild{EnvdUnresponsive: new(false)}
	legacyBuild := queries.EnvBuild{}

	tests := []struct {
		name          string
		memory        *bool
		snapshot      queries.Snapshot
		build         queries.EnvBuild
		flag          *bool
		want          bool
		wantCode      int
		wantErrorCode string
	}{
		// nil memory is what the implicit paths (auto-resume, fork) pass; a
		// nil flag expectation makes the mock fail on any consultation.
		{"absent field: memory resume", nil, memorySnapshot, legacyBuild, nil, false, 0, ""},
		{"memory true: memory resume", new(true), memorySnapshot, healthyBuild, nil, false, 0, ""},
		{"memory false on fs-only snapshot: no-op, flag not consulted", new(false), fsOnlySnapshot, legacyBuild, nil, false, 0, ""},
		{"memory false, flag off: rejected, never downgraded", new(false), memorySnapshot, healthyBuild, new(false), false, http.StatusBadRequest, errCodeMemoryOverrideDisabled},
		{"memory false, flag on: cold boot demanded", new(false), memorySnapshot, legacyBuild, new(true), true, 0, ""},
		{"legacy snapshot row without config: flag-gated like memory", new(false), queries.Snapshot{SandboxID: "sbx"}, legacyBuild, new(false), false, http.StatusBadRequest, errCodeMemoryOverrideDisabled},
		// A snapshot taken over an unresponsive guest agent: a memory resume
		// is a conflict with its own code, and memory:false cold-boots it
		// without consulting the team flag.
		{"absent field on a marked snapshot: refused", nil, memorySnapshot, markedBuild, nil, false, http.StatusConflict, errCodeSnapshotGuestAgentUnresponsive},
		{"memory true on a marked snapshot: refused", new(true), memorySnapshot, markedBuild, nil, false, http.StatusConflict, errCodeSnapshotGuestAgentUnresponsive},
		{"memory false on a marked snapshot: cold boot demanded, flag not consulted", new(false), memorySnapshot, markedBuild, nil, true, 0, ""},
		{"a marked filesystem-only snapshot cold-boots as any other", nil, fsOnlySnapshot, markedBuild, nil, false, 0, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			flags := handlersmocks.NewMockFeatureFlagsClient(t)
			if tt.flag != nil {
				flags.EXPECT().BoolFlag(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(*tt.flag)
			}

			got, apiErr := resolveFilesystemBoot(t.Context(), flags, tt.memory, tt.snapshot, tt.build)

			assert.Equal(t, tt.want, got)
			if tt.wantCode == 0 {
				assert.Nil(t, apiErr)
			} else {
				assert.NotNil(t, apiErr)
				assert.Equal(t, tt.wantCode, apiErr.Code)
				assert.Equal(t, tt.wantErrorCode, apiErr.ErrorCode)
				assert.Contains(t, apiErr.ClientMsg, "memory: false")
			}
		})
	}
}

func TestSetResumeGateOutcome(t *testing.T) {
	t.Parallel()

	marked := queries.EnvBuild{EnvdUnresponsive: new(true)}
	plain := queries.EnvBuild{}
	refusal := &api.APIError{Code: http.StatusConflict, ErrorCode: errCodeSnapshotGuestAgentUnresponsive}

	tests := []struct {
		name    string
		memory  *bool
		build   queries.EnvBuild
		err     *api.APIError
		outcome string
	}{
		{"plain resume of a marked snapshot is refused", nil, marked, refusal, "refused_guest_agent_unresponsive"},
		{"memory true of a marked snapshot is refused", new(true), marked, refusal, "refused_guest_agent_unresponsive"},
		{"memory false of a marked snapshot is served", new(false), marked, nil, "served_without_memory"},
		{"memory false of a marked snapshot that failed is unlabeled", new(false), marked, &api.APIError{Code: http.StatusServiceUnavailable, ErrorCode: "sandbox_capacity_unavailable"}, ""},
		{"memory false of a plain snapshot is unlabeled", new(false), plain, nil, ""},
		{"plain resume of a plain snapshot is unlabeled", nil, plain, nil, ""},
		{"another refusal is unlabeled", new(false), plain, &api.APIError{Code: http.StatusBadRequest, ErrorCode: errCodeMemoryOverrideDisabled}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			setResumeGateOutcome(c, tt.memory, tt.build, tt.err)

			got, ok := c.Get(metricResumeGate)
			if tt.outcome == "" {
				assert.False(t, ok)

				return
			}
			assert.True(t, ok)
			assert.Equal(t, tt.outcome, got)
		})
	}
}

func TestSnapshotEnvdUnresponsive(t *testing.T) {
	t.Parallel()

	assert.True(t, snapshotEnvdUnresponsive(queries.EnvBuild{EnvdUnresponsive: new(true)}))
	assert.False(t, snapshotEnvdUnresponsive(queries.EnvBuild{EnvdUnresponsive: new(false)}))
	assert.False(t, snapshotEnvdUnresponsive(queries.EnvBuild{}), "a build from before the mark is a memory snapshot like any other")

	assert.Nil(t, envdUnresponsiveAtPause(nil), "the API field is omitted without a mark")
	assert.Nil(t, envdUnresponsiveAtPause(new(false)))
	require.NotNil(t, envdUnresponsiveAtPause(new(true)))
	assert.True(t, *envdUnresponsiveAtPause(new(true)))
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
