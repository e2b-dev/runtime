package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
)

func TestCreateSandbox_VcpuTargetFromRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		vcpu     *int32
		stored   int64
		isResume bool
		want     int64
	}{
		{name: "create inherits build count", stored: 2, want: 2},
		{name: "create accepts an odd online count", stored: 2, vcpu: new(int32(3)), want: 3},
		{name: "create uses full capacity", stored: 2, vcpu: new(int32(4)), want: 4},
		{name: "resume inherits snapshot count", stored: 4, isResume: true, want: 4},
		{name: "resume lowers online count", stored: 4, isResume: true, vcpu: new(int32(1)), want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := &cpuRecordingSandboxClient{}
			// Any build with a capacity needs a worker that supports resizable CPUs.
			o := newCreateSandboxTestOrchestrator(t,
				nodemanager.WithOrchestratorVersion(clusters.ResizableTemplateBuildsMinVersion.String()),
				func(node *nodemanager.TestNode) { node.SetSandboxClient(rec) },
			)
			o.featureFlagsClient = sandboxCPUFlags(t, true)
			build := testBuild()
			build.Vcpu = tt.stored
			build.MaxVcpu = new(int64(4))
			fetcher := func(_ context.Context) (SandboxMetadata, *api.APIError) {
				return SandboxMetadata{TemplateID: "tpl", BaseTemplateID: "base-tpl", Build: build}, nil
			}

			now := time.Now()
			team := testTeam()
			sbx, apiErr := o.CreateSandbox(t.Context(), "sbx-vcpu-"+uuid.New().String()[:8], uuid.New().String(), team,
				fetcher, now, now.Add(time.Hour), time.Hour, tt.isResume, false, sandbox.CreationMetadata{}, WithCPUCount(tt.vcpu))
			require.Nil(t, apiErr)

			assert.Equal(t, int32(1), rec.calls.Load())
			req := rec.request.Load()
			require.NotNil(t, req)
			assert.Equal(t, tt.want, req.GetSandbox().GetVcpu())
			assert.Equal(t, int64(4), req.GetSandbox().GetMaxVcpus())
			assert.Equal(t, tt.want, sbx.VCpu)
			stored, err := o.GetSandbox(t.Context(), team.ID, sbx.SandboxID)
			require.NoError(t, err)
			assert.Equal(t, tt.want, stored.VCpu)
		})
	}
}

func TestCreateSandboxCPUOverrideRefusesJoin(t *testing.T) {
	t.Parallel()
	o := newCreateSandboxTestOrchestrator(t)
	o.featureFlagsClient = sandboxCPUFlags(t, true)
	team := testTeam()
	sandboxID := "sbx-cpu-join-" + uuid.NewString()
	finish, wait, err := o.sandboxStore.Reserve(t.Context(), team.ID, sandboxID, 10)
	require.NoError(t, err)
	require.Nil(t, wait)
	defer finish(sandbox.Sandbox{}, errors.New("test reservation released"))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	now := time.Now()
	_, apiErr := o.CreateSandbox(ctx, sandboxID, uuid.NewString(), team,
		func(context.Context) (SandboxMetadata, *api.APIError) {
			t.Error("a joined request must not fetch or change the snapshot")

			return SandboxMetadata{}, nil
		}, now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{}, WithCPUCount(new(int32(4))))
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusConflict, apiErr.Code)
	assert.Equal(t, ErrCodeStartInFlight, apiErr.ErrorCode)
}

// With the feature off, a cpuCount that meets an in-flight start must still
// get the 400 for the disabled feature, not the retryable-looking 409.
func TestCreateSandboxCPUOverrideDisabledBeforeJoin(t *testing.T) {
	t.Parallel()
	o := newCreateSandboxTestOrchestrator(t)
	o.featureFlagsClient = sandboxCPUFlags(t, false)
	team := testTeam()
	sandboxID := "sbx-cpu-off-join-" + uuid.NewString()
	finish, wait, err := o.sandboxStore.Reserve(t.Context(), team.ID, sandboxID, 10)
	require.NoError(t, err)
	require.Nil(t, wait)
	defer finish(sandbox.Sandbox{}, errors.New("test reservation released"))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	now := time.Now()
	_, apiErr := o.CreateSandbox(ctx, sandboxID, uuid.NewString(), team,
		func(context.Context) (SandboxMetadata, *api.APIError) {
			t.Error("a refused request must not fetch the snapshot")

			return SandboxMetadata{}, nil
		}, now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{}, WithCPUCount(new(int32(4))))
	require.NotNil(t, apiErr)
	assert.Equal(t, http.StatusBadRequest, apiErr.Code)
	assert.Equal(t, errCodeSandboxCpuCountDisabled, apiErr.ErrorCode)
}

func TestCreateSandboxEnforcesCPUAllowanceBeforePlacement(t *testing.T) {
	t.Parallel()
	for _, requested := range []*int32{nil, new(int32(4))} {
		name := "inherited CPU count"
		if requested != nil {
			name = "explicit CPU count"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := &cpuRecordingSandboxClient{}
			o := newCreateSandboxTestOrchestrator(t,
				nodemanager.WithOrchestratorVersion(clusters.ResizableTemplateBuildsMinVersion.String()),
				func(node *nodemanager.TestNode) { node.SetSandboxClient(rec) },
			)
			o.featureFlagsClient = sandboxCPUFlags(t, true)
			team := testTeam()
			team.Limits.MaxVcpu = 1
			build := testBuild()
			build.MaxVcpu = new(int64(4))
			fetcher := func(context.Context) (SandboxMetadata, *api.APIError) {
				return SandboxMetadata{Build: build}, nil
			}
			now := time.Now()
			sandboxID := "sbx-cpu-limit-" + uuid.NewString()
			_, apiErr := o.CreateSandbox(t.Context(), sandboxID, uuid.NewString(), team, fetcher,
				now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{}, WithCPUCount(requested))
			if requested == nil {
				// An inherited count above the limit starts at the limit.
				require.Nil(t, apiErr)
				req := rec.request.Load()
				require.NotNil(t, req)
				assert.Equal(t, int64(1), req.GetSandbox().GetVcpu())
				assert.Equal(t, int64(4), req.GetSandbox().GetMaxVcpus())

				return
			}
			require.NotNil(t, apiErr)
			assert.Equal(t, http.StatusBadRequest, apiErr.Code)
			assert.Equal(t, errCodeSandboxCpuCountTeamLimit, apiErr.ErrorCode)
			assert.Zero(t, rec.calls.Load(), "a rejected CPU request must not reach a worker")

			sbx, apiErr := o.CreateSandbox(t.Context(), sandboxID, uuid.NewString(), team, fetcher,
				now, now.Add(time.Hour), time.Hour, true, false, sandbox.CreationMetadata{}, WithCPUCount(new(int32(1))))
			require.Nil(t, apiErr, "a refused start must release its reservation")
			assert.Equal(t, int64(1), sbx.VCpu)
			assert.Equal(t, int32(1), rec.calls.Load())
			req := rec.request.Load()
			require.NotNil(t, req)
			assert.Equal(t, int64(1), req.GetSandbox().GetVcpu())
			assert.Equal(t, int64(4), req.GetSandbox().GetMaxVcpus())
		})
	}
}
