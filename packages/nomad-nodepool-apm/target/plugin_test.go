package target

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad-autoscaler/sdk"
	"github.com/hashicorp/nomad/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testNamespace = "workloads"

type nomadStub struct {
	t                       *testing.T
	mu                      sync.Mutex
	count                   int
	jobModifyIndex          uint64
	autoRevert              *bool
	deployment              *api.Deployment
	scaleConflicts          int
	injectDeploymentOnScale bool
	scaleDeployments        int
	calls                   []string
	scaleRequests           []api.ScalingRequest
	status                  api.JobScaleStatusResponse
}

func newNomadStub(t *testing.T) *nomadStub {
	t.Helper()

	return &nomadStub{
		t:              t,
		count:          2,
		jobModifyIndex: 20,
		status: api.JobScaleStatusResponse{
			JobID:      "example",
			Namespace:  testNamespace,
			JobStopped: true,
			TaskGroups: map[string]api.TaskGroupScaleStatus{
				"web": {Desired: 3, Running: 2, Events: []api.ScalingEvent{{Time: 12345}}},
			},
		},
	}
}

func (s *nomadStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if got := r.URL.Query().Get("namespace"); got != testNamespace {
		http.Error(w, "wrong namespace "+got, http.StatusBadRequest)

		return
	}
	s.calls = append(s.calls, r.Method+" "+r.URL.Path)
	w.Header().Set("Content-Type", "application/json")

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/job/example":
		writeJSON(s.t, w, s.job())
	case r.Method == http.MethodGet && r.URL.Path == "/v1/job/example/deployment":
		writeJSON(s.t, w, s.deployment)
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/deployment/fail/"):
		id := strings.TrimPrefix(r.URL.Path, "/v1/deployment/fail/")
		if s.deployment != nil && s.deployment.ID == id {
			s.deployment.Status = api.DeploymentStatusFailed
			s.deployment.ModifyIndex++
		}
		writeJSON(s.t, w, map[string]any{})
	case r.Method == http.MethodPut && r.URL.Path == "/v1/job/example/scale":
		var req api.ScalingRequest
		if err := json.NewDecoder(r.Body).Decode(&req); !assert.NoError(s.t, err, "decode scale request") {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}
		s.scaleRequests = append(s.scaleRequests, req)
		if s.injectDeploymentOnScale {
			// Simulate a deployment appearing between the plugin's read and
			// its scale write.
			s.injectDeploymentOnScale = false
			s.deployment = &api.Deployment{
				ID:             "deployment-injected",
				JobID:          "example",
				Namespace:      testNamespace,
				JobCreateIndex: 10,
				Status:         api.DeploymentStatusRunning,
				CreateIndex:    40,
				ModifyIndex:    41,
			}
			http.Error(w, "job scaling blocked due to active deployment", http.StatusBadRequest)

			return
		}
		if deploymentActive(s.deployment) {
			// Real Nomad rejects scaling while a deployment is in progress.
			http.Error(w, "job scaling blocked due to active deployment", http.StatusBadRequest)

			return
		}
		if s.scaleConflicts > 0 {
			s.scaleConflicts--
			s.jobModifyIndex++
			http.Error(w, "job modify index conflict", http.StatusConflict)

			return
		}
		if req.Count != nil {
			s.count = int(*req.Count)
		}
		s.jobModifyIndex++
		// Real Nomad registers a new job version and spawns a deployment to
		// roll out the new count (for jobs with an update stanza).
		s.scaleDeployments++
		s.deployment = &api.Deployment{
			ID:             fmt.Sprintf("deployment-scale-%d", s.scaleDeployments),
			JobID:          "example",
			Namespace:      testNamespace,
			JobCreateIndex: 10,
			Status:         api.DeploymentStatusRunning,
			CreateIndex:    50 + uint64(s.scaleDeployments),
			ModifyIndex:    50 + uint64(s.scaleDeployments),
		}
		writeJSON(s.t, w, map[string]any{})
	case r.Method == http.MethodGet && r.URL.Path == "/v1/job/example/scale":
		writeJSON(s.t, w, s.status)
	default:
		http.Error(w, "unexpected request", http.StatusNotFound)
	}
}

func (s *nomadStub) job() *api.Job {
	taskGroup := api.NewTaskGroup("web", s.count)
	if s.autoRevert != nil {
		taskGroup.Update = &api.UpdateStrategy{AutoRevert: s.autoRevert}
	}

	return &api.Job{
		ID:             new("example"),
		Namespace:      new(testNamespace),
		CreateIndex:    new(uint64(10)),
		JobModifyIndex: new(s.jobModifyIndex),
		TaskGroups:     []*api.TaskGroup{taskGroup},
	}
}

func (s *nomadStub) snapshot() ([]string, []api.ScalingRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.calls...), append([]api.ScalingRequest(nil), s.scaleRequests...)
}

func (s *nomadStub) deploymentSnapshot() *api.Deployment {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deployment == nil {
		return nil
	}
	copied := *s.deployment

	return &copied
}

func deploymentActive(deployment *api.Deployment) bool {
	if deployment == nil {
		return false
	}
	switch deployment.Status {
	case api.DeploymentStatusRunning, api.DeploymentStatusPaused, api.DeploymentStatusBlocked,
		api.DeploymentStatusUnblocking, api.DeploymentStatusPending, deploymentStatusInitializing:
		return true
	default:
		return false
	}
}

func newTestPlugin(t *testing.T, stub *nomadStub) *Plugin {
	t.Helper()
	server := httptest.NewServer(stub)
	t.Cleanup(server.Close)

	p := New(hclog.NewNullLogger())
	p.retryDelay = 0
	require.NoError(t, p.SetConfig(map[string]string{
		"nomad_address":   server.URL,
		"nomad_namespace": "configured-default",
	}))

	return p
}

func targetConfig() map[string]string {
	return map[string]string{
		sdk.TargetConfigKeyJob:       "example",
		sdk.TargetConfigKeyTaskGroup: "web",
		sdk.TargetConfigKeyNamespace: testNamespace,
	}
}

func TestScaleNormal(t *testing.T) {
	t.Parallel()

	stub := newNomadStub(t)
	p := newTestPlugin(t, stub)
	action := sdk.ScalingAction{
		Count:  5,
		Reason: "load increased",
		Error:  true,
		Meta:   map[string]any{"policy": "web"},
	}

	require.NoError(t, p.Scale(action, targetConfig()))

	calls, requests := stub.snapshot()
	wantCalls := []string{
		"GET /v1/job/example",
		"GET /v1/job/example/deployment",
		"PUT /v1/job/example/scale",
		"GET /v1/job/example",
	}
	require.Equal(t, wantCalls, calls)
	require.Len(t, requests, 1)
	req := requests[0]
	require.NotNil(t, req.Count)
	require.Equal(t, int64(5), *req.Count)
	require.Equal(t, uint64(20), req.JobModifyIndex)
	require.Equal(t, action.Reason, req.Message)
	require.Equal(t, action.Error, req.Error)
	require.Equal(t, action.Meta, req.Meta)
	rollout := stub.deploymentSnapshot()
	require.NotNil(t, rollout, "rollout deployment spawned by the scale")
	require.Equal(t, api.DeploymentStatusRunning, rollout.Status, "rollout deployment spawned by the scale was not left running")
}

func TestScaleFailsActiveDeploymentBeforeScaling(t *testing.T) {
	t.Parallel()

	statuses := []string{
		api.DeploymentStatusRunning,
		api.DeploymentStatusPaused,
		api.DeploymentStatusBlocked,
		api.DeploymentStatusUnblocking,
		api.DeploymentStatusPending,
		deploymentStatusInitializing,
	}
	for _, status := range statuses {
		t.Run(status, func(t *testing.T) {
			t.Parallel()

			stub := newNomadStub(t)
			stub.deployment = &api.Deployment{
				ID:             "deployment-1",
				JobID:          "example",
				Namespace:      testNamespace,
				JobCreateIndex: 10,
				Status:         status,
				CreateIndex:    30,
				ModifyIndex:    31,
			}
			p := newTestPlugin(t, stub)

			require.NoError(t, p.Scale(sdk.ScalingAction{Count: 4}, targetConfig()))

			calls, _ := stub.snapshot()
			wantCalls := []string{
				"GET /v1/job/example",
				"GET /v1/job/example/deployment",
				"PUT /v1/deployment/fail/deployment-1",
				"GET /v1/job/example",
				"GET /v1/job/example/deployment",
				"PUT /v1/job/example/scale",
				"GET /v1/job/example",
			}
			require.Equal(t, wantCalls, calls)
			rollout := stub.deploymentSnapshot()
			require.NotNil(t, rollout, "rollout deployment spawned by the scale")
			require.NotEqual(t, "deployment-1", rollout.ID)
			require.Equal(t, api.DeploymentStatusRunning, rollout.Status, "rollout deployment spawned by the scale was not left running")
		})
	}
}

func TestScaleLeavesOwnRolloutAloneWhenCountMatches(t *testing.T) {
	t.Parallel()

	stub := newNomadStub(t)
	stub.count = 4
	stub.deployment = &api.Deployment{
		ID:             "deployment-1",
		JobID:          "example",
		Namespace:      testNamespace,
		JobCreateIndex: 10,
		Status:         api.DeploymentStatusRunning,
		CreateIndex:    30,
		ModifyIndex:    31,
	}
	p := newTestPlugin(t, stub)

	require.NoError(t, p.Scale(sdk.ScalingAction{Count: 4}, targetConfig()))

	calls, requests := stub.snapshot()
	wantCalls := []string{"GET /v1/job/example"}
	require.Equal(t, wantCalls, calls)
	require.Empty(t, requests)
	rollout := stub.deploymentSnapshot()
	require.Equal(t, api.DeploymentStatusRunning, rollout.Status, "in-flight rollout was disturbed")
}

func TestScaleRetriesWhenDeploymentAppearsBeforeWrite(t *testing.T) {
	t.Parallel()

	stub := newNomadStub(t)
	stub.injectDeploymentOnScale = true
	p := newTestPlugin(t, stub)

	require.NoError(t, p.Scale(sdk.ScalingAction{Count: 6}, targetConfig()))

	calls, requests := stub.snapshot()
	require.Len(t, requests, 2)
	require.Contains(t, calls, "PUT /v1/deployment/fail/deployment-injected", "conflicting deployment was not failed before retrying")
	rollout := stub.deploymentSnapshot()
	require.NotNil(t, rollout, "rollout deployment spawned by the retried scale")
	require.NotEqual(t, "deployment-injected", rollout.ID)
	require.Equal(t, api.DeploymentStatusRunning, rollout.Status, "rollout deployment spawned by the retried scale was not left running")
}

func TestScaleRefusesAutoRevertJob(t *testing.T) {
	t.Parallel()

	stub := newNomadStub(t)
	stub.autoRevert = new(true)
	stub.deployment = &api.Deployment{
		ID:             "deployment-1",
		JobID:          "example",
		Namespace:      testNamespace,
		JobCreateIndex: 10,
		Status:         api.DeploymentStatusRunning,
		CreateIndex:    30,
		ModifyIndex:    31,
	}
	p := newTestPlugin(t, stub)

	err := p.Scale(sdk.ScalingAction{Count: 4}, targetConfig())
	require.ErrorContains(t, err, "auto_revert")

	calls, requests := stub.snapshot()
	// The guard trips before any deployment read or mutation, and the error
	// is terminal so there is no retry.
	wantCalls := []string{"GET /v1/job/example"}
	require.Equal(t, wantCalls, calls)
	require.Empty(t, requests)
	rollout := stub.deploymentSnapshot()
	require.Equal(t, api.DeploymentStatusRunning, rollout.Status, "deployment of auto_revert job was disturbed")
}

func TestScaleAutoRevertJobNoOpWhenCountMatches(t *testing.T) {
	t.Parallel()

	stub := newNomadStub(t)
	stub.autoRevert = new(true)
	stub.count = 4
	p := newTestPlugin(t, stub)

	require.NoError(t, p.Scale(sdk.ScalingAction{Count: 4}, targetConfig()))
}

func TestScaleAllowsExplicitAutoRevertFalse(t *testing.T) {
	t.Parallel()

	stub := newNomadStub(t)
	stub.autoRevert = new(false)
	p := newTestPlugin(t, stub)

	require.NoError(t, p.Scale(sdk.ScalingAction{Count: 5}, targetConfig()))
}

func TestScaleLeavesTerminalDeploymentAlone(t *testing.T) {
	t.Parallel()

	stub := newNomadStub(t)
	stub.deployment = &api.Deployment{
		ID:             "deployment-1",
		JobCreateIndex: 10,
		Status:         api.DeploymentStatusSuccessful,
	}
	p := newTestPlugin(t, stub)

	require.NoError(t, p.Scale(sdk.ScalingAction{Count: 3}, targetConfig()))

	calls, _ := stub.snapshot()
	for _, call := range calls {
		require.False(t, strings.HasPrefix(call, "PUT /v1/deployment/fail/"), "terminal deployment was failed")
	}
}

func TestScaleDryRunDoesNotReadOrWrite(t *testing.T) {
	t.Parallel()

	stub := newNomadStub(t)
	stub.deployment = &api.Deployment{
		ID:             "deployment-1",
		JobCreateIndex: 10,
		Status:         api.DeploymentStatusRunning,
	}
	p := newTestPlugin(t, stub)

	require.NoError(t, p.Scale(sdk.ScalingAction{Count: sdk.StrategyActionMetaValueDryRunCount}, targetConfig()))
	calls, _ := stub.snapshot()
	require.Empty(t, calls, "dry run made requests")
}

func TestScaleRetriesCASRaceFromFreshJob(t *testing.T) {
	t.Parallel()

	stub := newNomadStub(t)
	stub.scaleConflicts = 1
	p := newTestPlugin(t, stub)

	require.NoError(t, p.Scale(sdk.ScalingAction{Count: 6}, targetConfig()))

	_, requests := stub.snapshot()
	require.Len(t, requests, 2)
	require.Equal(t, uint64(20), requests[0].JobModifyIndex)
	require.Equal(t, uint64(21), requests[1].JobModifyIndex)
}

func TestStatusUsesDurableTaskGroupCount(t *testing.T) {
	t.Parallel()

	stub := newNomadStub(t)
	p := newTestPlugin(t, stub)

	status, err := p.Status(targetConfig())
	require.NoError(t, err)
	require.False(t, status.Ready)
	require.Equal(t, int64(3), status.Count)
	require.Equal(t, "true", status.Meta["nomad_autoscaler.target.nomad.example.stopped"])
	require.Equal(t, "12345", status.Meta[sdk.TargetStatusMetaKeyLastEvent])
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	assert.NoError(t, json.NewEncoder(w).Encode(value), "encode response")
}
