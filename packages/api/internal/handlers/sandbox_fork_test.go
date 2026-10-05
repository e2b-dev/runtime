package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/api/internal/api"
)

func TestForkOutcome(t *testing.T) {
	t.Parallel()

	started := api.SandboxForkResult{Sandbox: &api.Sandbox{}}
	failed := api.SandboxForkResult{Error: &api.Error{Code: 429, Message: "limit"}}

	tests := []struct {
		name        string
		results     []api.SandboxForkResult
		nodeIDs     []string
		wantStarted int
		wantNodes   int
	}{
		{"none", nil, nil, 0, 0},
		{"all on one node", []api.SandboxForkResult{started, started}, []string{"n1", "n1"}, 2, 1},
		{"spread across nodes", []api.SandboxForkResult{started, started, started}, []string{"n1", "n2", "n1"}, 3, 2},
		{"failed forks count no node", []api.SandboxForkResult{failed, started, failed}, []string{"", "n2", ""}, 1, 1},
		{"all failed", []api.SandboxForkResult{failed, failed}, []string{"", ""}, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotStarted, gotNodes := forkOutcome(tt.results, tt.nodeIDs)
			assert.Equal(t, tt.wantStarted, gotStarted)
			assert.Equal(t, tt.wantNodes, gotNodes)
		})
	}
}
