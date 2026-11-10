package process

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	rpc "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process"
	spec "github.com/e2b-dev/infra/packages/envd/internal/services/spec/process/processconnect"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
)

// newFrozenTestService builds a process service and returns its freezer, so a test can open
// a frozen window with freezer.Freeze.
//
// Unlike newTestService it needs no root, and that is the point rather than a convenience:
// a start refused by the window fails before the fork, so nothing switches uid. A version
// of this that skipped without root would stop covering the guard in CI.
func newFrozenTestService(t *testing.T) (spec.ProcessClient, *cgroups.WorkloadFreezer) {
	t.Helper()

	cwd := t.TempDir()
	logger := zerolog.Nop()
	freezer := cgroups.NewWorkloadFreezer(cgroups.NewNoopManager())

	svc := newService(&logger, &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    "root",
		Workdir: &cwd,
	}, freezer)

	mux := http.NewServeMux()
	path, handler := spec.NewProcessHandler(svc)
	mux.Handle(path, handler)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return spec.NewProcessClient(srv.Client(), srv.URL), freezer
}

// TestStart_FrozenWorkloadIsRefusedAsRetryable pins the client-visible contract of the
// freeze window. The code has to be a retryable one: a freeze of ours belongs to a pause and
// is undone on the resume, and the caller that meets it is typically a client polling every
// few hundred milliseconds -- so anything a client treats as terminal would turn a pause
// into a command failure.
func TestStart_FrozenWorkloadIsRefusedAsRetryable(t *testing.T) {
	t.Parallel()

	client, freezer := newFrozenTestService(t)

	_, err := freezer.Freeze(t.Context(), cgroups.FreezeOptions{MaxWait: 0})
	require.NoError(t, err)

	stream, err := client.Start(t.Context(), connect.NewRequest(&rpc.StartRequest{
		Process: &rpc.ProcessConfig{Cmd: "echo", Args: []string{"hello"}},
	}))
	require.NoError(t, err, "the stream opens; the refusal arrives on the first receive")
	defer stream.Close()

	assert.False(t, stream.Receive(), "no process may be started into a frozen cgroup")
	require.Error(t, stream.Err())
	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(stream.Err()))
}

// TestStart_SystemTagSurvivesTheFrozenWindow is what keeps the pause itself working. The
// orchestrator drives its pre-pause reclaim chain through this same Start RPC, AFTER the
// freeze it asked for -- so a guard that refused those calls would break the pause it is
// meant to protect. The system tag stays in envd's own cgroup, which no sweep freezes.
//
// The start goes through the RPC with the tag, so the tag's mapping to the system process
// type, which no sweep freezes, is under test too. Without root the fork itself fails on the credential switch; what matters is
// that the answer is not the frozen-window refusal.
func TestStart_SystemTagSurvivesTheFrozenWindow(t *testing.T) {
	t.Parallel()

	client, freezer := newFrozenTestService(t)

	_, err := freezer.Freeze(t.Context(), cgroups.FreezeOptions{MaxWait: 0})
	require.NoError(t, err)

	stream, err := client.Start(t.Context(), connect.NewRequest(&rpc.StartRequest{
		Process: &rpc.ProcessConfig{Cmd: "echo", Args: []string{"hello"}},
		Tag:     new("_system"),
	}))
	require.NoError(t, err)
	defer stream.Close()

	for stream.Receive() {
		_ = stream.Msg()
	}
	assert.NotEqual(t, connect.CodeUnavailable, connect.CodeOf(stream.Err()),
		"a system-tagged start was refused by the frozen window: %v", stream.Err())
	assert.NotErrorIs(t, stream.Err(), cgroups.ErrWorkloadFrozen)
}

// TestStart_EveryOutcomeGivesItsAdmissionBack guards the drain against a leaked weight. A
// start that never returns its admission leaves one unit held forever, and from then on
// every freeze fails its drain -- so each exit from a start, including the failing ones,
// must hand it back.
//
// Two exits, taken at different points: a cwd that does not exist fails in handler.New,
// before an admission is taken, and a start that is admitted and forks returns after the
// fork -- successfully as root, and as anyone else with EPERM from the credential switch,
// which is a fork that failed while admitted. Needs no root either way.
func TestStart_EveryOutcomeGivesItsAdmissionBack(t *testing.T) {
	t.Parallel()

	client, freezer := newFrozenTestService(t)

	for _, cfg := range []*rpc.ProcessConfig{
		{Cmd: "echo", Cwd: new("/nonexistent/cwd")},
		{Cmd: "echo", Args: []string{"hello"}},
	} {
		stream, err := client.Start(t.Context(), connect.NewRequest(&rpc.StartRequest{Process: cfg}))
		require.NoError(t, err)
		for stream.Receive() {
			_ = stream.Msg()
		}
		require.NoError(t, stream.Close())
	}

	_, err := freezer.Freeze(t.Context(), cgroups.FreezeOptions{MaxWait: 0})
	require.NotErrorIs(t, err, cgroups.ErrSpawnDrainTimeout, "a start leaked its admission")
	require.NoError(t, err)
}
