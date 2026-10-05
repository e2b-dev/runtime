//go:build linux

package server

import (
	"context"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var flagContextProbe = featureflags.NewStringFlag("test-sandbox-flag-context-probe", "fallback")

func flagContextSandbox(sandboxID, teamID string) *sandbox.Sandbox {
	return &sandbox.Sandbox{
		Metadata: &sandbox.Metadata{
			Config: sandbox.NewConfig(sandbox.Config{
				Envd:              sandbox.EnvdMetadata{Version: "0.9.0"},
				FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.14.1", KernelVersion: "vmlinux-6.1"},
			}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: sandboxID, TeamID: teamID, TemplateID: "tmpl-probe"},
		},
	}
}

// TestSandboxFlagContexts evaluates a real flag through the contexts Pause and
// Checkpoint attach, so a target of each kind the helper claims to carry is
// resolved by the LaunchDarkly evaluator rather than inspected structurally.
func TestSandboxFlagContexts(t *testing.T) {
	t.Parallel()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(flagContextProbe.Key()).
		Variations(ldvalue.String("default"), ldvalue.String("team"), ldvalue.String("sandbox"), ldvalue.String("envd")).
		FallthroughVariationIndex(0).
		VariationIndexForKey(featureflags.TeamKind, "team-targeted", 1).
		VariationIndexForKey(featureflags.SandboxKind, "sbx-targeted", 2).
		IfMatchContext(featureflags.SandboxKind, featureflags.SandboxEnvdVersionAttribute, ldvalue.String("0.9.0")).
		AndMatchContext(featureflags.SandboxKind, featureflags.SandboxTemplateAttribute, ldvalue.String("tmpl-attr")).
		AndMatchContext(featureflags.SandboxKind, featureflags.SandboxKernelVersionAttribute, ldvalue.String("vmlinux-6.1")).
		AndMatchContext(featureflags.SandboxKind, featureflags.SandboxFirecrackerVersionAttribute, ldvalue.String("v1.14.1")).
		ThenReturnIndex(3))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	eval := func(sbx *sandbox.Sandbox) string {
		return ff.StringFlag(featureflags.AddToContext(t.Context(), sandboxFlagContexts(sbx)...), flagContextProbe)
	}

	attrSbx := flagContextSandbox("sbx-attr", "team-other")
	attrSbx.Runtime.TemplateID = "tmpl-attr"

	for name, tc := range map[string]struct {
		sbx  *sandbox.Sandbox
		want string
	}{
		"team target matches":             {flagContextSandbox("sbx-plain", "team-targeted"), "team"},
		"other team falls through":        {flagContextSandbox("sbx-plain", "team-other"), "default"},
		"sandbox target still matches":    {flagContextSandbox("sbx-targeted", "team-other"), "sandbox"},
		"sandbox attributes still match":  {attrSbx, "envd"},
		"empty team keeps sandbox target": {flagContextSandbox("sbx-targeted", ""), "sandbox"},
		"empty team falls through":        {flagContextSandbox("sbx-plain", ""), "default"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, eval(tc.sbx))
		})
	}
}

// teamRefusesAdmissionServer returns a server whose snapshot-admission
// pre-flight is off for everyone except team-refused, which gets the instant
// probe. With the parent header still pending, that probe refuses before any
// destructive step, so the handler's outcome shows which context it evaluated.
func teamRefusesAdmissionServer(t *testing.T) *Server {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.PauseAdmissionGraceMs.Key()).
		Variations(ldvalue.Int(-1), ldvalue.Int(0)).
		FallthroughVariationIndex(0).
		VariationIndexForKey(featureflags.TeamKind, "team-refused", 1))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	s := admissionTestServer(t, nil)
	s.featureFlags = ff

	return s
}

// awaitRefusal runs op and requires it to return ResourceExhausted. If the
// team context is missing, the pre-flight is off and op proceeds into the
// snapshot path, where the test template parks it; the guard reports that
// instead of hanging.
func awaitRefusal(t *testing.T, op func() error) {
	t.Helper()

	errCh := make(chan error, 1)
	go func() { errCh <- op() }()

	select {
	case err := <-errCh:
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.ResourceExhausted, st.Code())
	case <-time.After(10 * time.Second):
		t.Fatal("handler passed the admission pre-flight: the team target was not evaluated")
	}
}

func TestPause_EvaluatesFlagsWithTeamContext(t *testing.T) {
	t.Parallel()

	s := teamRefusesAdmissionServer(t)
	sbx := admissionTestSandbox(t, "sbx-team-ctx-pause", 61, utils.NewSetOnce[*header.Header]())
	sbx.Runtime.TeamID = "team-refused"
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	awaitRefusal(t, func() error {
		_, err := s.Pause(t.Context(), &orchestrator.SandboxPauseRequest{SandboxId: "sbx-team-ctx-pause"})

		return err
	})

	_, live := s.sandboxFactory.Sandboxes.Get("sbx-team-ctx-pause")
	assert.True(t, live, "a refused pause must leave the sandbox live")
}

func TestCheckpoint_EvaluatesFlagsWithTeamContext(t *testing.T) {
	t.Parallel()

	s := teamRefusesAdmissionServer(t)
	sbx := admissionTestSandbox(t, "sbx-team-ctx-ckpt", 62, utils.NewSetOnce[*header.Header]())
	sbx.Runtime.TeamID = "team-refused"
	s.sandboxFactory.Sandboxes.MarkRunning(t.Context(), sbx)

	awaitRefusal(t, func() error {
		_, err := s.Checkpoint(t.Context(), &orchestrator.SandboxCheckpointRequest{SandboxId: "sbx-team-ctx-ckpt"})

		return err
	})

	_, live := s.sandboxFactory.Sandboxes.Get("sbx-team-ctx-ckpt")
	assert.True(t, live, "a refused checkpoint must leave the sandbox live")
}
