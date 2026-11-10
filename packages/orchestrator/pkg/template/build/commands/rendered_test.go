//go:build linux

package commands

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type renderFunc func(guest.Step, guest.StepState) (guest.RenderedStep, error)

func (f renderFunc) Render(step guest.Step, state guest.StepState) (guest.RenderedStep, error) {
	return f(step, state)
}

type renderingProfile struct {
	guest.Profile

	renderer guest.StepRenderer
}

func (p renderingProfile) Template() guest.TemplateBuild {
	return guest.TemplateBuild{Renderer: p.renderer}
}

func TestGetCommandUsesTheProfilesRenderer(t *testing.T) {
	t.Parallel()

	renderer := renderFunc(func(guest.Step, guest.StepState) (guest.RenderedStep, error) { return guest.RenderedStep{}, nil })
	ce := &CommandExecutor{BuildContext: buildcontext.BuildContext{Config: config.TemplateConfig{Guest: renderingProfile{guest.Default(), renderer}}}}
	for _, stepType := range []string{"RUN", "COPY", "ADD", "USER", "WORKDIR", "ENV", "ARG", "VOLUME"} {
		cmd, err := ce.getCommand(&templatemanager.TemplateStep{Type: stepType})
		require.NoError(t, err, stepType)
		assert.IsType(t, &Rendered{}, cmd, stepType)
	}

	linux := &CommandExecutor{BuildContext: buildcontext.BuildContext{}}
	for stepType, want := range map[string]Command{"RUN": &Run{}, "COPY": &Copy{}, "USER": &User{}, "WORKDIR": &Workdir{}, "ENV": &Env{}} {
		cmd, err := linux.getCommand(&templatemanager.TemplateStep{Type: stepType})
		require.NoError(t, err)
		assert.IsType(t, want, cmd, "the default profile keeps the default %s", stepType)
	}
	_, err := linux.getCommand(&templatemanager.TemplateStep{Type: "VOLUME"})
	require.Error(t, err)
}

func TestRenderedRecordsTheRenderedState(t *testing.T) {
	t.Parallel()

	var got guest.Step
	var gotState guest.StepState
	r := &Rendered{Renderer: renderFunc(func(step guest.Step, state guest.StepState) (guest.RenderedStep, error) {
		got, gotState = step, state
		state.Env["B"] = "2"

		return guest.RenderedStep{State: guest.StepState{User: "alice", WorkDir: "/app", Env: state.Env}, Warnings: []string{"recorded only"}}, nil
	})}

	parentEnv := map[string]string{"A": "1"}
	meta, err := r.Execute(t.Context(), logger.NewNopLogger(), 0, nil, "", "builder 1/1",
		&templatemanager.TemplateStep{Type: "env", Args: []string{"B", "2"}}, metadata.Context{User: "root", EnvVars: parentEnv})
	require.NoError(t, err)

	assert.Equal(t, guest.Step{Type: "ENV", Args: []string{"B", "2"}}, got, "the step type reaches the renderer upper-cased")
	assert.Equal(t, "root", gotState.User)
	assert.Empty(t, gotState.WorkDir)
	assert.Equal(t, "alice", meta.User)
	require.NotNil(t, meta.WorkDir)
	assert.Equal(t, "/app", *meta.WorkDir)
	assert.Equal(t, map[string]string{"A": "1", "B": "2"}, meta.EnvVars)
	assert.Equal(t, map[string]string{"A": "1"}, parentEnv, "the parent's map is never written")
}

func TestRenderedRefusesAnArchiveWithoutFiles(t *testing.T) {
	t.Parallel()

	r := &Rendered{Renderer: renderFunc(func(guest.Step, guest.StepState) (guest.RenderedStep, error) {
		return guest.RenderedStep{ArchivePath: "/staging/x.tar", Command: "unpack"}, nil
	})}
	_, err := r.Execute(t.Context(), logger.NewNopLogger(), 0, nil, "", "", &templatemanager.TemplateStep{Type: "COPY"}, metadata.Context{})
	require.ErrorContains(t, err, "COPY requires files hash to be set")
}

func TestRenderedReportsARenderError(t *testing.T) {
	t.Parallel()

	refused := errors.New("refused")
	r := &Rendered{Renderer: renderFunc(func(guest.Step, guest.StepState) (guest.RenderedStep, error) { return guest.RenderedStep{}, refused })}
	_, err := r.Execute(t.Context(), logger.NewNopLogger(), 0, nil, "", "", &templatemanager.TemplateStep{Type: "RUN"}, metadata.Context{})
	require.ErrorIs(t, err, refused)
}

// A WORKDIR command that prints more than a path is cut off and its run cancelled.
func TestBoundedOutput(t *testing.T) {
	t.Parallel()

	cancelled := 0
	out := newBoundedOutput(8, func() { cancelled++ })
	out.add("C:\\app")
	assert.False(t, out.overflowed)
	assert.Equal(t, "C:\\app", out.String())

	for range 1000 {
		out.add(strings.Repeat("x", 64*1024))
	}
	assert.True(t, out.overflowed, "an oversize stream overflows")
	assert.Equal(t, 1, cancelled, "the command is cancelled once, at the first overflow")
	assert.Equal(t, "C:\\app", out.String(), "nothing past the limit is kept")
}

// A rendered step runs as the user its rendering names, as a RUN step with a
// user argument does, and as the build's user otherwise.
func TestRenderedRunsAsTheRenderedUser(t *testing.T) {
	t.Parallel()

	build := metadata.Context{User: "builder", EnvVars: map[string]string{"A": "1"}}

	assert.Equal(t, build, runContext(build, guest.RenderedStep{}), "no user keeps the build's")

	got := runContext(build, guest.RenderedStep{User: "root"})
	assert.Equal(t, "root", got.User)
	assert.Equal(t, build.EnvVars, got.EnvVars, "only the user changes")
	assert.Equal(t, "builder", build.User, "the build state is not touched")
}
