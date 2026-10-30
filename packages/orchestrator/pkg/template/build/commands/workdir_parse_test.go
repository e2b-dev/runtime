package commands

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
)

func TestSaveWorkdirMetaFromOutputPreservesImageMetadata(t *testing.T) {
	oldWorkdir := "/image-default"
	envs := map[string]string{"MAMBA_EXE": "/bin/micromamba", "APP_MODE": "freecad"}
	image := metadata.Context{User: "app-user", WorkDir: &oldWorkdir, EnvVars: envs}

	got, err := saveWorkdirMetaFromOutput(
		image,
		"/app",
		"login hook stdout: /noise/path\n"+workdirOutputMarker+"/app\n",
		"_activate_current_env.sh: line 8: : command not found\ncritical libmamba Shell not initialized\n",
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.User != image.User {
		t.Fatalf("user changed: got %q want %q", got.User, image.User)
	}
	if !reflect.DeepEqual(got.EnvVars, image.EnvVars) {
		t.Fatalf("environment changed: got %#v want %#v", got.EnvVars, image.EnvVars)
	}
	if got.WorkDir == nil || *got.WorkDir != "/app" {
		t.Fatalf("got workdir %v want /app", got.WorkDir)
	}
	if image.WorkDir == nil || *image.WorkDir != oldWorkdir {
		t.Fatalf("input metadata was mutated: %#v", image)
	}
}

func TestSaveWorkdirMetaFromOutputRejectsMissingAbsolutePath(t *testing.T) {
	_, err := saveWorkdirMetaFromOutput(
		metadata.Context{}, "/app", "hook output only", "login hook diagnostic", nil,
	)
	if err == nil {
		t.Fatal("expected an error without an absolute marked path")
	}
	if !strings.Contains(err.Error(), "login hook diagnostic") {
		t.Fatalf("error omits stderr context: %v", err)
	}
}

func TestSaveWorkdirMetaFromOutputPreservesCommandFailure(t *testing.T) {
	commandErr := errors.New("exit status 1")
	_, err := saveWorkdirMetaFromOutput(
		metadata.Context{}, "/app", workdirOutputMarker+"/app\n", "", commandErr,
	)
	if !errors.Is(err, commandErr) {
		t.Fatalf("got %v, want wrapped command error", err)
	}
}
