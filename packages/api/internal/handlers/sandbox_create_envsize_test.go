package handlers

import (
	"net/http"
	"strings"
	"testing"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

func TestValidateEnvVarsSize(t *testing.T) {
	t.Run("nil map is admitted", func(t *testing.T) {
		if err := validateEnvVarsSize(nil); err != nil {
			t.Fatalf("nil env map rejected: %v", err)
		}
	})

	t.Run("empty map is admitted", func(t *testing.T) {
		if err := validateEnvVarsSize(api.EnvVars{}); err != nil {
			t.Fatalf("empty env map rejected: %v", err)
		}
	})

	t.Run("ordinary map is admitted", func(t *testing.T) {
		env := api.EnvVars{"FOO": "bar", "PATH": "/usr/bin:/bin", "TOKEN": strings.Repeat("x", 4096)}
		if err := validateEnvVarsSize(env); err != nil {
			t.Fatalf("ordinary env map rejected: %v", err)
		}
	})

	t.Run("map at the budget is admitted", func(t *testing.T) {
		// Value sized so the JSON encoding lands just under the cap:
		// {"K":"<value>"} framing is 8 bytes.
		val := strings.Repeat("a", int(consts.MaxSandboxEnvVarsBytes)-8)
		env := api.EnvVars{"K": val}
		if err := validateEnvVarsSize(env); err != nil {
			t.Fatalf("env map at budget rejected: %v", err)
		}
	})

	t.Run("map over the budget is rejected with 400", func(t *testing.T) {
		val := strings.Repeat("a", int(consts.MaxSandboxEnvVarsBytes))
		env := api.EnvVars{"K": val}
		err := validateEnvVarsSize(env)
		if err == nil {
			t.Fatal("oversized env map admitted")
		}
		if err.Code != http.StatusBadRequest {
			t.Fatalf("got code %d, want %d", err.Code, http.StatusBadRequest)
		}
	})
}
