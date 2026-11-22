package setup

import (
	"os"
	"time"

	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	apiTimeout  = 120 * time.Second
	envdTimeout = 600 * time.Second
)

var (
	APIServerURL      = utils.RequiredEnv("TESTS_API_SERVER_URL", "e.g. https://api.great-innovations.dev")
	SandboxTemplateID = utils.RequiredEnv("TESTS_SANDBOX_TEMPLATE_ID", "e.g. base")
	APIKey            = utils.RequiredEnv("TESTS_E2B_API_KEY", "your Team API key")

	TeamID = os.Getenv("TESTS_SANDBOX_TEAM_ID")
	UserID = os.Getenv("TESTS_SANDBOX_USER_ID")

	OrchestratorHost = os.Getenv("TESTS_ORCHESTRATOR_HOST")
	EnvdProxy        = os.Getenv("TESTS_ENVD_PROXY")
)
