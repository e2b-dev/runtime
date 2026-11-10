package template

import (
	_ "embed"
	"net/http"
)

//go:embed browser_team_sandbox_limit.html
var teamSandboxLimitHtml string
var teamSandboxLimitHtmlTemplate = newBrowserPage("teamSandboxLimitHtml", teamSandboxLimitHtml)

type teamSandboxLimitData struct {
	SandboxId string `json:"sandboxId"`
	Message   string `json:"message"`
	Code      int    `json:"code"`
	Host      string `json:"-"`
}

func (e teamSandboxLimitData) StatusCode() int {
	return e.Code
}

func NewTeamSandboxLimitError(sandboxId, host, message string) *TemplatedError[teamSandboxLimitData] {
	if message == "" {
		message = "Sandbox limit reached"
	}

	return &TemplatedError[teamSandboxLimitData]{
		template: teamSandboxLimitHtmlTemplate,
		vars: teamSandboxLimitData{
			SandboxId: sandboxId,
			Message:   message,
			Host:      host,
			Code:      http.StatusTooManyRequests,
		},
	}
}
