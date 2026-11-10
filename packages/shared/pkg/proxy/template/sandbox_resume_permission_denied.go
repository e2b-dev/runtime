package template

import (
	_ "embed"
	"net/http"
)

//go:embed browser_sandbox_resume_permission_denied.html
var sandboxResumePermissionDeniedHtml string
var sandboxResumePermissionDeniedHtmlTemplate = newBrowserPage("sandboxResumePermissionDeniedHtml", sandboxResumePermissionDeniedHtml)

type sandboxResumePermissionDeniedData struct {
	SandboxId string `json:"sandboxId"`
	Message   string `json:"message"`
	Code      int    `json:"code"`
	Host      string `json:"-"`
}

func (e sandboxResumePermissionDeniedData) StatusCode() int {
	return e.Code
}

func NewSandboxResumePermissionDeniedError(sandboxId, host string) *TemplatedError[sandboxResumePermissionDeniedData] {
	return &TemplatedError[sandboxResumePermissionDeniedData]{
		template: sandboxResumePermissionDeniedHtmlTemplate,
		vars: sandboxResumePermissionDeniedData{
			SandboxId: sandboxId,
			Message:   "This sandbox can't be resumed with the credentials provided. Check your access token and try again.",
			Host:      host,
			Code:      http.StatusForbidden,
		},
	}
}
