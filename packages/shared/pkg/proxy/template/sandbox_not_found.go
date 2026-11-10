package template

import (
	_ "embed"
	"net/http"
)

//go:embed browser_sandbox_not_found.html
var sandboxNotFoundHtml string
var sandboxNotFoundHtmlTemplate = newBrowserPage("sandboxNotFoundHtml", sandboxNotFoundHtml)

type sandboxNotFoundData struct {
	SandboxId string `json:"sandboxId"`
	Message   string `json:"message"`
	Code      int    `json:"code"`
	Host      string `json:"-"`
}

func (e sandboxNotFoundData) StatusCode() int {
	return e.Code
}

func NewSandboxNotFoundError(sandboxId, host string) *TemplatedError[sandboxNotFoundData] {
	return &TemplatedError[sandboxNotFoundData]{
		template: sandboxNotFoundHtmlTemplate,
		vars: sandboxNotFoundData{
			SandboxId: sandboxId,
			Message:   "The sandbox was not found",
			Host:      host,
			Code:      http.StatusBadGateway,
		},
	}
}
