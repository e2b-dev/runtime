package template

import (
	_ "embed"
	"net/http"
)

//go:embed browser_port_closed.html
var portClosedHtml string
var portClosedHtmlTemplate = newBrowserPage("portClosedHtml", portClosedHtml)

type portClosedError struct {
	SandboxId string `json:"sandboxId"`
	Message   string `json:"message"`
	Port      uint64 `json:"port"`
	Code      int    `json:"code"`
	Host      string `json:"-"`
}

func (e portClosedError) StatusCode() int {
	return e.Code
}

func NewPortClosedError(sandboxId, host string, port uint64) *TemplatedError[portClosedError] {
	return &TemplatedError[portClosedError]{
		template: portClosedHtmlTemplate,
		vars: portClosedError{
			Message:   "The sandbox is running but port is not open",
			SandboxId: sandboxId,
			Host:      host,
			Port:      port,
			Code:      http.StatusBadGateway,
		},
	}
}
