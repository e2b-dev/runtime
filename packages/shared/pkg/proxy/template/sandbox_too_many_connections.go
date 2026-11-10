package template

import (
	_ "embed"
	"fmt"
	"net/http"
)

//go:embed browser_sandbox_too_many_connections.html
var sandboxTooManyConnectionsHtml string
var sandboxTooManyConnectionsHtmlTemplate = newBrowserPage("sandboxTooManyConnectionsHtml", sandboxTooManyConnectionsHtml)

type sandboxTooManyConnectionsData struct {
	SandboxId       string `json:"sandboxId"`
	Message         string `json:"message"`
	Code            int    `json:"code"`
	ConnectionLimit int    `json:"connectionLimit"`
	Host            string `json:"-"`
}

func (e sandboxTooManyConnectionsData) StatusCode() int {
	return e.Code
}

func NewSandboxTooManyConnectionsError(sandboxId, host string, connectionLimit int) *TemplatedError[sandboxTooManyConnectionsData] {
	return &TemplatedError[sandboxTooManyConnectionsData]{
		template: sandboxTooManyConnectionsHtmlTemplate,
		vars: sandboxTooManyConnectionsData{
			SandboxId:       sandboxId,
			Message:         fmt.Sprintf("The sandbox has too many concurrent incoming connections (limit: %d)", connectionLimit),
			Host:            host,
			ConnectionLimit: connectionLimit,
			Code:            http.StatusTooManyRequests,
		},
	}
}
