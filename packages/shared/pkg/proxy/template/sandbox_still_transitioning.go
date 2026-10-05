package template

import (
	_ "embed"
	"net/http"
)

//go:embed browser_sandbox_still_transitioning.html
var sandboxStillTransitioningHTML string
var sandboxStillTransitioningHTMLTemplate = newBrowserPage("sandboxStillTransitioningHTML", sandboxStillTransitioningHTML)

type sandboxStillTransitioningData struct {
	SandboxId string `json:"sandboxId"`
	Message   string `json:"message"`
	Code      int    `json:"code"`
	Host      string `json:"-"`
}

func (e sandboxStillTransitioningData) StatusCode() int {
	return e.Code
}

func NewSandboxStillTransitioningError(sandboxId, host string) *TemplatedError[sandboxStillTransitioningData] {
	return &TemplatedError[sandboxStillTransitioningData]{
		template: sandboxStillTransitioningHTMLTemplate,
		vars: sandboxStillTransitioningData{
			SandboxId: sandboxId,
			Message:   "The sandbox is still transitioning. Try again in a moment.",
			Host:      host,
			Code:      http.StatusConflict,
		},
	}
}
