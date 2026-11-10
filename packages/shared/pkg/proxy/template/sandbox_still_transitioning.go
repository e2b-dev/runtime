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
	return newSandboxStillTransitioningPage(sandboxId, host, http.StatusConflict)
}

// NewSandboxStillTransitioningUnavailableError is the same page as 503, for
// envd traffic, where the SDK reads a 409 as "already exists".
func NewSandboxStillTransitioningUnavailableError(sandboxId, host string) *TemplatedError[sandboxStillTransitioningData] {
	return newSandboxStillTransitioningPage(sandboxId, host, http.StatusServiceUnavailable)
}

func newSandboxStillTransitioningPage(sandboxId, host string, code int) *TemplatedError[sandboxStillTransitioningData] {
	return &TemplatedError[sandboxStillTransitioningData]{
		template: sandboxStillTransitioningHTMLTemplate,
		vars: sandboxStillTransitioningData{
			SandboxId: sandboxId,
			Message:   "The sandbox is still transitioning. Try again in a moment.",
			Host:      host,
			Code:      code,
		},
	}
}
