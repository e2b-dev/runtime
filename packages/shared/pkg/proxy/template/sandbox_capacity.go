package template

import (
	_ "embed"
	"html/template"
	"net/http"
)

//go:embed browser_sandbox_capacity.html
var sandboxCapacityHTML string
var sandboxCapacityHTMLTemplate = newBrowserPage("sandboxCapacityHTML", sandboxCapacityHTML)

// The refused page has no retry action: no retry helps.
//
//go:embed browser_sandbox_placement_refused.html
var sandboxPlacementRefusedHTML string
var sandboxPlacementRefusedHTMLTemplate = newBrowserPage("sandboxPlacementRefusedHTML", sandboxPlacementRefusedHTML)

// sandboxCapacityMessage is the whole of what the page says about the
// cause; the proxy's own reason stays in the log.
const sandboxCapacityMessage = "The sandbox cannot be resumed right now. Try again in a moment."

// sandboxPlacementRefusedMessage is the page for a placement no retry helps:
// no node in the cluster can run the sandbox as it stands.
const sandboxPlacementRefusedMessage = "The sandbox cannot be resumed: no node in this cluster can run it."

// SandboxShedErrorCode names, in the JSON body, a request the proxy turned
// away for lack of room to wait; the capacity answer carries no code, so the
// two 503s differ by more than the Retry-After header.
const SandboxShedErrorCode = "proxy_parking_lot_full"

type sandboxCapacityData struct {
	SandboxId string `json:"sandboxId"`
	Message   string `json:"message"`
	Code      int    `json:"code"`
	// ErrorCode names the answer for clients that act on a code: the REST
	// API's error_code for a refusal no retry helps, the proxy's own for a
	// shed request; empty for the capacity answer a retry can help.
	ErrorCode string `json:"error_code,omitempty"`
	Host      string `json:"-"`
}

func (e sandboxCapacityData) StatusCode() int {
	return e.Code
}

func NewSandboxCapacityError(sandboxId, host string) *TemplatedError[sandboxCapacityData] {
	return newSandboxCapacityPage(sandboxCapacityHTMLTemplate, sandboxId, host, sandboxCapacityMessage, "", http.StatusServiceUnavailable)
}

// NewSandboxShedError renders the capacity page for a shed request, with
// SandboxShedErrorCode in the JSON body.
func NewSandboxShedError(sandboxId, host string) *TemplatedError[sandboxCapacityData] {
	return newSandboxCapacityPage(sandboxCapacityHTMLTemplate, sandboxId, host, sandboxCapacityMessage, SandboxShedErrorCode, http.StatusServiceUnavailable)
}

// NewSandboxPlacementRefusedError renders a refusal no retry helps; errorCode
// is the API's code for it, carried in the JSON body, and statusCode the
// status the API answered with (zero renders as 503).
func NewSandboxPlacementRefusedError(sandboxId, host, errorCode string, statusCode int) *TemplatedError[sandboxCapacityData] {
	if statusCode == 0 {
		statusCode = http.StatusServiceUnavailable
	}

	return newSandboxCapacityPage(sandboxPlacementRefusedHTMLTemplate, sandboxId, host, sandboxPlacementRefusedMessage, errorCode, statusCode)
}

func newSandboxCapacityPage(page *template.Template, sandboxId, host, message, errorCode string, statusCode int) *TemplatedError[sandboxCapacityData] {
	return &TemplatedError[sandboxCapacityData]{
		template: page,
		vars: sandboxCapacityData{
			SandboxId: sandboxId,
			Message:   message,
			Code:      statusCode,
			ErrorCode: errorCode,
			Host:      host,
		},
	}
}
