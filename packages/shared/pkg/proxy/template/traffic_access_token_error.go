package template

import (
	_ "embed"
	"fmt"
	"net/http"
)

//go:embed browser_traffic_access_token_missing_error.html
var missingTrafficAccessTokenErrHtml string
var missingTrafficAccessTokenErrHtmlTemplate = newBrowserPage("missingTrafficAccessTokenErrHtml", missingTrafficAccessTokenErrHtml)

//go:embed browser_traffic_access_token_invalid_error.html
var invalidTrafficAccessTokenErrHtml string
var invalidTrafficAccessTokenErrHtmlTemplate = newBrowserPage("invalidTrafficAccessTokenErrHtml", invalidTrafficAccessTokenErrHtml)

type trafficAccessTokenErrData struct {
	SandboxId string `json:"sandboxId"`
	Message   string `json:"message"`
	Code      int    `json:"code"`
	Header    string `json:"-"`
	Host      string `json:"-"`
}

func (e trafficAccessTokenErrData) StatusCode() int {
	return e.Code
}

func NewTrafficAccessTokenMissingHeader(sandboxId, host string, header string) *TemplatedError[trafficAccessTokenErrData] {
	return &TemplatedError[trafficAccessTokenErrData]{
		template: missingTrafficAccessTokenErrHtmlTemplate,
		vars: trafficAccessTokenErrData{
			SandboxId: sandboxId,
			Message:   fmt.Sprintf("Sandbox is secured with traffic access token. Token header '%s' is missing", header),
			Host:      host,
			Header:    header,
			Code:      http.StatusForbidden,
		},
	}
}

func NewTrafficAccessTokenInvalidHeader(sandboxId, host string, header string) *TemplatedError[trafficAccessTokenErrData] {
	return &TemplatedError[trafficAccessTokenErrData]{
		template: invalidTrafficAccessTokenErrHtmlTemplate,
		vars: trafficAccessTokenErrData{
			SandboxId: sandboxId,
			Message:   fmt.Sprintf("Sandbox is secured with traffic access token. Provided token in header '%s' is invalid", header),
			Host:      host,
			Header:    header,
			Code:      http.StatusForbidden,
		},
	}
}
