package template

import (
	_ "embed"
	"net/http"
)

//go:embed browser_internal_route.html
var internalRouteHtml string
var internalRouteHtmlTemplate = newBrowserPage("internalRouteHtml", internalRouteHtml)

type internalRouteData struct {
	Message string `json:"message"`
	Path    string `json:"path"`
	Code    int    `json:"code"`
	Host    string `json:"-"`
}

func (e internalRouteData) StatusCode() int {
	return e.Code
}

// NewInternalRouteError reports that the request addressed a control-plane route
// that the proxy does not route.
//
// 404 rather than 403, because the route genuinely does not exist at this
// address — it is only reachable on the control plane's own network path. The
// message still names the reason, since the routes are described in a public
// OpenAPI spec and a vague response would buy no secrecy while costing every
// caller a debugging session.
//
// The sandbox ID is deliberately absent from the body: the caller learns nothing
// about which sandbox it failed to address that it did not already supply.
func NewInternalRouteError(host, path string) *TemplatedError[internalRouteData] {
	return &TemplatedError[internalRouteData]{
		template: internalRouteHtmlTemplate,
		vars: internalRouteData{
			Message: "This endpoint is reserved for the E2B control plane and is not reachable through the sandbox URL",
			Path:    path,
			Host:    host,
			Code:    http.StatusNotFound,
		},
	}
}
