// Package removedroutes lists API routes that were removed and now answer
// 410 Gone, with the message each one gives. They sit outside the OpenAPI
// spec, so a server must register them ahead of spec validation.
package removedroutes

import "strings"

const (
	// AccessTokensMessage answers the removed access token routes.
	AccessTokensMessage = "E2B_ACCESS_TOKEN is deprecated and no longer supported. Use an API key (E2B_API_KEY) instead. See https://e2b.dev/docs/migration/access-token-deprecation"
	// TemplateBuildV1Message answers the removed v1 template build routes.
	TemplateBuildV1Message = "The v1 template build API is no longer supported. Upgrade the CLI and migrate to v2 templates. See https://e2b.dev/docs/template/migration-v2"
)

// Route is one removed route. Path uses {name} placeholders, as in the spec
// and net/http patterns.
type Route struct {
	Method  string
	Path    string
	Message string
}

// Routes are every removed route.
var Routes = []Route{
	{Method: "POST", Path: "/access-tokens", Message: AccessTokensMessage},
	{Method: "DELETE", Path: "/access-tokens/{accessTokenID}", Message: AccessTokensMessage},
	{Method: "POST", Path: "/templates", Message: TemplateBuildV1Message},
	{Method: "POST", Path: "/templates/{templateID}", Message: TemplateBuildV1Message},
	{Method: "POST", Path: "/templates/{templateID}/builds/{buildID}", Message: TemplateBuildV1Message},
	{Method: "POST", Path: "/v2/templates", Message: TemplateBuildV1Message},
}

// GinPath is the route's path in gin syntax, with :name placeholders.
func (r Route) GinPath() string {
	return strings.NewReplacer("{", ":", "}", "").Replace(r.Path)
}
