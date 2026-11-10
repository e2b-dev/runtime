package proxy

import (
	"path"
	"strings"
)

// unspecifiedInternalPaths are envd control routes the generator cannot see
// because the OpenAPI spec does not describe them. envd registers /upgrade
// directly on its mux rather than through the generated spec handlers, so
// marking it `x-internal` is not an option.
//
// Prefer describing a new control route in the spec over adding it here: a route
// in the spec is a route the generator keeps in sync on its own.
var unspecifiedInternalPaths = []string{
	"/upgrade",
}

var internalPaths = newInternalPathSet(specInternalPaths, unspecifiedInternalPaths)

func newInternalPathSet(sets ...[]string) map[string]struct{} {
	paths := make(map[string]struct{})

	for _, set := range sets {
		for _, p := range set {
			paths[p] = struct{}{}
		}
	}

	return paths
}

// isInternalEnvdPath reports whether requestPath addresses an envd route
// reserved for the orchestrator's control plane. The orchestrator reaches envd over the
// host network at the sandbox slot IP, so a request for one of these paths that
// arrives through the public sandbox URL has no legitimate sender.
//
// The answer is deliberately method-agnostic: envd answers a known path with 405
// rather than 404 for the wrong method, which confirms the route exists, and a
// method added to a control route later would otherwise slip through.
//
// requestPath must be the decoded path — http.Request.URL.Path — and never
// RawPath, EscapedPath() or RequestURI. This does not percent-decode, so passing
// a still-escaped path reopens the very bypass it exists to close.
//
// Given the decoded path, cleaning before the lookup makes the answer a superset
// of what envd itself would route. envd's router matches the escaped path
// whenever it differs from the decoded one, and an escaped path differs from the
// decoded one exactly when it is not that path's canonical encoding — so the
// router can only ever reach a control route through a request whose decoded path
// is that route, spelled exactly. Cleaning only widens that: /init/, //init and
// /files/../init are refused here though envd would have 404'd them itself.
func isInternalEnvdPath(requestPath string) bool {
	if requestPath == "" {
		return false
	}

	if !strings.HasPrefix(requestPath, "/") {
		requestPath = "/" + requestPath
	}

	_, internal := internalPaths[path.Clean(requestPath)]

	return internal
}
