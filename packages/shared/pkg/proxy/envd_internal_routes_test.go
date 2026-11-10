package proxy

import (
	"slices"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const specFile = "../../../envd/spec/envd.yaml"

// specMarksInternal reports whether the spec marks an operation as control plane.
// It re-derives the answer independently of the generator so that a bug in one is
// not mirrored by the other.
func specMarksInternal(op *openapi3.Operation) bool {
	internal, ok := op.Extensions["x-internal"].(bool)

	return ok && internal
}

func loadSpec(t *testing.T) *openapi3.T {
	t.Helper()

	doc, err := openapi3.NewLoader().LoadFromFile(specFile)
	require.NoError(t, err)

	return doc
}

// TestSpecInternalPathsMatchSpec fails when envd_internal_routes.gen.go is stale, so a
// control route added to the spec cannot ship without the proxy learning to
// reject it. Re-run `go generate ./...` in this package to fix.
func TestSpecInternalPathsMatchSpec(t *testing.T) {
	t.Parallel()

	var want []string

	for path, item := range loadSpec(t).Paths.Map() {
		for _, op := range item.Operations() {
			if specMarksInternal(op) {
				want = append(want, path)

				break
			}
		}
	}

	slices.Sort(want)

	assert.Equal(t, want, specInternalPaths, "envd_internal_routes.gen.go is out of date; run go generate ./...")
}

// TestSpecOperationsAgreeOnInternal guards the assumption the generated set
// rests on: because the proxy rejects an internal path for every method, a path
// may not carry both internal and public operations.
func TestSpecOperationsAgreeOnInternal(t *testing.T) {
	t.Parallel()

	for path, item := range loadSpec(t).Paths.Map() {
		var internal, public []string

		for method, op := range item.Operations() {
			if specMarksInternal(op) {
				internal = append(internal, method)
			} else {
				public = append(public, method)
			}
		}

		if len(internal) > 0 {
			assert.Emptyf(t, public, "%s mixes internal operations %v with public operations %v", path, internal, public)
		}
	}
}

// TestUnspecifiedInternalPathsAreAbsentFromSpec keeps the hand-maintained list
// from outliving its reason: once a route is described in the spec, the
// generator owns it and the manual entry is dead weight.
func TestUnspecifiedInternalPathsAreAbsentFromSpec(t *testing.T) {
	t.Parallel()

	doc := loadSpec(t)

	for _, path := range unspecifiedInternalPaths {
		assert.Nilf(t, doc.Paths.Find(path), "%s is now in the spec; mark it x-internal there and drop it from unspecifiedInternalPaths", path)
	}
}

func TestIsInternalEnvdPath(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		path     string
		internal bool
	}{
		{name: "init", path: "/init", internal: true},
		{name: "freeze", path: "/freeze", internal: true},
		{name: "unfreeze", path: "/unfreeze", internal: true},
		{name: "collapse", path: "/collapse", internal: true},
		{name: "fsfreeze", path: "/fsfreeze", internal: true},
		{name: "fsthaw", path: "/fsthaw", internal: true},
		// Absent from the spec, so only the hand-maintained list covers it.
		{name: "upgrade", path: "/upgrade", internal: true},

		// envd would 404 each of these itself; rejecting them here costs nothing
		// and keeps the answer from depending on how a router normalizes.
		{name: "trailing slash", path: "/init/", internal: true},
		{name: "doubled separator", path: "//init", internal: true},
		{name: "single dot segment", path: "/./init", internal: true},
		{name: "traversal onto a control route", path: "/files/../init", internal: true},
		{name: "unrooted", path: "init", internal: true},

		{name: "health", path: "/health", internal: false},
		{name: "metrics", path: "/metrics", internal: false},
		{name: "envs", path: "/envs", internal: false},
		{name: "files", path: "/files", internal: false},
		{name: "files compose", path: "/files/compose", internal: false},
		{name: "unknown route", path: "/nope", internal: false},
		{name: "control route as a prefix", path: "/initialize", internal: false},
		{name: "control route as a subpath", path: "/init/nested", internal: false},
		{name: "empty", path: "", internal: false},
		// envd routes case-sensitively, so this reaches no handler regardless.
		{name: "different case", path: "/INIT", internal: false},
		// Pins the documented contract: callers pass the decoded path, so a
		// still-escaped one is not this function's job to see through. envd would
		// route this to no handler either, since its router matches the escaped
		// path whenever it differs from the decoded one.
		{name: "percent-encoded, still escaped", path: "/%69nit", internal: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.internal, isInternalEnvdPath(tt.path))
		})
	}
}
