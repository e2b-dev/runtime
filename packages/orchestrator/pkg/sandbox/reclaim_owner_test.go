//go:build linux

package sandbox

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	reclaimRegistrar = "reclaimLiveEntryOnCleanup"
	networkAssign    = "AssignNetwork"
)

// TestEveryFactoryRegistersTheReclaimOwner fails when a sandbox is given the live
// map without the callback that reclaims its entry.
//
// This is a source-level guard because no behavioural test in this package can
// be one. The factories need Firecracker, a network slot and a rootfs, so every
// test here builds the cleanup chain by hand — and a fixture that registers the
// registrar itself passes whether or not the factory does. Deleting the call
// from either factory left the whole suite green when this was measured.
//
// A sandbox that reaches the live map without it keeps its entry unless an
// operation-initiated stop happens to take it — so a lifecycle that ends by
// crashing or by its own timeout leaks one permanently, and the only symptom is
// upward drift in the count of running sandboxes, which is also what real growth
// looks like.
func TestEveryFactoryRegistersTheReclaimOwner(t *testing.T) {
	t.Parallel()

	files := packageFiles(t)
	require.NotEmpty(t, files, "found no sources to scan; is the test running outside the package directory?")

	registrars := map[string]int{}
	factories := map[string]int{}

	for _, path := range files {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoErrorf(t, err, "parse %s", path)

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}

			if n := countLiveMapConstructions(fn); n > 0 {
				factories[fn.Name.Name] += n
			}
			if n := countCalls(fn, reclaimRegistrar); n > 0 {
				registrars[fn.Name.Name] += n
			}
		}
	}

	// The map reference is what makes a sandbox reachable through the live map,
	// so every construction that sets it owes exactly one registration.
	require.NotEmpty(t, factories, "found no sandbox construction setting the live map; has the field been renamed?")

	for name, constructions := range factories {
		assert.Equalf(t, constructions, registrars[name],
			"%s builds %d sandbox(es) holding the live map but registers %d reclaim owner(s); "+
				"every factory path must call %s", name, constructions, registrars[name], reclaimRegistrar)
	}

	for name := range registrars {
		assert.Containsf(t, factories, name,
			"%s registers the reclaim owner but builds no sandbox holding the live map; "+
				"the registration belongs with the construction it covers", name)
	}
}

// TestReclaimOwnerIsRegisteredWithTheNetworkAssignment pins where the registrar
// is called, which decides when the entry is reclaimed relative to the rest of the
// chain: the chain runs backward, so registering after the network slot's release
// has been registered runs the reclaim before it. Nothing fails at build time if
// the call moves, and no unit test can see the consequence — one subscriber acts
// on the notification, and what it does is delete a routing record.
//
// Pairing it with AssignNetwork is the relation the call sites are written to
// express: the sandbox becomes findable by address and acquires its reclaimer in
// the same breath.
func TestReclaimOwnerIsRegisteredWithTheNetworkAssignment(t *testing.T) {
	t.Parallel()

	files := packageFiles(t)
	checked := 0

	for _, path := range files {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoErrorf(t, err, "parse %s", path)

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || countCalls(fn, reclaimRegistrar) == 0 {
				continue
			}

			at := statementIndex(fn, reclaimRegistrar)
			require.GreaterOrEqualf(t, at, 0,
				"%s: the %s call is not a statement of the function body, so its position cannot be checked",
				fn.Name.Name, reclaimRegistrar)
			require.NotZerof(t, at,
				"%s: %s is the function's first statement, so it cannot follow %s",
				fn.Name.Name, reclaimRegistrar, networkAssign)

			assert.Equalf(t, networkAssign, calleeName(statementCall(fn.Body.List[at-1])),
				"%s: %s must be registered immediately after %s, the pairing both call sites are written "+
					"to express; moving it changes when the entry is reclaimed within the chain",
				fn.Name.Name, reclaimRegistrar, networkAssign)
			checked++
		}
	}

	require.NotZero(t, checked, "found no %s call to check", reclaimRegistrar)
}

// countLiveMapConstructions counts the composite literals in fn that set the
// sandboxes field — the field that puts a sandbox in the live map.
func countLiveMapConstructions(fn *ast.FuncDecl) int {
	n := 0

	ast.Inspect(fn.Body, func(node ast.Node) bool {
		lit, ok := node.(*ast.CompositeLit)
		if !ok {
			return true
		}

		for _, elt := range lit.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "sandboxes" {
				n++
			}
		}

		return true
	})

	return n
}

func countCalls(fn *ast.FuncDecl, name string) int {
	n := 0

	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok && calleeName(call) == name {
			n++
		}

		return true
	})

	return n
}

// statementIndex is the position in fn's body of the statement that calls name,
// or -1 when name is not called from a statement of the body at all. The two are
// different failures and the test reports them differently.
func statementIndex(fn *ast.FuncDecl, name string) int {
	for i, stmt := range fn.Body.List {
		if calleeName(statementCall(stmt)) == name {
			return i
		}
	}

	return -1
}

// statementCall is the call a bare expression statement makes, or nil for any
// other statement.
func statementCall(stmt ast.Stmt) *ast.CallExpr {
	expr, ok := stmt.(*ast.ExprStmt)
	if !ok {
		return nil
	}
	call, ok := expr.X.(*ast.CallExpr)
	if !ok {
		return nil
	}

	return call
}

// calleeName is the bare function or method name of a call, ignoring whatever
// it is called on. A nil call has no name, so a statement that is not a call at
// all can never match one.
func calleeName(call *ast.CallExpr) string {
	if call == nil {
		return ""
	}

	switch fun := call.Fun.(type) {
	case *ast.Ident:
		return fun.Name
	case *ast.SelectorExpr:
		return fun.Sel.Name
	}

	return ""
}
