//go:build linux

package sandbox

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	reclaimRegistrar = "reclaimLiveEntryOnCleanup"
	networkAssign    = "AssignNetwork"

	// ctx, cleanup, sandboxID, lifecycleID, sandboxType.
	reclaimRegistrarArgs = 5
	sandboxTypeField     = "SandboxType"

	counterSerializer = "serializeUnstoppedCounter"
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

// TestEveryFactoryRegistersTheReclaimOwnerWithItsSandboxType pins the argument the
// owner labels its counter with. A factory that registers the owner but passes no
// sandbox type — or passes a literal instead of its own runtime metadata — mislabels
// every increment on that path, and no behavioural test can see it: the counter is
// emitted either way and the label is only wrong, never absent. The build tree is
// what makes that expensive, since it reaches the counted branch on every layer.
func TestEveryFactoryRegistersTheReclaimOwnerWithItsSandboxType(t *testing.T) {
	t.Parallel()

	files := packageFiles(t)
	checked := 0

	for _, path := range files {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoErrorf(t, err, "parse %s", path)

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}

			for _, call := range callsTo(fn, reclaimRegistrar) {
				require.Lenf(t, call.Args, reclaimRegistrarArgs,
					"%s: %s takes %d arguments; the last is the sandbox type its counter is labelled with",
					fn.Name.Name, reclaimRegistrar, reclaimRegistrarArgs)

				assert.Equalf(t, sandboxTypeField, selectorField(call.Args[reclaimRegistrarArgs-1]),
					"%s: %s must be passed the sandbox's own %s, not a literal or a constant; "+
						"the label is what separates build traffic from customer traffic",
					fn.Name.Name, reclaimRegistrar, sandboxTypeField)
				checked++
			}
		}
	}

	require.NotZero(t, checked, "found no %s call to check", reclaimRegistrar)
}

// TestEveryOwnedCleanupTestSerializesTheCounter fails when a test registers the
// reclaim owner and never claims the counter. It checks reachability, not
// ordering: a test that read its baseline before taking the lock would pass here,
// and only the delta it then computes would show it.
//
// Registering the owner puts the counter's increment on whatever the test then
// does with Close, whether or not the test cares — it fires when the chain finds
// the entry still live, which is a property of the fixture and can change without
// the test's subject changing. The counter carries no per-run attribute by design,
// so every assertion about it is a delta against a process-wide cumulative reader,
// and one parallel test left outside the lock races every one of those deltas,
// intermittently, in a test that looks unrelated to the one that fails. Four tests
// predating the counter register the owner, three of them only through a shared
// helper — so a reviewer reading the diff that added the counter sees neither
// those three nor what they do to it.
//
// This is a source-level guard for the same reason its neighbours are: nothing
// behavioural can observe a missing lock, only a flake.
func TestEveryOwnedCleanupTestSerializesTheCounter(t *testing.T) {
	t.Parallel()

	files := packageTestFiles(t)
	require.NotEmpty(t, files, "found no test sources to scan")

	bodies := map[string]*ast.FuncDecl{}
	for _, path := range files {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoErrorf(t, err, "parse %s", path)

		for _, decl := range file.Decls {
			// Methods are skipped rather than keyed: this map is keyed by bare
			// name, and only top-level function names are unique within a
			// package — three OnStopping methods already share one in this
			// very directory. Tests and their helpers are plain functions, so
			// nothing checkable is lost.
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil && fn.Recv == nil {
				bodies[fn.Name.Name] = fn
			}
		}
	}

	// A test reaches either helper through however many layers of its own, so both
	// sets are closed over calls between functions defined in these files.
	owners := reachers(bodies, reclaimRegistrar)
	serializers := reachers(bodies, counterSerializer)
	require.NotEmpty(t, owners, "found no test registering %s; has it been renamed?", reclaimRegistrar)

	for name := range owners {
		if !strings.HasPrefix(name, "Test") {
			continue
		}

		assert.Containsf(t, serializers, name,
			"%s registers %s, which moves the unstopped counter, but never calls %s; "+
				"its increments race every other test's delta reading",
			name, reclaimRegistrar, counterSerializer)
	}
}

// reachers is every function in bodies that calls target, directly or through
// another function in bodies.
func reachers(bodies map[string]*ast.FuncDecl, target string) map[string]struct{} {
	out := map[string]struct{}{}

	for grew := true; grew; {
		grew = false
		for name, fn := range bodies {
			if _, seen := out[name]; seen {
				continue
			}

			reaches := countCalls(fn, target) > 0
			for reached := range out {
				if reaches {
					break
				}
				reaches = countCalls(fn, reached) > 0
			}

			if reaches {
				out[name] = struct{}{}
				grew = true
			}
		}
	}

	return out
}

// packageTestFiles is this package's test sources — the files packageFiles filters
// out, though not on the same scope: packageFiles recurses.
//
// This one does not descend, and that is load-bearing rather than tidiness. Its
// caller builds one map keyed by bare function name, and Go only guarantees those
// unique within a package. Recursing would fold in the subpackages' test files —
// 115 of them against this directory's 31 — where TestMain already repeats three
// times over, so a future test here colliding with one of them would have the
// wrong body checked and the guard would pass vacuously. That is the failure it
// exists to prevent.
func packageTestFiles(t *testing.T) []string {
	t.Helper()

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	var out []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}

		out = append(out, e.Name())
	}

	return out
}

// selectorField is the field name a selector expression reads (the "SandboxType"
// of "runtime.SandboxType"), or "" for anything that is not one — a literal, a
// constant, a call. Those are exactly the arguments this test exists to reject.
func selectorField(expr ast.Expr) string {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return ""
	}

	return sel.Sel.Name
}

// callsTo is every call to name made anywhere inside fn.
func callsTo(fn *ast.FuncDecl, name string) []*ast.CallExpr {
	var out []*ast.CallExpr

	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok && calleeName(call) == name {
			out = append(out, call)
		}

		return true
	})

	return out
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
