// Package bodylimit caps the request-body size per path at the server edge, so
// a process inside the VM cannot OOM-kill envd -- the sandbox control plane --
// by streaming an unbounded body into a handler that reads it into memory.
//
// It is defense-in-depth, layered UNDER the cgroup memory protection that is
// the primary guarantee against an envd OOM kill (envd.service MemoryMin /
// system.slice drop-in). The cap covers the one allocation path a handler
// controls; memory.min covers every path, including the ones a cap cannot.
//
// The policy is a default cap on every path, with an explicit exempt list for
// the endpoints that stream their body instead of buffering it. Default-deny
// means a handler added later is capped without the author remembering to; the
// single-endpoint fix it replaces protected only the one path it touched.
package bodylimit

import (
	"net/http"
	"strings"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

// DefaultMaxBytes caps any endpoint that reads its body into memory. The largest
// legitimate /init payload (env vars plus a CA bundle) is a few KiB; 1 MiB is far
// above it and cannot reject orchestrator traffic. It is defined once in the shared
// consts package because the public API derives its env-var admission bound from the
// same ceiling (consts.MaxSandboxEnvVarsBytes), so an admitted sandbox cannot have
// its /init body rejected here; the two must not drift.
const DefaultMaxBytes int64 = consts.EnvdMaxInitBodyBytes // 1 MiB

// exemptPrefixes stream their body to disk or parse it part by part, so they never
// hold the whole body in heap and must NOT be size-capped by MaxBytesReader:
//
//   - POST /files   : multipart.Part streaming / raw octet-stream to disk (upload.go)
//   - POST /upgrade : io.Copy(f, r.Body) to a fixed path (main.go)
//
// The connect RPC services carry their own framing and their own read cap
// (connect.WithReadMaxBytes, set where each handler is mounted); wrapping their
// body in a MaxBytesReader here would corrupt gRPC/connect framing, so their
// path prefixes are exempt too.
var exemptPrefixes = []string{
	"/files",
	"/upgrade",
	"/filesystem.", // connect: /filesystem.Filesystem/*
	"/process.",    // connect: /process.Process/*
}

func exempt(path string) bool {
	for _, p := range exemptPrefixes {
		if strings.HasPrefix(path, p) {
			return true
		}
	}

	return false
}

// Wrap caps the request body on every non-exempt path. MaxBytesReader also makes
// the server close the connection after the response instead of draining the
// oversized remainder, and surfaces *http.MaxBytesError on the read so a handler
// can answer 413 Request Entity Too Large (see PostInit).
func Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil && !exempt(r.URL.Path) {
			r.Body = http.MaxBytesReader(w, r.Body, DefaultMaxBytes)
		}

		next.ServeHTTP(w, r)
	})
}
