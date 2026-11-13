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
	"time"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

// DefaultMaxBytes caps any endpoint that reads its body into memory. The largest
// legitimate /init payload (env vars plus a CA bundle) is a few KiB; 1 MiB is far
// above it and cannot reject orchestrator traffic. It is defined once in the shared
// consts package because the public API derives its env-var admission bound from the
// same ceiling (consts.MaxSandboxEnvVarsBytes), so an admitted sandbox cannot have
// its /init body rejected here; the two must not drift.
const DefaultMaxBytes int64 = consts.EnvdMaxInitBodyBytes // 1 MiB

// DefaultBodyTimeout bounds how long a non-exempt (buffered) path may take to
// read its whole body. The server keeps ReadTimeout at 0 so the long-lived
// streaming paths are not cut; that leaves a slow-body slowloris on the capped
// paths -- an attacker finishes the headers fast (so ReadHeaderTimeout does not
// fire) then dribbles a sub-cap body one byte at a time, holding the connection
// and its goroutine open indefinitely (MaxBytesReader caps size, not time).
// A per-request read deadline on just the non-exempt paths closes that surface
// without touching the exempt streaming/connect paths, which legitimately read
// their body for a long time and are never wrapped here. 30s is far above the
// time to read a few-KiB /init over any real link.
const DefaultBodyTimeout = 30 * time.Second

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
//
// On the same non-exempt paths it also sets a body read deadline
// (DefaultBodyTimeout): the size cap alone does not bound a slow body, and the
// server's ReadTimeout stays 0 for the streaming paths, so without a deadline a
// capped path is still open to a slow-body slowloris. The deadline is set only
// here, on the buffered paths; the exempt streaming/connect paths are never
// wrapped and keep their unbounded read. SetReadDeadline is best-effort: a
// ResponseWriter that does not support it (ErrNotSupported) just leaves the
// behaviour as before the deadline, never failing the request.
func Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil && !exempt(r.URL.Path) {
			r.Body = http.MaxBytesReader(w, r.Body, DefaultMaxBytes)
			// Bound the time to read the (now size-capped) body. Ignore
			// ErrNotSupported so a non-deadline-capable writer degrades to the
			// prior size-only behaviour instead of erroring.
			_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(DefaultBodyTimeout))
		}

		next.ServeHTTP(w, r)
	})
}
