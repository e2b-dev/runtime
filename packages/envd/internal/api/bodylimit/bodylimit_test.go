package bodylimit

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// readAllHandler reads the whole body (like PostInit) and maps a MaxBytesError to
// 413, any other read error to 400, and success to 200.
func readAllHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

func do(t *testing.T, path string, size int) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(strings.Repeat("a", size)))
	rec := httptest.NewRecorder()
	Wrap(readAllHandler()).ServeHTTP(rec, req)
	return rec.Code
}

func TestWrap_RejectsOversizedBodyOnCappedPath(t *testing.T) {
	if code := do(t, "/init", int(DefaultMaxBytes)+1); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("one byte over the cap: got %d, want 413", code)
	}
}

func TestWrap_AcceptsBodyAtCap(t *testing.T) {
	if code := do(t, "/init", int(DefaultMaxBytes)); code != http.StatusOK {
		t.Fatalf("body at the cap: got %d, want 200", code)
	}
}

func TestWrap_ExemptPathNotCapped(t *testing.T) {
	for _, path := range []string{"/files", "/upgrade", "/filesystem.Filesystem/Stat", "/process.Process/Start"} {
		if code := do(t, path, int(DefaultMaxBytes)+1024); code != http.StatusOK {
			t.Fatalf("exempt path %q should not be capped: got %d, want 200", path, code)
		}
	}
}

func TestExempt(t *testing.T) {
	cases := map[string]bool{
		"/init":                       false,
		"/metrics":                    false,
		"/health":                     false,
		"/files":                      true,
		"/files/anything":             true,
		"/upgrade":                    true,
		"/filesystem.Filesystem/Stat": true,
		"/process.Process/Start":      true,
	}
	for path, want := range cases {
		if got := exempt(path); got != want {
			t.Errorf("exempt(%q) = %v, want %v", path, got, want)
		}
	}
}

// deadlineRecorder is an httptest.ResponseRecorder that also satisfies the
// SetReadDeadline probe http.NewResponseController uses, so Wrap's best-effort
// deadline call is observable instead of hitting ErrNotSupported.
type deadlineRecorder struct {
	*httptest.ResponseRecorder
	deadlineSet bool
}

func (d *deadlineRecorder) SetReadDeadline(time.Time) error {
	d.deadlineSet = true

	return nil
}

func didSetReadDeadline(t *testing.T, path string) bool {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("a"))
	rec := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	Wrap(readAllHandler()).ServeHTTP(rec, req)

	return rec.deadlineSet
}

// A non-exempt (buffered) path gets a body read deadline, closing the slow-body
// slowloris that the size cap alone leaves open while ReadTimeout stays 0.
func TestWrap_SetsBodyDeadlineOnCappedPath(t *testing.T) {
	if !didSetReadDeadline(t, "/init") {
		t.Fatal("a capped path must get a body read deadline")
	}
}

// Exempt streaming/connect paths must NOT get the deadline: they legitimately
// read their body for a long time and are never wrapped.
func TestWrap_NoBodyDeadlineOnExemptPath(t *testing.T) {
	for _, path := range []string{"/files", "/upgrade", "/filesystem.Filesystem/Stat", "/process.Process/Start"} {
		if didSetReadDeadline(t, path) {
			t.Fatalf("exempt path %q must not get a body read deadline", path)
		}
	}
}
