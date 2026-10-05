package template

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"regexp"
	"strings"

	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/cors"
)

var browserRegex = regexp.MustCompile(`(?i)mozilla|chrome|safari|firefox|edge|opera|msie`)

//go:embed browser_layout.html
var browserLayoutHtml string

// newBrowserPage parses a page into the shared browser layout. The page defines
// the "title", "tone" and "body" blocks, and may define "actions".
func newBrowserPage(name, page string) *template.Template {
	layout := template.Must(template.New(name).
		Funcs(template.FuncMap{"statusText": http.StatusText}).
		Parse(browserLayoutHtml))

	return template.Must(layout.Parse(page))
}

type jsonErrorMessage interface {
	StatusCode() int
}

type TemplatedError[T jsonErrorMessage] struct {
	template *template.Template
	vars     T
}

func (e *TemplatedError[T]) buildHtml() ([]byte, error) {
	var html bytes.Buffer

	err := e.template.Execute(&html, e.vars)
	if err != nil {
		return nil, err
	}

	return html.Bytes(), nil
}

func (e *TemplatedError[T]) buildJson() ([]byte, error) {
	return json.Marshal(e.vars)
}

func (e *TemplatedError[T]) HandleError(
	w http.ResponseWriter,
	r *http.Request,
) error {
	if e.vars.StatusCode() <= 0 {
		return fmt.Errorf("invalid status code: %d", e.vars.StatusCode())
	}

	// These responses are synthesized by the proxy, so nobody upstream can mark
	// them readable for browser JS; without this header a browser reports an
	// opaque network error instead of the status and body.
	cors.SetHeaders(w)

	if wantsHtml(r) {
		body, buildErr := e.buildHtml()
		if buildErr != nil {
			return buildErr
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(e.vars.StatusCode())
		_, writeErr := w.Write(body)
		if writeErr != nil {
			return writeErr
		}

		return nil
	}

	body, buildErr := e.buildJson()
	if buildErr != nil {
		return buildErr
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(e.vars.StatusCode())

	_, writeErr := w.Write(body)
	if writeErr != nil {
		return writeErr
	}

	return nil
}

// wantsHtml reports whether the error should be rendered as the browser error
// page rather than as JSON.
//
// Intent comes first, because a fetch() from page scripts carries the browser's
// own User-Agent and cannot override it — sniffing the User-Agent alone hands an
// HTML page to a caller that is about to parse it as JSON. Sniffing is left as
// the fallback, where it only catches genuine top-level navigations.
func wantsHtml(r *http.Request) bool {
	if prefersJson(r) || isScriptInitiated(r) {
		return false
	}

	return isBrowser(r)
}

// prefersJson reports whether the Accept header asks for JSON over HTML.
func prefersJson(r *http.Request) bool {
	accept := strings.ToLower(r.Header.Get("Accept"))

	return strings.Contains(accept, "application/json") && !strings.Contains(accept, "text/html")
}

// isScriptInitiated reports whether the request was made by page scripts rather
// than by navigating to the URL. Sec-Fetch-Mode is set by the browser itself and
// is a forbidden header name for scripts; X-Requested-With covers older clients.
func isScriptInitiated(r *http.Request) bool {
	// A top-level navigation, which is what the HTML pages are for, says so
	// outright, and is trusted over any header the caller can set.
	switch strings.ToLower(r.Header.Get("Sec-Fetch-Mode")) {
	case "cors", "no-cors", "same-origin", "websocket":
		return true
	case "navigate":
		return false
	}

	return r.Header.Get("X-Requested-With") != ""
}

func isBrowser(r *http.Request) bool {
	return browserRegex.MatchString(r.UserAgent())
}
