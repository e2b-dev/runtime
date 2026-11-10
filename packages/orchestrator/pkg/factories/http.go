//go:build linux

package factories

import (
	"net/http"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/localupload"
)

func NewHTTPServer() *http.Server {
	return &http.Server{}
}

func newHTTPHandler(healthHandler http.Handler, uploadHandler *localupload.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/health", healthHandler)
	mux.Handle("/live", healthHandler)
	if uploadHandler != nil {
		mux.Handle("/upload", uploadHandler)
	}

	return mux
}
