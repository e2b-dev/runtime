// Temporary, loopback-only ingress for two separate HTTPS quick tunnels.
// Build with: GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o cathedral-ingress ingress-proxy.go
package main

import (
	"crypto/subtle"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

var (
	operationKey = `[A-Za-z0-9._:-]{8,128}`
	sandboxID    = `[A-Za-z0-9_-]+`
	createRead   = regexp.MustCompile(`^/v1/cathedral/operations/` + operationKey + `$`)
	deleteRead   = regexp.MustCompile(`^/v1/cathedral/lifecycle-operations/` + operationKey + `$`)
	identityRead = regexp.MustCompile(`^/v1/cathedral/sandboxes/` + sandboxID + `/identity$`)
	deleteWrite  = regexp.MustCompile(`^/v1/cathedral/sandboxes/` + sandboxID + `/lifecycle-operations$`)
	sandboxRead  = regexp.MustCompile(`^/sandboxes/` + sandboxID + `$`)
	timeoutWrite = regexp.MustCompile(`^/sandboxes/` + sandboxID + `/timeout$`)
	guestID      = regexp.MustCompile(`^` + sandboxID + `$`)
	guestRPC     = regexp.MustCompile(`^/(process\.Process|filesystem\.Filesystem)/[A-Za-z]+$`)
)

func allowedControl(r *http.Request) bool {
	// net/http decodes percent escapes into Path. A colon in an operation key
	// is valid even when the caller quotes it as %3A. Encoded slashes become
	// separators and cannot satisfy the anchored key/ID patterns below.
	path := r.URL.Path
	if strings.Contains(path, "%") || r.URL.RawQuery != "" && !(r.Method == http.MethodGet && path == "/v2/templates" && r.URL.RawQuery == "limit=100") {
		return false
	}
	switch {
	case r.Method == http.MethodGet && (path == "/v1/cathedral/capabilities" ||
		createRead.MatchString(path) || deleteRead.MatchString(path) || identityRead.MatchString(path) ||
		sandboxRead.MatchString(path) || path == "/v2/templates"):
		return path != "/v2/templates" || r.URL.RawQuery == "limit=100"
	case r.Method == http.MethodPost && r.URL.RawQuery == "" &&
		(path == "/sandboxes" || deleteWrite.MatchString(path) || timeoutWrite.MatchString(path)):
		return true
	default:
		return false
	}
}

func allowedGuest(r *http.Request) bool {
	path := r.URL.Path
	if strings.Contains(path, "%") {
		return false
	}
	if guestRPC.MatchString(path) {
		return r.Method == http.MethodPost && r.URL.RawQuery == ""
	}
	if path == "/files" {
		return r.Method == http.MethodGet || r.Method == http.MethodPost
	}
	return path == "/files/compose" && r.Method == http.MethodPost && r.URL.RawQuery == ""
}

func singleHeader(h http.Header, name string) string {
	values := h.Values(name)
	if len(values) != 1 || strings.TrimSpace(values[0]) == "" || strings.Contains(values[0], ",") {
		return ""
	}
	return values[0]
}

func guestRootAuth(r *http.Request) bool {
	// The SDK sends Basic root: for Connect process RPCs. envd selects the
	// command user from it; stripping it runs commands as the template default.
	values := r.Header.Values("Authorization")
	if len(values) == 0 {
		return true // Some envd file requests authenticate with the access token alone.
	}
	if len(values) != 1 || strings.Contains(values[0], ",") {
		return false
	}
	username, password, ok := r.BasicAuth()
	return ok && username == "root" && password == ""
}

func proxy(target string) *httputil.ReverseProxy {
	u, _ := url.Parse(target)
	p := httputil.NewSingleHostReverseProxy(u)
	baseDirector := p.Director
	p.Director = func(r *http.Request) {
		baseDirector(r)
		r.Host = u.Host // client-proxy uses IP Host plus routing headers.
		for _, header := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Real-IP", "CF-Connecting-IP"} {
			r.Header.Del(header)
		}
	}
	p.ErrorLog = log.New(io.Discard, "", 0)
	p.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "local runtime unavailable", http.StatusBadGateway)
	}
	return p
}

func handler(mode string, key []byte, upstream http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if mode == "control" {
			presented := singleHeader(r.Header, "X-API-Key")
			if presented == "" || subtle.ConstantTimeCompare([]byte(presented), key) != 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			if !allowedControl(r) {
				http.NotFound(w, r)
				return
			}
			r.Header.Del("Authorization")
			r.Header.Del("X-Admin-Token")
			r.Header.Del("Cookie")
			r.Body = http.MaxBytesReader(w, r.Body, 256<<10)
		} else {
			id := singleHeader(r.Header, "E2b-Sandbox-Id")
			if !guestID.MatchString(id) || singleHeader(r.Header, "E2b-Sandbox-Port") != "49983" ||
				singleHeader(r.Header, "X-Access-Token") == "" || !guestRootAuth(r) {
				http.Error(w, "sandbox credentials required", http.StatusUnauthorized)
				return
			}
			if !allowedGuest(r) {
				http.NotFound(w, r)
				return
			}
			r.Header.Del("X-API-Key")
			r.Header.Del("Cookie")
			r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
		}
		upstream.ServeHTTP(w, r)
	})
}

func listenLoopback(port string, h http.Handler) (*http.Server, net.Listener, error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:"+port)
	if err != nil {
		return nil, nil, err
	}
	server := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second,
		MaxHeaderBytes: 16 << 10,
		ErrorLog:       log.New(io.Discard, "", 0)}
	return server, listener, nil
}

func main() {
	keyFile := flag.String("api-key-file", "", "path to the existing seeded team API key")
	flag.Parse()
	if *keyFile == "" {
		fmt.Fprintln(os.Stderr, "api-key-file is required")
		os.Exit(2)
	}
	key, err := os.ReadFile(*keyFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "API key file is unreadable")
		os.Exit(1)
	}
	key = []byte(strings.TrimSpace(string(key)))
	if len(key) < 32 || strings.ContainsAny(string(key), "\r\n") {
		fmt.Fprintln(os.Stderr, "API key file is invalid")
		os.Exit(1)
	}
	control, controlListener, err := listenLoopback("13000", handler("control", key, proxy("http://127.0.0.1:3000")))
	if err != nil {
		fmt.Fprintln(os.Stderr, "control listener unavailable")
		os.Exit(1)
	}
	guest, guestListener, err := listenLoopback("13002", handler("guest", nil, proxy("http://127.0.0.1:3002")))
	if err != nil {
		_ = controlListener.Close()
		fmt.Fprintln(os.Stderr, "guest listener unavailable")
		os.Exit(1)
	}
	go func() { _ = control.Serve(controlListener) }()
	if err := guest.Serve(guestListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, "guest listener failed")
		os.Exit(1)
	}
}
