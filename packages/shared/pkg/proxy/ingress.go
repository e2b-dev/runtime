package proxy

import (
	"crypto/subtle"
	"net/http"
	"strconv"
	"strings"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
)

const trafficAccessTokenHeader = "e2b-traffic-access-token"

// IngressTarget is the sandbox a request is for, as its owner resolved it.
type IngressTarget struct {
	// Destination is where to reach the sandbox. Url names the host and port
	// and is the lookup's own allocation: ResolveIngress sets its scheme.
	Destination pool.Destination
	// Config is the ingress policy the API admitted for the sandbox. nil is
	// a sandbox with no ingress settings, reached open over plain HTTP, so a
	// lookup that cannot load the sandbox's configuration must fail rather
	// than answer with none.
	Config *orchestrator.SandboxNetworkIngressConfig
}

// ResolveIngress applies the guest ingress policy to a request and returns the
// destination it may be forwarded to. lookup resolves a sandbox ID and port to
// its target; it runs after the request is known to address a public route.
//
// envd serves the orchestrator's control plane (/init, the pause freeze and
// thaw hooks, the live self-upgrade) on the same port as the sandbox's public
// surface. The orchestrator calls those routes directly at the sandbox's
// address and never through a proxy, so a request for one that arrives here
// came from outside and has no business reaching envd: envd's own access
// token check would let the sandbox's owner wedge or re-image their guest,
// and /init is exempt from that check altogether. They are refused before
// the lookup, so the answer is the same whether or not the addressed sandbox
// is known here, and the reply carries nothing about which sandboxes exist.
//
// The traffic access token guards every other port. envd's port is exempt:
// it has its own access validation. Preflights fail the check too, since the
// Fetch standard forbids a preflight from carrying custom headers; the
// handler answers them from the resulting error, so no unauthenticated
// request reaches the guest port.
func ResolveIngress(r *http.Request, lookup func(sandboxID string, port uint64) (IngressTarget, error)) (*pool.Destination, error) {
	id, port, err := GetTargetFromRequest()(r)
	if err != nil {
		return nil, err
	}
	envd := port == uint64(consts.DefaultEnvdServerPort)
	if envd && isInternalEnvdPath(r.URL.Path) {
		return nil, NewErrInternalRoute(id, r.URL.Path)
	}
	target, err := lookup(id, port)
	if err != nil {
		return nil, err
	}
	config := target.Config
	if token := config.GetTrafficAccessToken(); !envd && token != "" {
		supplied := r.Header.Get(trafficAccessTokenHeader)
		if supplied == "" {
			return nil, NewErrMissingTrafficAccessToken(id, trafficAccessTokenHeader)
		}
		if subtle.ConstantTimeCompare([]byte(supplied), []byte(token)) != 1 {
			return nil, NewErrInvalidTrafficAccessToken(id, trafficAccessTokenHeader)
		}
	}
	d := target.Destination
	d.SandboxId, d.SandboxPort = id, port
	d.MaskRequestHost = nil
	if host := config.GetMaskRequestHost(); !envd && host != "" {
		host = strings.ReplaceAll(host, pool.MaskRequestHostPortPlaceholder, strconv.FormatUint(port, 10))
		d.MaskRequestHost = &host
	}
	// envd speaks plaintext, so its port stays HTTP even when the config
	// lists it. The API refuses that at create time; this covers direct gRPC
	// callers.
	d.Url.Scheme = "http"
	for _, httpsPort := range config.GetHttpsPorts() {
		if !envd && uint64(httpsPort) == port {
			d.Url.Scheme = "https"
		}
	}
	// Guest certificates are self-signed, so verification cannot succeed.
	// Safe to decide per destination because the ingress settings are fixed
	// for the lifecycle the pool keys its clients by.
	d.InsecureSkipTLSVerify = len(config.GetHttpsPorts()) > 0

	return &d, nil
}
