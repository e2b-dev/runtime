//go:build linux

package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/connlimit"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	reverseproxy "github.com/e2b-dev/infra/packages/shared/pkg/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

const testSandboxID = "im9r2ycjiy2534qsdy1oo"

// resolve routes a request for the given sandbox port and path through the
// proxy's destination resolver against an empty sandbox map, which is all the
// internal-route refusal needs: it is decided before the sandbox is looked up.
func resolve(t *testing.T, port int64, path string) error {
	t.Helper()

	resolveDestination := (&SandboxProxy{sandboxes: sandbox.NewSandboxesMap()}).resolveDestination

	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, nil)
	r.Host = fmt.Sprintf("%d-%s.e2b.app", port, testSandboxID)

	_, err := resolveDestination(r)
	require.Error(t, err, "an empty sandbox map cannot resolve a destination")

	return err
}

// TestResolveDestinationRefusesInternalEnvdRoutes covers the wiring: envd's
// control plane is refused on the envd port, and only there.
func TestResolveDestinationRefusesInternalEnvdRoutes(t *testing.T) {
	t.Parallel()

	const userPort = 8000

	for _, tt := range []struct {
		name    string
		port    int64
		path    string
		refused bool
	}{
		{name: "init on the envd port", port: consts.DefaultEnvdServerPort, path: "/init", refused: true},
		{name: "freeze on the envd port", port: consts.DefaultEnvdServerPort, path: "/freeze", refused: true},
		{name: "upgrade on the envd port", port: consts.DefaultEnvdServerPort, path: "/upgrade", refused: true},

		// The public envd surface is untouched.
		{name: "files on the envd port", port: consts.DefaultEnvdServerPort, path: "/files", refused: false},
		{name: "health on the envd port", port: consts.DefaultEnvdServerPort, path: "/health", refused: false},

		// A user process may serve whatever it likes on its own ports; envd's
		// control plane does not live there and the name is not reserved.
		{name: "init on a user port", port: userPort, path: "/init", refused: false},
		{name: "upgrade on a user port", port: userPort, path: "/upgrade", refused: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := resolve(t, tt.port, tt.path)

			var internalRouteErr *reverseproxy.InternalRouteError
			if !tt.refused {
				assert.NotErrorAs(t, err, &internalRouteErr)

				return
			}

			require.ErrorAs(t, err, &internalRouteErr)
			assert.Equal(t, tt.path, internalRouteErr.Path)
			assert.Equal(t, testSandboxID, internalRouteErr.SandboxId)
		})
	}
}

// TestResolveDestinationRefusesInternalRoutesBeforeSandboxLookup pins the
// ordering the refusal relies on: a request for a control route is answered the
// same way whether or not the sandbox is on this node, so the reply reveals
// nothing about which sandboxes are here.
func TestResolveDestinationRefusesInternalRoutesBeforeSandboxLookup(t *testing.T) {
	t.Parallel()

	var notFoundErr *reverseproxy.SandboxNotFoundError

	err := resolve(t, consts.DefaultEnvdServerPort, "/init")
	assert.NotErrorAs(t, err, &notFoundErr, "the refusal must not depend on the sandbox being absent")

	err = resolve(t, consts.DefaultEnvdServerPort, "/files")
	assert.ErrorAs(t, err, &notFoundErr, "a public route on an absent sandbox still reports the sandbox missing")
}

// A full-router sandbox is refused even with a valid traffic access token,
// and no connection reaches the guest. Egress-router still reaches it.
func TestSandboxProxyRefusesFullRouterIngress(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name      string
		placement *orchestrator.SandboxNetworkPlacement
		refused   bool
	}{
		{name: "egress-router", placement: &orchestrator.SandboxNetworkPlacement{ExecutionId: "execution", NetworkMode: "egress-router", RouterId: "router"}},
		{name: "full-router", placement: &orchestrator.SandboxNetworkPlacement{ExecutionId: "execution", NetworkMode: "full-router", RouterId: "router", IngressAddress: "127.0.0.1", IngressPort: 5010}, refused: true},
		{name: "full-router without its Router", placement: &orchestrator.SandboxNetworkPlacement{NetworkMode: "full-router"}, refused: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var accepted atomic.Int32
			guest := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte("guest"))
			}))
			guest.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					accepted.Add(1)
				}
			}
			guest.Start()
			defer guest.Close()
			address, err := url.Parse(guest.URL)
			require.NoError(t, err)

			sandboxes := sandbox.NewSandboxesMap()
			p := &SandboxProxy{sandboxes: sandboxes, owners: make(map[string]*pool.Owner), limiter: connlimit.NewConnectionLimiter(), retirement: retirementTimeout}
			p.proxy = reverseproxy.New(0, 1, time.Minute, p.resolveDestination, nil, true, nil)
			defer p.proxy.Close()
			frontend := httptest.NewServer(p.proxy.Handler)
			defer frontend.Close()

			token := "test-traffic-token"
			sbx := &sandbox.Sandbox{
				LifecycleID: "lifetime",
				Metadata: &sandbox.Metadata{
					Config:  sandbox.NewConfig(sandbox.Config{Network: &orchestrator.SandboxNetworkConfig{Ingress: &orchestrator.SandboxNetworkIngressConfig{TrafficAccessToken: &token}}}),
					Runtime: sandboxtypes.RuntimeMetadata{SandboxID: testSandboxID, ExecutionID: "execution"},
				},
				Resources:        &sandbox.Resources{Slot: &network.Slot{HostIP: net.ParseIP("127.0.0.1")}},
				NetworkPlacement: tt.placement,
			}
			require.NoError(t, sandboxes.MarkRunning(t.Context(), sbx))

			r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, frontend.URL, nil)
			require.NoError(t, err)
			r.Host = fmt.Sprintf("%s-%s.e2b.app", address.Port(), testSandboxID)
			r.Header.Set("e2b-traffic-access-token", token)
			response, err := http.DefaultClient.Do(r)
			require.NoError(t, err)
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())

			if !tt.refused {
				require.Equal(t, http.StatusOK, response.StatusCode)
				assert.Equal(t, "guest", string(body))
				assert.EqualValues(t, 1, accepted.Load())

				return
			}
			require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
			assert.Contains(t, string(body), `"error_code":"`+proxygrpc.PlacementFeatureUnsupported+`"`)
			assert.Empty(t, response.Header.Get("Retry-After"))
			assert.Zero(t, accepted.Load(), "no connection reaches the guest")
		})
	}
}

func TestSandboxProxyResetAndLifecycleRetirement(t *testing.T) {
	t.Parallel()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ready\n"))
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
	}))
	defer backend.Close()
	address, err := url.Parse(backend.URL)
	require.NoError(t, err)
	sandboxes := sandbox.NewSandboxesMap()
	p := &SandboxProxy{sandboxes: sandboxes, owners: make(map[string]*pool.Owner), limiter: connlimit.NewConnectionLimiter(), retirement: 50 * time.Millisecond}
	// A gate holds one real handler after owner admission but before limiter
	// acquisition. Each holder defers its release so that, on a failed
	// assertion, it runs before the deferred server closes wait on the handler.
	var gate atomic.Pointer[limitGate]
	p.proxy = reverseproxy.New(0, 1, time.Minute, p.resolveDestination, &reverseproxy.ConnectionLimitConfig{Limiter: p.limiter, GetMaxLimit: func(context.Context) int {
		if g := gate.Load(); g != nil {
			g.reached.Do(func() { close(g.reachedCh) })
			<-g.releaseCh
		}

		return 8
	}}, true, nil)
	defer p.proxy.Close()
	hold := func() *limitGate {
		g := &limitGate{reachedCh: make(chan struct{}), releaseCh: make(chan struct{})}
		gate.Store(g)

		return g
	}
	sandboxes.Subscribe(p)
	frontend := httptest.NewServer(p.proxy.Handler)
	defer frontend.Close()
	token := "test-traffic-token"
	sbx := &sandbox.Sandbox{LifecycleID: "lifetime-one", Metadata: &sandbox.Metadata{Config: sandbox.NewConfig(sandbox.Config{Network: &orchestrator.SandboxNetworkConfig{Ingress: &orchestrator.SandboxNetworkIngressConfig{TrafficAccessToken: &token, HttpsPorts: []uint32{8443}}}}), Runtime: sandboxtypes.RuntimeMetadata{SandboxID: testSandboxID, ExecutionID: "same-execution"}}, Resources: &sandbox.Resources{Slot: &network.Slot{HostIP: net.ParseIP("127.0.0.1")}}}
	require.NoError(t, sandboxes.MarkRunning(t.Context(), sbx))
	sandboxes.AssignNetwork(t.Context(), sbx)
	releaseNetwork := sandboxes.NetworkRelease(testSandboxID, sbx.LifecycleID)
	request := func() *http.Request {
		r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, frontend.URL, nil)
		require.NoError(t, err)
		r.Host = fmt.Sprintf("%s-%s.e2b.app", address.Port(), testSandboxID)
		r.Header.Set("e2b-traffic-access-token", token)

		return r
	}
	stream := func() *http.Response {
		response, err := http.DefaultClient.Do(request())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode)
		data := make([]byte, 6)
		_, err = io.ReadFull(response.Body, data)
		require.NoError(t, err)
		assert.Equal(t, "ready\n", string(data))

		return response
	}
	policyRequest := request()
	policyRequest.Header.Del("e2b-traffic-access-token")
	_, err = p.resolveDestination(policyRequest)
	var missing *reverseproxy.MissingTrafficAccessTokenError
	require.ErrorAs(t, err, &missing)
	policyRequest.Header.Set("e2b-traffic-access-token", token)
	policyRequest.Host = "8443-" + testSandboxID + ".e2b.app"
	policy, err := p.resolveDestination(policyRequest)
	require.NoError(t, err)
	assert.Equal(t, "https", policy.Url.Scheme)
	first := stream()
	stale, err := p.resolveDestination(request())
	require.NoError(t, err)
	require.NoError(t, p.RemoveFromPool(t.Context(), sbx.LifecycleID))
	_, err = io.ReadAll(first.Body)
	require.Error(t, err)
	require.NoError(t, first.Body.Close())
	assert.Zero(t, p.limiter.Count(sbx.LifecycleID))
	// The envd reset admits another generation in the same live lifecycle.
	second := stream()
	current, err := p.resolveDestination(request())
	require.NoError(t, err)
	require.NotSame(t, stale.Owner, current.Owner)
	held := func() (*limitGate, <-chan error) {
		g := hold()
		blocked := make(chan error, 1)
		r := request()
		go func() {
			response, err := http.DefaultClient.Do(r)
			if response != nil {
				_ = response.Body.Close()
			}
			blocked <- err
		}()
		<-g.reachedCh
		gate.Store(nil)

		return g, blocked
	}
	// An envd reset whose old generation does not settle in time still lets
	// the live lifecycle admit into a fresh owner.
	g, blocked := held()
	defer g.release() // before the deferred server closes wait on the handler
	require.ErrorIs(t, p.RemoveFromPool(t.Context(), sbx.LifecycleID), context.DeadlineExceeded)
	fresh, err := p.resolveDestination(request())
	require.NoError(t, err)
	require.NotSame(t, current.Owner, fresh.Owner)
	_, err = io.ReadAll(second.Body)
	require.Error(t, err)
	require.NoError(t, second.Body.Close())
	g.release()
	require.NoError(t, <-blocked)
	second = stream()
	g, blocked = held()
	defer g.release()
	require.True(t, sandboxes.MarkStopping(t.Context(), testSandboxID, sbx.LifecycleID))
	// In production order, the slot's release reaches the proxy through the
	// map after the stop. It waits for the held handler on the proxy's own
	// budget: it fails with the budget's deadline, which keeps the slot held,
	// and a retried release succeeds once the handler released. The caller's
	// cancellation does not shorten the wait.
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, releaseNetwork(t.Context(), "127.0.0.1"), context.DeadlineExceeded)
	require.ErrorIs(t, p.OnNetworkRelease(canceled, sbx), context.DeadlineExceeded)
	g.release()
	require.NoError(t, <-blocked)
	require.NoError(t, releaseNetwork(t.Context(), "127.0.0.1"))
	_, err = io.ReadAll(second.Body)
	require.Error(t, err)
	require.NoError(t, second.Body.Close())
	_, err = p.resolveDestination(request())
	require.Error(t, err)
	// Resume retains execution identity but has a new lifecycle and owner.
	next := &sandbox.Sandbox{LifecycleID: "lifetime-two", Metadata: sbx.Metadata, Resources: sbx.Resources}
	require.NoError(t, sandboxes.MarkRunning(t.Context(), next))
	third := stream()
	require.NoError(t, p.OnNetworkRelease(t.Context(), sbx), "late old callback must not stop successor")
	assert.EqualValues(t, 1, p.limiter.Count(next.LifecycleID))
	require.True(t, sandboxes.MarkStopping(t.Context(), testSandboxID, next.LifecycleID))
	// The stop alone resets the lifecycle's streams, before any release.
	streamEnded := make(chan error, 1)
	thirdBody := third.Body
	go func() {
		_, err := io.ReadAll(thirdBody)
		streamEnded <- err
	}()
	select {
	case err := <-streamEnded:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		require.Fail(t, "the stream survived the stop; only the release would have ended it")
	}
	require.NoError(t, third.Body.Close())
	require.NoError(t, p.OnNetworkRelease(t.Context(), next))
	assert.Zero(t, p.proxy.CurrentPoolConnections())
}

// limitGate holds a handler inside the limiter's limit lookup.
type limitGate struct {
	reached   sync.Once
	reachedCh chan struct{}
	released  sync.Once
	releaseCh chan struct{}
}

func (g *limitGate) release() { g.released.Do(func() { close(g.releaseCh) }) }
