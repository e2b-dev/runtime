package proxy

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
)

// What each routing outcome answers a probe: "true" only once the sandbox was
// found, "false" only once it was looked up and not found. An error raised
// before the lookup must not read as "here", or the asker would route the
// sandbox's traffic to a proxy that does not have it.
func TestRoutePresence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		err          error
		wantPresence Presence
	}{
		{name: "routed", err: nil, wantPresence: PresenceHeld},
		{name: "not found", err: NewErrSandboxNotFound("sbx", PresenceAbsent), wantPresence: PresenceAbsent},
		{name: "wrapped not found", err: fmt.Errorf("lookup: %w", NewErrSandboxNotFound("sbx", PresenceAbsent)), wantPresence: PresenceAbsent},
		{name: "held but not routable", err: NewErrSandboxNotFound("sbx", PresenceHeld), wantPresence: PresenceHeld},
		{name: "lookup failed", err: NewErrSandboxNotFound("sbx", PresenceUnknown), wantPresence: PresenceUnknown},
		{name: "not found without a presence", err: &SandboxNotFoundError{}, wantPresence: PresenceUnknown},
		{name: "resume permission denied", err: NewErrSandboxResumePermissionDenied("sbx", nil), wantPresence: PresenceHeld},
		{name: "still transitioning", err: NewErrSandboxStillTransitioning("sbx"), wantPresence: PresenceHeld},
		{name: "still transitioning unavailable", err: NewErrSandboxStillTransitioningUnavailable("sbx"), wantPresence: PresenceHeld},
		{name: "team sandbox limit", err: NewErrSandboxResourceExhausted("sbx", "limit"), wantPresence: PresenceHeld},
		{name: "permanent placement refusal", err: NewErrSandboxPlacementRefused("sbx", "refused", "code", http.StatusServiceUnavailable), wantPresence: PresenceHeld},
		{name: "missing traffic access token", err: NewErrMissingTrafficAccessToken("sbx", trafficAccessTokenHeader), wantPresence: PresenceHeld},
		{name: "invalid traffic access token", err: NewErrInvalidTrafficAccessToken("sbx", trafficAccessTokenHeader), wantPresence: PresenceHeld},
		{name: "placement the API attempted", err: NewErrSandboxCapacity("sbx", PresenceHeld, "timed out"), wantPresence: PresenceHeld},
		{name: "capacity", err: NewErrSandboxCapacity("sbx", PresenceUnknown, "no room"), wantPresence: PresenceUnknown},
		{name: "shed", err: NewErrSandboxShed("sbx", "lot full"), wantPresence: PresenceUnknown},
		{name: "internal route", err: NewErrInternalRoute("sbx", "/init"), wantPresence: PresenceUnknown},
		{name: "invalid host", err: ErrInvalidHost, wantPresence: PresenceUnknown},
		{name: "invalid sandbox ID", err: ErrInvalidSandboxID, wantPresence: PresenceUnknown},
		{name: "invalid port", err: InvalidSandboxPortError{Port: "0"}, wantPresence: PresenceUnknown},
		{name: "missing header", err: MissingHeaderError{Header: headerSandboxID}, wantPresence: PresenceUnknown},
		{name: "unexpected", err: errors.New("redis down"), wantPresence: PresenceUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.wantPresence, routePresence(tt.err))
		})
	}
}

// headerBackend counts the requests it receives and records the probe
// header of the last one.
func headerBackend(t *testing.T) (*url.URL, *atomic.Int32, *atomic.Value) {
	t.Helper()

	var requests atomic.Int32
	var header atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		header.Store(r.Header.Get(HeaderProbe))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	u, err := url.Parse(server.URL)
	require.NoError(t, err)

	return u, &requests, &header
}

func TestProxyAnswersProbeRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		probe        bool
		err          error
		wantStatus   int
		wantAnswer   string
		wantRequests int32
	}{
		{name: "routed sandbox is answered without forwarding", probe: true, wantStatus: http.StatusNoContent, wantAnswer: "true"},
		{name: "unknown sandbox", probe: true, err: NewErrSandboxNotFound("sbx", PresenceAbsent), wantStatus: http.StatusBadGateway, wantAnswer: "false"},
		{name: "held sandbox without a route keeps the not-found page", probe: true, err: NewErrSandboxNotFound("sbx", PresenceHeld), wantStatus: http.StatusBadGateway, wantAnswer: "true"},
		{name: "failed lookup cannot tell", probe: true, err: NewErrSandboxNotFound("sbx", PresenceUnknown), wantStatus: http.StatusBadGateway},
		{name: "sandbox found but refused keeps its page", probe: true, err: NewErrSandboxResumePermissionDenied("sbx", nil), wantStatus: http.StatusForbidden, wantAnswer: "true"},
		{name: "capacity cannot tell", probe: true, err: NewErrSandboxCapacity("sbx", PresenceUnknown, "no room"), wantStatus: http.StatusServiceUnavailable},
		{name: "request without the header is forwarded", probe: false, wantStatus: http.StatusOK, wantRequests: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backendURL, requests, _ := headerBackend(t)
			p := New(0, ClientProxyRetries, 20*time.Second, func(*http.Request) (*pool.Destination, error) {
				if tt.err != nil {
					return nil, tt.err
				}

				return &pool.Destination{
					Url:           backendURL,
					SandboxId:     "sbx",
					RequestLogger: logger.NewNopLogger(),
					ConnectionKey: "probe-" + tt.name,
				}, nil
			}, nil, false, WithProbe())
			t.Cleanup(func() { require.NoError(t, p.Close()) })

			req := httptest.NewRequestWithContext(t.Context(), http.MethodHead, "http://49983-sbx.e2b.app/", nil)
			if tt.probe {
				req.Header.Set(HeaderProbe, "true")
			}
			rr := httptest.NewRecorder()
			p.Handler.ServeHTTP(rr, req)

			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Equal(t, tt.wantAnswer, rr.Header().Get(HeaderProbe))
			assert.Equal(t, tt.wantRequests, requests.Load())
		})
	}
}

// A proxy that does not answer probe requests treats the header like any
// other and forwards the request.
func TestProxyWithoutProbeForwardsTheHeader(t *testing.T) {
	t.Parallel()

	backendURL, requests, header := headerBackend(t)
	p := New(0, ClientProxyRetries, 20*time.Second, func(*http.Request) (*pool.Destination, error) {
		return &pool.Destination{
			Url:           backendURL,
			SandboxId:     "sbx",
			RequestLogger: logger.NewNopLogger(),
			ConnectionKey: "probe-disabled",
		}, nil
	}, nil, false)
	t.Cleanup(func() { require.NoError(t, p.Close()) })

	req := httptest.NewRequestWithContext(t.Context(), http.MethodHead, "http://49983-sbx.e2b.app/", nil)
	req.Header.Set(HeaderProbe, "true")
	rr := httptest.NewRecorder()
	p.Handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Empty(t, rr.Header().Get(HeaderProbe))
	assert.Equal(t, int32(1), requests.Load())
	assert.Equal(t, "true", header.Load())
}
