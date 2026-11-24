package api

import (
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
)

type fakeLocalhostForwarder struct {
	mu       sync.Mutex
	on       bool
	calls    []bool
	err      error
	released int
}

func (f *fakeLocalhostForwarder) SetLocalhostForwarding(on bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.on = on
	f.calls = append(f.calls, on)

	return f.err
}

func (f *fakeLocalhostForwarder) Release() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.released++
}

func (f *fakeLocalhostForwarder) releases() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.released
}

func (f *fakeLocalhostForwarder) recorded() []bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]bool(nil), f.calls...)
}

func newAPIWithForwarder(f LocalhostForwarder) *API {
	api := newTestAPI(nil, &mockMMDSClient{hash: keys.HashAccessToken("")})
	if f != nil {
		api.SetLocalhostForwarder(f)
	}

	return api
}

func TestPostInit_ExposeLocalhostPorts(t *testing.T) {
	t.Parallel()

	t.Run("false turns forwarding off", func(t *testing.T) {
		t.Parallel()

		f := &fakeLocalhostForwarder{on: true}
		rec := postInit(t, newAPIWithForwarder(f), map[string]any{"exposeLocalhostPorts": false})

		require.Equal(t, http.StatusNoContent, rec.Code)
		assert.Equal(t, []bool{false}, f.recorded())
		assert.Equal(t, 1, f.releases(), "an authenticated /init releases a freshly booted guest")
	})

	t.Run("an incomplete off still answers 204", func(t *testing.T) {
		t.Parallel()

		f := &fakeLocalhostForwarder{on: true, err: errors.New("not recorded")}
		rec := postInit(t, newAPIWithForwarder(f), map[string]any{"exposeLocalhostPorts": false})

		require.Equal(t, http.StatusNoContent, rec.Code, "the forwarder retries what is undone")
		assert.Equal(t, []bool{false}, f.recorded())
	})

	t.Run("absent leaves the setting", func(t *testing.T) {
		t.Parallel()

		f := &fakeLocalhostForwarder{on: true}
		rec := postInit(t, newAPIWithForwarder(f), map[string]any{})

		require.Equal(t, http.StatusNoContent, rec.Code)
		assert.Empty(t, f.recorded(), "a host that does not send the field must not change forwarding")
	})

	t.Run("true turns it back on", func(t *testing.T) {
		t.Parallel()

		f := &fakeLocalhostForwarder{on: false}
		rec := postInit(t, newAPIWithForwarder(f), map[string]any{"exposeLocalhostPorts": true})

		require.Equal(t, http.StatusNoContent, rec.Code)
		assert.Equal(t, []bool{true}, f.recorded())
	})

	t.Run("without a forwarder the field is ignored", func(t *testing.T) {
		t.Parallel()

		rec := postInit(t, newAPIWithForwarder(nil), map[string]any{"exposeLocalhostPorts": false})

		require.Equal(t, http.StatusNoContent, rec.Code)
	})

	t.Run("an unauthorized call changes nothing", func(t *testing.T) {
		t.Parallel()

		f := &fakeLocalhostForwarder{on: true}
		api := newTestAPI(secureTokenPtr("real-token"), &mockMMDSClient{hash: keys.HashAccessToken("real-token")})
		api.SetLocalhostForwarder(f)

		rec := postInit(t, api, map[string]any{"accessToken": "wrong-token", "exposeLocalhostPorts": false})

		require.Equal(t, http.StatusUnauthorized, rec.Code)
		assert.Empty(t, f.recorded(), "only an authenticated /init may switch forwarding")
		assert.Zero(t, f.releases(), "only an authenticated /init may release the hold")
	})
}
