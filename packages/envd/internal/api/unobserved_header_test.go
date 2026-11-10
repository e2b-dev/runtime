package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The count on X-Envd-Init-Unobserved is the number of authorized /init handlers whose thaw
// ran and finished with their request context already done since the last 204 that carried
// the header or the last freeze, whichever came later. The handler whose client is still
// connected when its headers are decided carries it and resets it; one whose client is gone
// leaves it to grow. A handler whose thaw did not run -- one that asked to skip the thaw, or
// one a freeze overtook -- is never counted, and a request refused at the /init lock is not
// either.
func TestPostInit_UnobservedHeaderCountsInlineThawsNobodyRead(t *testing.T) {
	t.Parallel()

	// liveInit serves an inline /init to a live client and returns the header it carried.
	liveInit := func(t *testing.T, api *API) string {
		t.Helper()

		rec := postInitJSON(t, t.Context(), api, PostInitJSONBody{})
		require.Equal(t, http.StatusNoContent, rec.Code)
		require.Contains(t, rec.Header(), unobservedHeader, "every 204 to a live client carries the count")

		return rec.Header().Get(unobservedHeader)
	}

	t.Run("a live inline handler reports zero and keeps reporting zero", func(t *testing.T) {
		t.Parallel()

		api := newAPIWithCgroupManager(&fakeCgroupManager{})
		api.isNotFC = true

		assert.Equal(t, "0", liveInit(t, api))
		assert.Equal(t, "0", liveInit(t, api))
	})

	t.Run("a client that leaves while the thaw runs is counted on the next live response", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		mgr := &fakeCgroupManager{onUnfreeze: cancel}
		api := newAPIWithCgroupManager(mgr)
		api.isNotFC = true

		rec := postInitJSON(t, ctx, api, PostInitJSONBody{})
		require.Equal(t, http.StatusNoContent, rec.Code)
		assert.NotContains(t, rec.Header(), unobservedHeader,
			"the thaw ran before the response was decided, so the client was already gone")

		mgr.onUnfreeze = nil
		assert.Equal(t, "1", liveInit(t, api), "the handler completed with its client gone")
		assert.Equal(t, "0", liveInit(t, api), "the live response reset the count")
	})

	t.Run("a client that leaves during the thaw does not reset what earlier handlers accumulated", func(t *testing.T) {
		t.Parallel()

		mgr := &fakeCgroupManager{}
		api := newAPIWithCgroupManager(mgr)

		// Two handlers whose clients were gone before the response: the count is 2.
		deadBeforeResponse := func() {
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			api.mmdsClient = &mockMMDSClient{onGet: cancel}
			require.Equal(t, http.StatusNoContent, postInitJSON(t, ctx, api, PostInitJSONBody{}).Code)
		}
		deadBeforeResponse()
		deadBeforeResponse()

		// A third whose client leaves while its thaw runs. Had the count been written into
		// its response before the thaw, the 2 would be reset into a response nobody reads.
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		api.isNotFC = true
		mgr.onUnfreeze = cancel
		require.Equal(t, http.StatusNoContent, postInitJSON(t, ctx, api, PostInitJSONBody{}).Code)
		mgr.onUnfreeze = nil

		assert.Equal(t, "3", liveInit(t, api), "the next live response carries every unobserved handler")
	})

	t.Run("a client gone before the response is written is counted and resets nothing", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		mgr := &fakeCgroupManager{}
		api := newAPIWithCgroupManager(mgr)
		// MMDS admits the request and ends its context on the way, after the /init lock.
		api.mmdsClient = &mockMMDSClient{onGet: cancel}

		rec := postInitJSON(t, ctx, api, PostInitJSONBody{})
		require.Equal(t, http.StatusNoContent, rec.Code)
		assert.NotContains(t, rec.Header(), unobservedHeader, "a response nobody reads must not reset the count")

		rec = postInitJSON(t, ctx, api, PostInitJSONBody{})
		require.Equal(t, http.StatusServiceUnavailable, rec.Code, "a request whose context is already done never takes the /init lock")

		api.isNotFC = true
		assert.Equal(t, "1", liveInit(t, api), "the completed handler counts; the one refused at the lock does not")
	})

	t.Run("a freeze zeroes the count", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		api := newAPIWithCgroupManager(&fakeCgroupManager{})
		api.mmdsClient = &mockMMDSClient{onGet: cancel}
		require.Equal(t, http.StatusNoContent, postInitJSON(t, ctx, api, PostInitJSONBody{}).Code)

		// The pause's freeze ends the life the count belongs to; the first live /init after
		// it is the resumed start, which must not be charged with it.
		freezeAPI(t, api)
		api.isNotFC = true
		assert.Equal(t, "0", liveInit(t, api), "no count is carried across a freeze")
	})

	t.Run("a skipping handler is never counted, whatever became of its client", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		mgr := &fakeCgroupManager{}
		api := newAPIWithCgroupManager(mgr)
		api.mmdsClient = &mockMMDSClient{onGet: cancel}

		rec := postInitJSON(t, ctx, api, PostInitJSONBody{Thaw: new(Skip)})
		require.Equal(t, http.StatusNoContent, rec.Code)
		assert.Empty(t, mgr.unfreezeAttempts)

		api.isNotFC = true
		assert.Equal(t, "0", liveInit(t, api))
	})

	t.Run("a handler a freeze overtook is never counted, whatever became of its client", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		mgr := &fakeCgroupManager{}
		api := newAPIWithCgroupManager(mgr)
		// After the handler has read its generation, a freeze lands and the client leaves.
		api.mmdsClient = &mockMMDSClient{onGet: func() {
			freezeAPI(t, api)
			cancel()
		}}

		rec := postInitJSON(t, ctx, api, PostInitJSONBody{})
		require.Equal(t, http.StatusNoContent, rec.Code)
		assert.Empty(t, mgr.unfreezeAttempts, "the freeze voided this handler's thaw")

		api.mmdsClient = &mockMMDSClient{}
		api.isNotFC = true
		assert.Equal(t, "0", liveInit(t, api))
	})

	t.Run("an unauthorized request neither counts nor resets", func(t *testing.T) {
		t.Parallel()

		mgr := &fakeCgroupManager{}
		api := newAPIWithCgroupManager(mgr)
		api.isNotFC = true
		api.accessToken.TakeFrom(secureTokenPtr("real"))

		// One authorized handler whose client leaves during its thaw: the count is 1.
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		mgr.onUnfreeze = cancel
		require.Equal(t, http.StatusNoContent, postInitRaw(t, ctx, api, []byte(`{"accessToken":"real"}`)).Code)
		mgr.onUnfreeze = nil

		rec := postInitRaw(t, t.Context(), api, []byte(`{"accessToken":"wrong"}`))
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		assert.NotContains(t, rec.Header(), unobservedHeader)

		assert.Equal(t, "1", postInitRaw(t, t.Context(), api, []byte(`{"accessToken":"real"}`)).Header().Get(unobservedHeader),
			"the 401 neither added to the count nor reset it")
	})
}
