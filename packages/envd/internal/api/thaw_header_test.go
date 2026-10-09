package api

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
)

// The thaw field is read on the exact value "skip" and nothing else. Absent, "inline" and an
// unknown value all keep today's path: the handler thaws before it answers and the response
// carries no thaw header, which is also what an envd predating the field answers.
func TestPostInit_ThawFieldOtherThanSkipThawsInline(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		thaw *PostInitJSONBodyThaw
	}{
		{name: "absent", thaw: nil},
		{name: "inline", thaw: new(Inline)},
		{name: "unknown value", thaw: new(PostInitJSONBodyThaw("later"))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mgr := &fakeCgroupManager{}
			api := newAPIWithCgroupManager(mgr)
			api.isNotFC = true

			rec := postInitJSON(t, t.Context(), api, PostInitJSONBody{Thaw: tt.thaw})

			require.Equal(t, http.StatusNoContent, rec.Code)
			assert.Equal(t, cgroups.WorkloadProcessTypes, mgr.unfrozen, "the handler must thaw before it answers")
			assert.NotContains(t, rec.Header(), thawHeader, "an inline thaw carries no thaw header")
		})
	}
}

// thaw: "skip" installs no thaw: the handler answers 204 with X-Envd-Thaw: skip and leaves the
// frozen set as the freeze left it.
func TestPostInit_SkipLeavesTheWorkloadFrozen(t *testing.T) {
	t.Parallel()

	mgr := &fakeCgroupManager{}
	api := newAPIWithCgroupManager(mgr)
	api.isNotFC = true
	freezeAPI(t, api)

	rec := postInitJSON(t, t.Context(), api, PostInitJSONBody{Thaw: new(Skip)})

	require.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, "skip", rec.Header().Get(thawHeader))
	assert.Empty(t, mgr.unfreezeAttempts, "a skipping /init thaws nothing")
	assert.Equal(t, cgroups.WorkloadProcessTypes, mgr.frozen)
}

// A skipping /init leaves the thaw watchdog armed by the freeze: nothing disarmed it, so it
// still fires.
func TestPostInit_SkipLeavesTheWatchdogArmed(t *testing.T) {
	t.Parallel()

	mgr := &fakeCgroupManager{}
	api := newAPIWithCgroupManager(mgr)
	api.isNotFC = true

	fired := make(chan cgroups.ThawResult, 1)
	api.workloadFreezer.SetThawWatchdog(20*time.Millisecond, func(res cgroups.ThawResult, _ error) { fired <- res })
	freezeAPI(t, api)

	rec := postInitJSON(t, t.Context(), api, PostInitJSONBody{Thaw: new(Skip)})
	require.Equal(t, http.StatusNoContent, rec.Code)

	select {
	case <-fired:
		assert.Equal(t, cgroups.WorkloadProcessTypes, mgr.unfrozen, "the watchdog's thaw is the only one that ran")
	case <-time.After(5 * time.Second):
		t.Fatal("the watchdog did not fire: the skipping /init disarmed it")
	}
}

// An /init that fails after auth thaws inline through the deferred call installed before it,
// as before the thaw field existed, so the start fails thawed; one that asked to skip thaws
// nothing and still says so on its error response. The failure here is SetData's CA install
// refusing a context that ended before it ran.
func TestPostInit_FailingAfterAuth(t *testing.T) {
	t.Parallel()

	bundle := "-----BEGIN CERTIFICATE-----\nnot a certificate\n-----END CERTIFICATE-----"

	for _, tt := range []struct {
		name       string
		thaw       *PostInitJSONBodyThaw
		wantThawed []cgroups.ProcessType
		wantHeader string
	}{
		{name: "inline thaws", thaw: nil, wantThawed: cgroups.WorkloadProcessTypes},
		{name: "skip thaws nothing", thaw: new(Skip), wantThawed: nil, wantHeader: "skip"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			mgr := &fakeCgroupManager{}
			api := newAPIWithCgroupManager(mgr)
			// MMDS answers "unconfigured", which admits a first-time /init, and ends the
			// request context on the way: after the /init lock, before SetData.
			api.mmdsClient = &mockMMDSClient{onGet: cancel}

			rec := postInitJSON(t, ctx, api, PostInitJSONBody{Thaw: tt.thaw, CaBundle: &bundle})

			require.Equal(t, http.StatusServiceUnavailable, rec.Code, "the CA install refuses the ended context")
			assert.Equal(t, tt.wantThawed, mgr.unfrozen)
			assert.Equal(t, tt.wantHeader, rec.Header().Get(thawHeader))
		})
	}
}
