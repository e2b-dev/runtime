package guest

import (
	"context"
	"os"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

// flagsOn answers every flag read with its value.
type flagsOn bool

func (f flagsOn) BoolFlag(context.Context, featureflags.BoolFlag, ...ldcontext.Context) bool {
	return bool(f)
}

// failingFlags fails the test on any flag read.
type failingFlags struct{ t *testing.T }

func (f failingFlags) BoolFlag(_ context.Context, flag featureflags.BoolFlag, _ ...ldcontext.Context) bool {
	f.t.Fatalf("the flag %s was read", flag.Key())

	return false
}

func TestServeNeverReadsTheFlagForTheDefaultProfile(t *testing.T) {
	t.Parallel()

	p, err := Serve(t.Context(), failingFlags{t}, Default())
	require.NoError(t, err)
	assert.Equal(t, DefaultName, p.Name())

	p, err = Serve(t.Context(), nil, Default())
	require.NoError(t, err)
	assert.Equal(t, DefaultName, p.Name())
}

func TestServeGatesANonDefaultProfileOnTheFlag(t *testing.T) {
	t.Parallel()

	other := namedProfile{name: "other", label: "Other"}

	p, err := Serve(t.Context(), flagsOn(true), other)
	require.NoError(t, err)
	assert.Equal(t, "other", p.Name())

	for name, flags := range map[string]Flags{
		"off":       flagsOn(false),
		"nil":       nil,
		"typed nil": (*featureflags.Client)(nil),
	} {
		_, err = Serve(t.Context(), flags, other)
		var notEnabled *NotEnabledError
		require.ErrorAs(t, err, &notEnabled, name)
		assert.Equal(t, "Other", notEnabled.Label, name)
		assert.Equal(t, "the Other guest profile is not enabled on this node: the guest-profiles-enabled flag is off", err.Error(), name)
	}
}

// The flag is read on every call, so turning it off refuses the next build or sandbox.
func TestServeReadsTheFlagOnEveryCall(t *testing.T) {
	t.Parallel()

	reads := 0
	counting := countingFlags{reads: &reads}
	other := namedProfile{name: "other"}
	for range 3 {
		_, err := Serve(t.Context(), counting, other)
		require.NoError(t, err)
	}
	assert.Equal(t, 3, reads)
}

type countingFlags struct{ reads *int }

func (c countingFlags) BoolFlag(context.Context, featureflags.BoolFlag, ...ldcontext.Context) bool {
	*c.reads++

	return true
}

func TestGuestProfilesFlagDefaultsOff(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "guest-profiles-enabled", featureflags.GuestProfilesFlag.Key())
	// The fallback reads GUEST_PROFILES_ENABLED at init; with it set, the
	// shared package's own test covers the fallback.
	if _, set := os.LookupEnv("GUEST_PROFILES_ENABLED"); !set {
		assert.False(t, featureflags.GuestProfilesFlag.Fallback())
	}
}

func TestFirecrackerSelectionAccept(t *testing.T) {
	t.Parallel()

	require.NoError(t, FirecrackerSelection{}.Accept("anything"))
	require.NoError(t, Default().Firecracker().Accept("anything"))
	refusing := FirecrackerSelection{AcceptVersion: func(version string) error {
		if version != "ok" {
			return assert.AnError
		}

		return nil
	}}
	require.NoError(t, refusing.Accept("ok"))
	require.ErrorIs(t, refusing.Accept("other"), assert.AnError)
}
