package metadata

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
)

type claimingProfile struct {
	guest.Profile

	name string
}

func (p claimingProfile) Name() string { return p.name }

func (p claimingProfile) Label() string { return p.name }

// Every copy-constructor must carry the guest OS, or a child build would be
// served under the default profile.
func TestCopyConstructorsCarryGuestOS(t *testing.T) {
	t.Parallel()

	base := Template{Version: CurrentVersion, Template: TemplateMetadata{BuildID: "build-1"}, GuestOS: "other"}

	tests := map[string]Template{
		"SameVersionTemplate": base.SameVersionTemplate(TemplateMetadata{BuildID: "build-2"}),
		"NewVersionTemplate":  base.NewVersionTemplate(TemplateMetadata{BuildID: "build-2"}),
		"WithPrefetch":        base.WithPrefetch(&Prefetch{Memory: &MemoryPrefetchMapping{}}),
		"BasedOn":             base.BasedOn(FromTemplate{Alias: "a", BuildID: "build-0"}),
		"WithVcpuCount":       base.WithVcpuCount(4),
		"WithBalloon":         base.WithBalloon(true, false),
	}
	for name, got := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, "other", got.GuestOS)
		})
	}
}

func TestDeserializeKeepsTheDefaultGuestOS(t *testing.T) {
	t.Parallel()

	reader, err := serialize(Template{Version: CurrentVersion, Template: TemplateMetadata{BuildID: "build-1"}})
	require.NoError(t, err)
	got, err := deserialize(reader)
	require.NoError(t, err)
	assert.Empty(t, got.GuestOS)
}

func TestDeserializeRefusesAnUnclaimedGuestOS(t *testing.T) {
	t.Parallel()

	reader, err := serialize(Template{Version: CurrentVersion, GuestOS: "unclaimed"})
	require.NoError(t, err)
	_, err = deserialize(reader)
	require.ErrorIs(t, err, guest.ErrUnclaimed)
}

func TestDeserializeRefusesAGuestOSOnADeprecatedVersion(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		doc     string
		refused bool
	}{
		{`{"version":1}`, false},
		{`{"version":1,"guest_os":""}`, false},
		{`{"version":1,"guest_os":null}`, false},
		{`{"version":1,"GUEST_OS":null}`, false},
		{`{"version":1,"guestOS":null}`, false},
		{`{"version":1,"guest_os":"other"}`, true},
		{`{"version":1,"guest_os":1}`, true},
		{`{"version":1,"guest_os":{}}`, true},
		{`{"version":1,"GUEST_OS":"other"}`, true},
		{`{"version":1,"guestOS":"other"}`, true},
		{`{"version":1,"GuestOS":"other"}`, true},
		{`{"version":1,"guest_os":"other","guestos":""}`, true},
	} {
		got, err := deserialize(bytes.NewBufferString(tc.doc))
		if tc.refused {
			require.ErrorContains(t, err, "cannot carry", tc.doc)

			continue
		}
		require.NoError(t, err, tc.doc)
		assert.Equal(t, uint64(DeprecatedVersion), got.Version, tc.doc)
	}
}

// The key is snake_case like every other key of the file.
func TestGuestOSSerializesAsSnakeCase(t *testing.T) {
	t.Parallel()

	reader, err := serialize(Template{Version: CurrentVersion, GuestOS: "other"})
	require.NoError(t, err)
	doc, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Contains(t, string(doc), `"guest_os":"other"`)
	assert.NotContains(t, string(doc), "guestOS")
}

// The decoder would drop the retired camelCase spelling and serve the build
// under the default profile, so a document that carries it is refused whatever
// its version and whichever reader opens it.
func TestDeserializeRefusesTheRetiredGuestOSKey(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		doc     string
		refused bool
		guestOS string
	}{
		{`{"version":2,"guest_os":"other"}`, false, "other"},
		{`{"version":2,"GUEST_OS":"other"}`, false, "other"},
		{`{"version":2,"guestOS":""}`, false, ""},
		{`{"version":2,"guestOS":null}`, false, ""},
		{`{"version":2,"guestOS":"other"}`, true, ""},
		{`{"version":2,"GuestOS":"other"}`, true, ""},
		{`{"version":2,"guestOS":1}`, true, ""},
		{`{"version":2,"guest_os":"other","guestOS":"other"}`, true, ""},
	} {
		got, err := deserializeWith(bytes.NewBufferString(tc.doc), anyGuest)
		if tc.refused {
			require.ErrorContains(t, err, "retired key", tc.doc)
			_, err = deserialize(bytes.NewBufferString(tc.doc))
			require.ErrorContains(t, err, "retired key", tc.doc)

			continue
		}
		require.NoError(t, err, tc.doc)
		assert.Equal(t, tc.guestOS, got.GuestOS, tc.doc)
	}
}

// A tool that copies a build reads it whatever its guest OS.
func TestAnyGuestReaderSkipsTheClaimCheck(t *testing.T) {
	t.Parallel()

	reader, err := serialize(Template{Version: CurrentVersion, GuestOS: "unclaimed"})
	require.NoError(t, err)
	got, err := deserializeWith(reader, anyGuest)
	require.NoError(t, err)
	assert.Equal(t, "unclaimed", got.GuestOS)

	_, err = got.GuestProfile()
	require.ErrorIs(t, err, guest.ErrUnclaimed, "the resolver still refuses it")
}

func TestGuestProfileResolvesTheDefault(t *testing.T) {
	t.Parallel()

	p, err := Template{}.GuestProfile()
	require.NoError(t, err)
	assert.Equal(t, guest.DefaultLabel, p.Label())
}

//nolint:paralleltest // installs the process registry
func TestDeserializeAcceptsAClaimedGuestOS(t *testing.T) {
	previous := guest.Installed()
	t.Cleanup(func() { require.NoError(t, guest.Install(previous)) })

	registry, err := guest.NewRegistry(claimingProfile{Profile: guest.Default(), name: "other"})
	require.NoError(t, err)
	require.NoError(t, guest.Install(registry))

	reader, err := serialize(Template{Version: CurrentVersion, GuestOS: "other"})
	require.NoError(t, err)
	got, err := deserialize(reader)
	require.NoError(t, err)
	assert.Equal(t, "other", got.GuestOS)
}
