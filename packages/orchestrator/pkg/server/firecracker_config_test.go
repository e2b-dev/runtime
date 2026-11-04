//go:build linux

package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// pinningProfile is a guest profile that pins the Firecracker binary of its builds.
type pinningProfile struct {
	guest.Profile
}

func (pinningProfile) Name() string { return "pinned" }

func (pinningProfile) Firecracker() guest.FirecrackerSelection {
	return guest.FirecrackerSelection{PinToBuild: true}
}

// A served sandbox is loaded with the device names its build's memory image
// bakes; a snapshot this runtime made carries none, so the configuration keeps
// its defaults and nothing changes for an existing template.
func TestFirecrackerConfigForCarriesTheBuildsDeviceNames(t *testing.T) {
	t.Parallel()

	profile := pinningProfile{Profile: guest.Default()}
	foreign := metadata.Template{GuestOS: "pinned"}.WithDeviceNames("root", "net0", "guest.disk")
	cfg := firecrackerConfigFor(foreign, profile, "firmware-fd", "v1.18.0-dev_4bb5b4de0")
	assert.Equal(t, "firmware-fd", cfg.KernelVersion)
	assert.Equal(t, "v1.18.0-dev_4bb5b4de0", cfg.FirecrackerVersion)
	assert.Equal(t, "pinned", cfg.GuestProfile().Name())
	assert.Equal(t, "root", cfg.RootfsDriveIDOrDefault())
	assert.Equal(t, "net0", cfg.NetIfaceIDOrDefault("eth0"))
	assert.Equal(t, "guest.disk", cfg.RootfsFileName())

	pinnedKernel := firecrackerConfigFor(metadata.Template{Template: metadata.TemplateMetadata{KernelVersion: "firmware-202505"}}, profile, "vmlinux-6.1.158", "v1.18.0-dev_4bb5b4de0")
	assert.Equal(t, "firmware-202505", pinnedKernel.KernelVersion, "a pinning profile's build keeps its kernel slot payload")

	linux := firecrackerConfigFor(metadata.Template{Template: metadata.TemplateMetadata{KernelVersion: "vmlinux-6.1.102"}}, guest.Default(), "vmlinux-6.1.158", "v1.14-0.3.1")
	assert.Equal(t, "vmlinux-6.1.158", linux.KernelVersion, "the default profile takes the request's kernel")
	assert.Equal(t, guest.DefaultName, linux.GuestProfile().Name())
	assert.Equal(t, "rootfs", linux.RootfsDriveIDOrDefault())
	assert.Equal(t, "eth0", linux.NetIfaceIDOrDefault("eth0"))
	assert.Equal(t, "rootfs.ext4", linux.RootfsFileName())
}

// The fleet's version map remaps the default profile's Firecracker version and
// leaves a pinning profile's alone: the same map, the same requested version,
// two answers; a pinned build's metadata wins over its build row.
func TestResolveSandboxFirecrackerVersion_PinningProfileKeepsTheBuildsBinary(t *testing.T) {
	t.Parallel()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.FirecrackerVersions.Key()).ValueForAll(
		ldvalue.ObjectBuild().Set("v1.14", ldvalue.String("v1.14.9")).Set("v1.18", ldvalue.String("v1.18.9")).Build()))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(t.Context()) })

	pinned := pinningProfile{Profile: guest.Default()}
	assert.Equal(t, "v1.14.9", resolveSandboxFirecrackerVersion(t.Context(), ff, metadata.Template{}, guest.Default(), "v1.14.1_431f1fc"),
		"the default profile follows the fleet map")
	assert.Equal(t, "v1.18.9", resolveSandboxFirecrackerVersion(t.Context(), ff, metadata.Template{}, guest.Default(), "v1.18.0-dev_4bb5b4de0"),
		"the map would remap the pinned binary's key too")
	assert.Equal(t, "v1.18.0-dev_4bb5b4de0", resolveSandboxFirecrackerVersion(t.Context(), ff, metadata.Template{}, pinned, "v1.18.0-dev_4bb5b4de0"),
		"a pinning profile keeps the binary its build row names")
	meta := metadata.Template{Template: metadata.TemplateMetadata{FirecrackerVersion: "v1.17-100.0.0"}}
	assert.Equal(t, "v1.17-100.0.0", resolveSandboxFirecrackerVersion(t.Context(), ff, meta, pinned, "v1.14.1_431f1fc"),
		"a pinned build's metadata wins over its build row")
	assert.Equal(t, "v1.14.9", resolveSandboxFirecrackerVersion(t.Context(), ff, meta, guest.Default(), "v1.14.1_431f1fc"),
		"the default profile ignores the metadata's binary")
}

// noMemfdProfile is a guest profile whose restore keeps the memory off memfd.
type noMemfdProfile struct {
	guest.Profile
}

func (noMemfdProfile) Restore() guest.RestoreConfig { return guest.RestoreConfig{NoMemfd: true} }

// The pause disk estimate follows the restore's memfd choice: a profile that
// restores without a memfd exports through the raw diff and its dedup copy,
// so it is not counted as a memfd export even where the version and flag allow one.
func TestMemfdExportFollowsTheProfile(t *testing.T) {
	t.Parallel()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.UseMemFdFlag.Key()).VariationForAll(true))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(t.Context()) })
	s := &Server{featureFlags: ff}

	sandboxWith := func(profile guest.Profile) *sandbox.Sandbox {
		return &sandbox.Sandbox{Metadata: &sandbox.Metadata{Config: sandbox.NewConfig(sandbox.Config{
			FirecrackerConfig: fc.Config{FirecrackerVersion: "v1.14.1_431f1fc", Guest: profile},
		})}}
	}

	assert.True(t, s.memfdExport(t.Context(), sandboxWith(nil)), "the default profile exports from a memfd")
	assert.False(t, s.memfdExport(t.Context(), sandboxWith(noMemfdProfile{Profile: guest.Default()})), "a profile without memfd does not")
}

// A template whose guest OS no registered profile claims is refused with
// FailedPrecondition, not reported as an unknown failure, and without the
// create-refused detail, so placement still tries a node that registers it.
func TestUnclaimedGuestIsARefusal(t *testing.T) {
	t.Parallel()

	_, err := metadata.Template{GuestOS: "unclaimed"}.GuestProfile()
	require.ErrorIs(t, err, guest.ErrUnclaimed)

	refusal := guestProfileRefusal(fmt.Errorf("failed to read template metadata: %w", err))
	require.Error(t, refusal)
	st := status.Convert(refusal)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	assert.Contains(t, st.Message(), "unclaimed")
	assert.Empty(t, st.Details(), "no create-refused detail, so placement retries elsewhere")

	assert.NoError(t, guestProfileRefusal(errors.New("disk read failed")), "any other error is not a refusal")
}

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

// A template of a registered non-default guest is refused while the flag is
// off, with the same FailedPrecondition and no create-refused detail as an
// unclaimed guest, so placement still tries a node with the flag on. The
// default guest never reads the flag.
func TestGuestProfileNotEnabledIsARefusal(t *testing.T) {
	t.Parallel()

	line := lineProfile{Profile: guest.Default()}
	profile, err := guest.Serve(t.Context(), flagsOn(false), line)
	require.Nil(t, profile)
	var notEnabled *guest.NotEnabledError
	require.ErrorAs(t, err, &notEnabled)

	refusal := guestProfileRefusal(fmt.Errorf("failed to resolve the guest profile: %w", err))
	require.Error(t, refusal)
	st := status.Convert(refusal)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	assert.Equal(t, "the line guest profile is not enabled on this node: the guest-profiles-enabled flag is off", st.Message(), "the typed error's own text, without the resolve wrapper")
	assert.Empty(t, st.Details(), "no create-refused detail, so placement retries elsewhere")

	profile, err = guest.Serve(t.Context(), flagsOn(true), line)
	require.NoError(t, err)
	assert.Equal(t, "line", profile.Name())

	profile, err = metadata.Template{}.ServingGuestProfile(t.Context(), failingFlags{t})
	require.NoError(t, err)
	assert.Equal(t, guest.DefaultName, profile.Name())
}

// lineProfile is a non-default guest profile.
type lineProfile struct{ guest.Profile }

func (lineProfile) Name() string  { return "line" }
func (lineProfile) Label() string { return "line" }

// caBundlelessProfile is a guest profile that sends envd no CA bundle.
type caBundlelessProfile struct {
	guest.Profile
}

func (caBundlelessProfile) Label() string { return "bundleless" }

func (caBundlelessProfile) EnvdInit() guest.EnvdInit { return guest.EnvdInit{NoCABundle: true} }

// A new sandbox whose rules the proxy intercepts is refused with
// InvalidArgument for a guest without the CA bundle; a resume carrying the
// same rules from its snapshot is let through, so it is never stranded.
func TestTLSInterceptionRefusedAtCreateOnly(t *testing.T) {
	t.Parallel()

	rules := &orchestrator.SandboxNetworkEgressConfig{Rules: map[string]*orchestrator.SandboxNetworkDomainRules{"api.example.com": {}}}
	profile := caBundlelessProfile{Profile: guest.Default()}

	err := tlsInterceptionRefusal(t.Context(), profile, rules, false)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
	assert.Contains(t, status.Convert(err).Message(), "the bundleless guest has no CA bundle capability")

	require.NoError(t, tlsInterceptionRefusal(t.Context(), profile, rules, true), "a resume is not refused")
	require.NoError(t, tlsInterceptionRefusal(t.Context(), profile, nil, false), "no rules, no refusal")
	require.NoError(t, tlsInterceptionRefusal(t.Context(), guest.Default(), rules, false), "the default profile takes the bundle")
}
