package sandbox

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// firmwareProfile is a guest profile that asks for every boot and envd init
// difference the profile can declare.
type firmwareProfile struct {
	guest.Profile
}

func (firmwareProfile) Name() string  { return "firmware" }
func (firmwareProfile) Label() string { return "firmware" }

func (firmwareProfile) Boot() guest.BootConfig {
	return guest.BootConfig{
		NoKernelArgs:           true,
		PCI:                    true,
		MachineConfigExtra:     map[string]json.RawMessage{"x_extension": json.RawMessage(`true`)},
		NoRootDevice:           true,
		BlockSectorSize:        512,
		NoEntropy:              true,
		NoBalloon:              true,
		NoCPUTemplate:          true,
		RefuseVcpusAboveTarget: true,
		NoFilesystemRecovery:   true,
		NoEnvdOfflineUpgrade:   true,
	}
}

func (firmwareProfile) Restore() guest.RestoreConfig {
	return guest.RestoreConfig{ClockRealtime: true, MmdsStoreOptional: true, NoMemfd: true, NoSyncWriteProtect: true}
}

func (firmwareProfile) EnvdInit() guest.EnvdInit {
	return guest.EnvdInit{RequestTimeoutFloor: 2 * time.Second, ColdBootEnvdTimeout: 3 * time.Minute, RestoreEnvdTimeoutFloor: 30 * time.Second, NoCABundle: true}
}

// newTestGuestSandbox builds a minimal Sandbox for a guest profile, with an
// envd version above every gate and no reachable envd.
func newTestGuestSandbox(profile guest.Profile) *Sandbox {
	return &Sandbox{
		Metadata: &Metadata{
			internalConfig: internalConfig{EnvdInitRequestTimeout: 5 * time.Second},
			Config: NewConfig(Config{
				Envd:              EnvdMetadata{Version: "9.9.9"},
				FirecrackerConfig: fc.Config{Guest: profile},
			}),
			Runtime: sandboxtypes.RuntimeMetadata{SandboxID: "test-sandbox"},
		},
	}
}

func TestRebootEnvdTimeoutForGuest(t *testing.T) {
	t.Parallel()

	assert.Equal(t, rebootEnvdTimeout, rebootEnvdTimeoutFor(newTestGuestSandbox(nil).Config))
	assert.Equal(t, 3*time.Minute, rebootEnvdTimeoutFor(newTestGuestSandbox(firmwareProfile{}).Config))
}

// A restore waits for envd as long as the flag says, raised to the profile's
// floor; the default profile and a flag above the floor keep the flag.
func TestRestoreEnvdTimeoutForGuest(t *testing.T) {
	t.Parallel()

	linux := newTestGuestSandbox(nil).Config
	floored := newTestGuestSandbox(firmwareProfile{}).Config

	assert.Equal(t, 10*time.Second, restoreEnvdTimeoutFor(linux, 10*time.Second))
	assert.Equal(t, 30*time.Second, restoreEnvdTimeoutFor(floored, 10*time.Second))
	assert.Equal(t, 45*time.Second, restoreEnvdTimeoutFor(floored, 45*time.Second))
}

// A profile that skips both pre-boot steps gets no pre-boot step. The factory
// has no flag client, so a step that was still resolved would fail on it.
func TestProfileSkipsTheRebootPreBoot(t *testing.T) {
	t.Parallel()

	config := NewConfig(Config{Envd: EnvdMetadata{Version: "9.9.9"}, FirecrackerConfig: fc.Config{Guest: firmwareProfile{}}})
	recovery, swap := (&Factory{}).rebootPreBoot(t.Context(), config, sandboxtypes.RuntimeMetadata{SandboxID: "test-sandbox"}, false, true, func(rootfs.RecoverOutcome) {})
	assert.Nil(t, recovery)
	assert.Nil(t, swap)
}

// The balloon stamp follows the device: a boot that installs none records
// none, whatever the config asked for.
func TestProfileWithoutBalloonRecordsNoBalloon(t *testing.T) {
	t.Parallel()

	linux := NewConfig(Config{FreePageReporting: true})
	assert.Equal(t, fc.BalloonCaps{Reporting: true, Hinting: true}, bootBalloonCaps(linux, true))

	firmware := NewConfig(Config{FreePageReporting: true, FirecrackerConfig: fc.Config{Guest: firmwareProfile{}}})
	assert.Equal(t, fc.BalloonCaps{}, bootBalloonCaps(firmware, true))
	assert.Equal(t, balloonModeOf(fc.BalloonCaps{}), balloonModeOf(bootBalloonCaps(firmware, true)))
}

func TestEnvdInitCABundleForGuest(t *testing.T) {
	t.Parallel()

	const bundle = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"

	linux := newTestGuestSandbox(nil)
	linux.egressProxy = &mockEgressProxy{bundle: bundle}
	assert.Equal(t, bundle, linux.newEnvdInitRequest("http://envd/init", "").body.CaBundle)

	firmware := newTestGuestSandbox(firmwareProfile{})
	firmware.egressProxy = &mockEgressProxy{bundle: bundle}
	assert.Empty(t, firmware.newEnvdInitRequest("http://envd/init", "").body.CaBundle)
}

func TestNilConfigHasTheDefaultProfile(t *testing.T) {
	t.Parallel()

	assert.Equal(t, guest.DefaultLabel, (*Config)(nil).GuestProfile().Label())
}

// A rule set makes the egress proxy intercept TLS for its domain, which a
// guest without the CA bundle cannot trust: it is refused for such a profile,
// with or without a transform. The default request and a domain allowlist are
// not intercepted and still skip the bundle silently, and the default profile
// takes any rule.
func TestRefuseTLSInterception(t *testing.T) {
	t.Parallel()

	transform := &orchestrator.SandboxNetworkEgressConfig{Rules: map[string]*orchestrator.SandboxNetworkDomainRules{
		"api.example.com": {Rules: []*orchestrator.SandboxNetworkRule{{Transform: &orchestrator.SandboxNetworkTransform{Headers: map[string]string{"Authorization": "Bearer x"}}}}},
	}}
	emptyRule := &orchestrator.SandboxNetworkEgressConfig{Rules: map[string]*orchestrator.SandboxNetworkDomainRules{"api.example.com": {Rules: []*orchestrator.SandboxNetworkRule{{}}}}}
	emptySet := &orchestrator.SandboxNetworkEgressConfig{Rules: map[string]*orchestrator.SandboxNetworkDomainRules{"api.example.com": {}}}
	allowlist := &orchestrator.SandboxNetworkEgressConfig{AllowedDomains: []string{"api.example.com"}}

	for name, egress := range map[string]*orchestrator.SandboxNetworkEgressConfig{"transform": transform, "empty rule": emptyRule, "empty rule set": emptySet} {
		err := RefuseTLSInterception(firmwareProfile{}, egress)
		require.ErrorIs(t, err, ErrTLSInterceptionUnsupported, name)
		assert.Contains(t, err.Error(), "the firmware guest has no CA bundle capability", name)
	}

	require.NoError(t, RefuseTLSInterception(firmwareProfile{}, nil), "the default request is not intercepted")
	require.NoError(t, RefuseTLSInterception(firmwareProfile{}, allowlist), "a domain allowlist is not intercepted")
	require.NoError(t, RefuseTLSInterception(guest.Default(), transform), "the default profile takes the bundle")

	firmware := newTestGuestSandbox(firmwareProfile{})
	firmware.egressProxy = &mockEgressProxy{bundle: "bundle"}
	assert.Empty(t, firmware.newEnvdInitRequest("http://envd/init", "").body.CaBundle, "a request without rules still skips the bundle silently")
}
