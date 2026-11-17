package distro

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Golden lines lifted VERBATIM from the pre-change provision.sh so the debian
// profile reproduces them and Debian/Ubuntu behaviour is preserved.
const (
	goldenDebianPackages = "systemd systemd-sysv openssh-server sudo chrony socat curl ca-certificates fuse3 iptables git nfs-common less nftables iputils-ping jq iproute2"
	goldenDebianQuery    = `dpkg-query -W -f='${Status}' "$1" 2>/dev/null | grep -q "install ok installed"`
	goldenDebianInit     = "/lib/systemd/systemd"
)

func profileByKey(t *testing.T, key string) Profile {
	t.Helper()
	for _, p := range Profiles {
		if p.Key == key {
			return p
		}
	}
	require.Failf(t, "profile not found", "no profile with key %q", key)

	return Profile{}
}

// The debian profile preserves today's Debian package set / query / init path.
func TestDebianPreserved(t *testing.T) {
	t.Parallel()
	p := profileByKey(t, "debian")
	assert.Equal(t, goldenDebianPackages, strings.Join(p.Packages, " "), "debian packages drifted")
	assert.Equal(t, goldenDebianQuery, p.PkgQueryBody, "debian query drifted")
	assert.Equal(t, goldenDebianInit, p.InitBinary, "debian init drifted")
	assert.Equal(t, "chrony", p.TimeSyncUnit, "debian unit drifted")
	assert.Equal(t, "sudo", p.AdminGroup, "debian group drifted")
}

// The families genuinely diverge on the axes that matter.
func TestFamiliesDiffer(t *testing.T) {
	t.Parallel()
	rhel := profileByKey(t, "rhel")
	assert.Equal(t, "chronyd", rhel.TimeSyncUnit, "rhel unit wrong")
	assert.Equal(t, "wheel", rhel.AdminGroup, "rhel group wrong")
	// Must both regenerate the trust store AND materialize the bundle at the
	// Debian-named path envd expects — update-ca-trust alone never creates
	// ca-certificates.crt, which left envd.service unable to start on Fedora.
	assert.Contains(t, rhel.CARefresh, "update-ca-trust extract", "rhel CA refresh wrong")
	assert.Contains(t, rhel.CARefresh, `ln -sf /etc/pki/tls/certs/ca-bundle.crt "$E2B_CA_BUNDLE"`, "rhel CA refresh wrong")
	assert.Equal(t, "/usr/lib/systemd/systemd", rhel.InitBinary, "rhel init path wrong")
	arch := profileByKey(t, "arch")
	assert.Contains(t, arch.PkgInstall, "pacman", "arch install should use pacman")
}

// dnf5 (Fedora 41+) only accepts --allowerasing after the subcommand and exits 2
// with "Unknown argument" when it comes first, which silently demoted every
// modern Fedora build to the flagless microdnf/yum fallback.
func TestRhelAllowErasingFollowsSubcommand(t *testing.T) {
	t.Parallel()
	rhel := profileByKey(t, "rhel")
	assert.Contains(t, rhel.PkgInstall, "install --allowerasing", "rhel install must place --allowerasing after the subcommand")
	assert.NotContains(t, rhel.PkgInstall, "--allowerasing install", "rhel install has --allowerasing before the subcommand, which dnf5 rejects")
}

// Minimal RPM images ship only one of dnf, microdnf or yum (OpenCloudOS and
// TencentOS publish -microdnf variants with no dnf), so every RPM profile has to
// fall through all three or provisioning dies with "dnf: command not found".
func TestRPMProfilesFallBackFromDnf(t *testing.T) {
	t.Parallel()
	for _, p := range Profiles {
		if !strings.HasPrefix(p.PkgQueryBody, "rpm ") {
			continue
		}
		for _, want := range []string{"dnf -y install --allowerasing", "|| microdnf -y install", "|| yum -y install"} {
			assert.Contains(t, p.PkgInstall, want, "%s install is missing %q", p.Key, want)
		}
	}
}

// The generated selector keys on the DECLARED distro id, never on which
// package-manager binary happens to exist.
func TestSelectorNoPackageManagerProbing(t *testing.T) {
	t.Parallel()
	sel := ShellSelector()
	for _, bad := range []string{
		"command -v apt-get", "command -v dnf", "command -v yum",
		"command -v microdnf", "command -v pacman", "PKG_FAMILY",
	} {
		assert.NotContains(t, sel, bad, "selector leaked package-manager probing")
	}
	assert.Contains(t, sel, `case "$E2B_DISTRO_ID" in`, "selector must switch on $E2B_DISTRO_ID (declared distro identity)")
}

// Every supported id gets a case arm; an unknown id hits the failing default.
func TestSelectorCoversIDsAndRejects(t *testing.T) {
	t.Parallel()
	sel := ShellSelector()
	for _, id := range SupportedIDs() {
		assert.Contains(t, sel, id, "selector missing arm for supported id")
	}
	for _, want := range []string{"*)", "unsupported base image", "exit 1"} {
		assert.Contains(t, sel, want, "selector missing fast-reject piece")
	}
	// Alpine is supported via the OpenRC track — and it must be the OpenRC
	// profile, never folded into a systemd family.
	alpine := profileByKey(t, "alpine")
	assert.Equal(t, InitOpenRC, alpine.Init, "alpine must be the OpenRC profile")
}

// Every profile declares a known init system with a rendered setup body, and
// no body leaks another init system's tooling (systemctl in OpenRC or
// rc-update in systemd would fail at provisioning time).
func TestInitSystemsDeclaredAndCoherent(t *testing.T) {
	t.Parallel()
	for _, p := range Profiles {
		setup, ok := initSetup[p.Init]
		if !assert.True(t, ok, "profile %q declares init %q with no setup body", p.Key, p.Init) {
			continue
		}
		switch p.Init {
		case InitSystemd:
			assert.NotContains(t, setup, "rc-update", "systemd init setup leaks rc-update (profile %q)", p.Key)
		case InitOpenRC:
			assert.NotContains(t, setup, "systemctl", "openrc init setup leaks systemctl (profile %q)", p.Key)
		}
	}
	sel := ShellSelector()
	assert.Contains(t, sel, "e2b_init_setup() {", "selector must define e2b_init_setup()")
	// The OpenRC boot chain pieces the alpine arm must carry.
	for _, want := range []string{"/etc/inittab", "rc-update add envd default", "openrc sysinit"} {
		assert.Contains(t, sel, want, "selector missing OpenRC boot piece")
	}
}

// The per-profile fragments are spliced into single-line generated shell
// functions (`e2b_pkg_install() { … ; }`), so they stay one-liners — chain with
// `;` or `&&` instead of embedding newlines.
func TestProfileFragmentsAreSingleLine(t *testing.T) {
	t.Parallel()
	for _, p := range Profiles {
		for name, frag := range map[string]string{
			"PkgQueryBody": p.PkgQueryBody,
			"PkgInstall":   p.PkgInstall,
			"CARefresh":    p.CARefresh,
		} {
			assert.NotContains(t, frag, "\n", "profile %q %s spans lines; chain with ; or && instead", p.Key, name)
		}
	}
}

// Both init families must wire the boot-time chrony source selector in, each
// with its own mechanism: chrony.conf includes a file only that selector writes,
// so a family that skips the wiring boots with no time source at all.
func TestInitSetupWiresChronySourceSelector(t *testing.T) {
	t.Parallel()
	systemd := initSetup[InitSystemd]
	// A drop-in on the family's own unit name, not a static symlink or a unit
	// [Install] section: the name differs per family and the RHEL preset policy
	// deletes enablement symlinks on first boot.
	for _, want := range []string{
		`/etc/systemd/system/$E2B_TIMESYNC_UNIT.service.d`,
		"Requires=e2b-chrony-source.service",
		"After=e2b-chrony-source.service",
	} {
		assert.Contains(t, systemd, want, "systemd init setup missing chrony source wiring")
	}
	openrc := initSetup[InitOpenRC]
	for _, want := range []string{
		"cp /usr/local/share/e2b/chrony-source.openrc /etc/init.d/e2b-chrony-source",
		"rc-update add e2b-chrony-source boot",
	} {
		assert.Contains(t, openrc, want, "openrc init setup missing chrony source wiring")
	}
}

// Alpine's chrony build takes a SIGSYS under the seccomp filter its OpenRC init
// script hardcodes (-F 1) the moment the PHC refclock is driven. Since
// e2b-chrony-source picks the source at BOOT, provisioning cannot know whether
// the PHC branch will be taken, so the filter must come off unconditionally —
// gating it on a build-time /dev/ptp0 probe (as the original fix did) puts the
// crash back on exactly the nodes that have a PHC.
func TestOpenRCDisablesChronySeccompRegardlessOfSource(t *testing.T) {
	t.Parallel()
	openrc := initSetup[InitOpenRC]
	assert.Contains(t, openrc, `echo 'command_args="-F 0"' >>"/etc/conf.d/$E2B_TIMESYNC_UNIT"`, "openrc init setup must disable the chronyd seccomp filter via conf.d command_args")
	// The whole block runs on every Alpine build, so no COMMAND in it may branch
	// on the device or on a provisioning-time PHC verdict. Comments are stripped
	// first — they are where the reasoning lives and legitimately say "refclock".
	var code []string
	for l := range strings.SplitSeq(openrc, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "#") {
			code = append(code, l)
		}
	}
	openrcCode := strings.Join(code, "\n")
	for _, bad := range []string{"/dev/ptp0", "E2B_CHRONY_PHC", "refclock"} {
		assert.NotContains(t, openrcCode, bad, "openrc init setup branches on %q; the time source is chosen at boot, not while provisioning", bad)
	}
}

// The cache fingerprint must cover the whole generated provisioning contract:
// stable across calls, and carrying both the selector text and the explicit
// Version (a profile change must rotate the base-layer cache key).
func TestFingerprintStableAndVersioned(t *testing.T) {
	t.Parallel()
	a, b := Fingerprint(), Fingerprint()
	assert.Equal(t, a, b, "fingerprint must be stable")
	assert.Len(t, a, 64, "fingerprint must be a sha256 hex")
	want := sha256.Sum256([]byte(Version + "\x00" + ShellSelector()))
	assert.Equal(t, hex.EncodeToString(want[:]), a, "fingerprint must hash Version + selector text")
}

// Sanity: the RHEL-family rebuilds (centos/rocky/alma) all resolve to one arm.
func TestRHELFamilyAliases(t *testing.T) {
	t.Parallel()
	rhel := profileByKey(t, "rhel")
	for _, want := range []string{"fedora", "centos", "rocky", "almalinux"} {
		assert.Contains(t, rhel.IDs, want, "rhel family missing alias")
	}
}

// RHEL, Oracle Linux and Amazon Linux are deliberately NOT accepted: sandboxes
// boot E2B's kernel, so the kernel-level fidelity those images are chosen for
// isn't there. Rejecting them up front beats building something subtly wrong.
func TestKernelDependentIDsAreRejected(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"rhel", "ol", "amzn"} {
		assert.NotContains(t, SupportedIDs(), id, "id must not be accepted: E2B supplies the guest kernel")
	}
}

// An id we don't know falls back to ID_LIKE with a warning instead of failing:
// switching provisioning to the declared id silently dropped every Debian
// derivative (Kali declares ID=kali ID_LIKE=debian) that used to work back when
// we probed for a package manager.
func TestUnknownIDFallsBackToIDLike(t *testing.T) {
	t.Parallel()
	sel := ShellSelector()
	assert.Contains(t, sel, "e2b_select_profile", "selection must be a function so it can be retried per ID_LIKE token")
	assert.Contains(t, sel, "for e2b_like in $E2B_ID_LIKE; do", "selector must retry each ID_LIKE token")
	assert.Contains(t, sel, "WARNING", "an ID_LIKE match must warn, not pass silently")
	// Nothing matched is still fatal — better than provisioning a guessed family.
	assert.Contains(t, sel, "unsupported base image distribution", "an id matching neither ID nor ID_LIKE must still fail")
	// An if-condition call runs the function body with errexit suppressed.
	assert.NotContains(t, sel, "if e2b_select_profile", "e2b_select_profile must not be invoked as an if-condition (errexit suppression)")
	assert.NotContains(t, sel, "if ! e2b_select_profile", "e2b_select_profile must not be invoked as an if-condition (errexit suppression)")
	assert.Contains(t, sel, "e2b_profile_matched=1", "a matched profile arm must set e2b_profile_matched")
}

// ID_LIKE must not re-admit the ids the rhel profile documents as out of scope:
// Oracle and Amazon Linux both declare ID_LIKE=fedora.
func TestRejectedIDsAreNotReachableViaIDLike(t *testing.T) {
	t.Parallel()
	sel := ShellSelector()
	guard := strings.Join(RejectedIDs, "|")
	assert.Contains(t, sel, guard, "selector must guard rejected ids before the ID_LIKE fallback")
	// The guard has to come first, or ID_LIKE=fedora would match them.
	assert.LessOrEqual(t, strings.Index(sel, guard), strings.Index(sel, "E2B_ID_LIKE"), "the rejected-id guard must precede the ID_LIKE fallback")
}
