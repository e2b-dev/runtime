package distro

import (
	"slices"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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
	t.Fatalf("no profile with key %q", key)

	return Profile{}
}

// The debian profile preserves today's Debian package set / query / init path.
func TestDebianPreserved(t *testing.T) {
	t.Parallel()
	p := profileByKey(t, "debian")
	if got := strings.Join(p.Packages, " "); got != goldenDebianPackages {
		t.Errorf("debian packages drifted:\n got: %s\nwant: %s", got, goldenDebianPackages)
	}
	if p.PkgQueryBody != goldenDebianQuery {
		t.Errorf("debian query drifted:\n got: %s\nwant: %s", p.PkgQueryBody, goldenDebianQuery)
	}
	if p.InitBinary != goldenDebianInit {
		t.Errorf("debian init drifted: got %s want %s", p.InitBinary, goldenDebianInit)
	}
	if p.TimeSyncUnit != "chrony" || p.AdminGroup != "sudo" {
		t.Errorf("debian unit/group drifted: %s / %s", p.TimeSyncUnit, p.AdminGroup)
	}
}

// The families genuinely diverge on the axes that matter.
func TestFamiliesDiffer(t *testing.T) {
	t.Parallel()
	rhel := profileByKey(t, "rhel")
	if rhel.TimeSyncUnit != "chronyd" || rhel.AdminGroup != "wheel" {
		t.Errorf("rhel unit/group wrong: %s / %s", rhel.TimeSyncUnit, rhel.AdminGroup)
	}
	// Must both regenerate the trust store AND materialize the bundle at the
	// Debian-named path envd expects — update-ca-trust alone never creates
	// ca-certificates.crt, which left envd.service unable to start on Fedora.
	if !strings.Contains(rhel.CARefresh, "update-ca-trust extract") ||
		!strings.Contains(rhel.CARefresh, `ln -sf /etc/pki/tls/certs/ca-bundle.crt "$E2B_CA_BUNDLE"`) {
		t.Errorf("rhel CA refresh wrong: %s", rhel.CARefresh)
	}
	if rhel.InitBinary != "/usr/lib/systemd/systemd" {
		t.Errorf("rhel init path wrong: %s", rhel.InitBinary)
	}
	arch := profileByKey(t, "arch")
	if !strings.Contains(arch.PkgInstall, "pacman") {
		t.Errorf("arch install should use pacman: %s", arch.PkgInstall)
	}
}

// dnf5 (Fedora 41+) only accepts --allowerasing after the subcommand and exits 2
// with "Unknown argument" when it comes first, which silently demoted every
// modern Fedora build to the flagless microdnf/yum fallback.
func TestRhelAllowErasingFollowsSubcommand(t *testing.T) {
	t.Parallel()
	rhel := profileByKey(t, "rhel")
	if !strings.Contains(rhel.PkgInstall, "install --allowerasing") {
		t.Errorf("rhel install must place --allowerasing after the subcommand: %s", rhel.PkgInstall)
	}
	if strings.Contains(rhel.PkgInstall, "--allowerasing install") {
		t.Errorf("rhel install has --allowerasing before the subcommand, which dnf5 rejects: %s", rhel.PkgInstall)
	}
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
		if strings.Contains(sel, bad) {
			t.Errorf("selector leaked package-manager probing: %q", bad)
		}
	}
	if !strings.Contains(sel, `case "$E2B_DISTRO_ID" in`) {
		t.Error("selector must switch on $E2B_DISTRO_ID (declared distro identity)")
	}
}

// Every supported id gets a case arm; an unknown id hits the failing default.
func TestSelectorCoversIDsAndRejects(t *testing.T) {
	t.Parallel()
	sel := ShellSelector()
	for _, id := range SupportedIDs() {
		if !strings.Contains(sel, id) {
			t.Errorf("selector missing arm for supported id %q", id)
		}
	}
	for _, want := range []string{"*)", "unsupported base image", "exit 1"} {
		if !strings.Contains(sel, want) {
			t.Errorf("selector missing fast-reject piece %q", want)
		}
	}
	// Alpine is supported via the OpenRC track — and it must be the OpenRC
	// profile, never folded into a systemd family.
	alpine := profileByKey(t, "alpine")
	if alpine.Init != InitOpenRC {
		t.Errorf("alpine must be the OpenRC profile, got init %q", alpine.Init)
	}
}

// Every profile declares a known init system with a rendered setup body, and
// no body leaks another init system's tooling (systemctl in OpenRC or
// rc-update in systemd would fail at provisioning time).
func TestInitSystemsDeclaredAndCoherent(t *testing.T) {
	t.Parallel()
	for _, p := range Profiles {
		setup, ok := initSetup[p.Init]
		if !ok {
			t.Errorf("profile %q declares init %q with no setup body", p.Key, p.Init)

			continue
		}
		switch p.Init {
		case InitSystemd:
			if strings.Contains(setup, "rc-update") {
				t.Errorf("systemd init setup leaks rc-update (profile %q)", p.Key)
			}
		case InitOpenRC:
			if strings.Contains(setup, "systemctl") {
				t.Errorf("openrc init setup leaks systemctl (profile %q)", p.Key)
			}
		}
	}
	sel := ShellSelector()
	if !strings.Contains(sel, "e2b_init_setup() {") {
		t.Error("selector must define e2b_init_setup()")
	}
	// The OpenRC boot chain pieces the alpine arm must carry.
	for _, want := range []string{"/etc/inittab", "rc-update add envd default", "openrc sysinit"} {
		if !strings.Contains(sel, want) {
			t.Errorf("selector missing OpenRC boot piece %q", want)
		}
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
			if strings.Contains(frag, "\n") {
				t.Errorf("profile %q %s spans lines; chain with ; or && instead: %s", p.Key, name, frag)
			}
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
		if !strings.Contains(systemd, want) {
			t.Errorf("systemd init setup missing chrony source wiring %q", want)
		}
	}
	openrc := initSetup[InitOpenRC]
	for _, want := range []string{
		"cp /usr/local/share/e2b/chrony-source.openrc /etc/init.d/e2b-chrony-source",
		"rc-update add e2b-chrony-source boot",
	} {
		if !strings.Contains(openrc, want) {
			t.Errorf("openrc init setup missing chrony source wiring %q", want)
		}
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
	if !strings.Contains(openrc, `echo 'command_args="-F 0"' >>"/etc/conf.d/$E2B_TIMESYNC_UNIT"`) {
		t.Error("openrc init setup must disable the chronyd seccomp filter via conf.d command_args")
	}
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
		if strings.Contains(openrcCode, bad) {
			t.Errorf("openrc init setup branches on %q; the time source is chosen at boot, not while provisioning", bad)
		}
	}
}

// The cache fingerprint must cover the whole generated provisioning contract:
// stable across calls, and carrying both the selector text and the explicit
// Version (a profile change must rotate the base-layer cache key).
func TestFingerprintStableAndVersioned(t *testing.T) {
	t.Parallel()
	a, b := Fingerprint(), Fingerprint()
	if a != b || len(a) != 64 {
		t.Errorf("fingerprint must be a stable sha256 hex: %q vs %q", a, b)
	}
	want := sha256.Sum256([]byte(Version + "\x00" + ShellSelector()))
	if a != hex.EncodeToString(want[:]) {
		t.Error("fingerprint must hash Version + selector text")
	}
}

// Sanity: the RHEL-family rebuilds (centos/rocky/alma) all resolve to one arm.
func TestRHELFamilyAliases(t *testing.T) {
	t.Parallel()
	rhel := profileByKey(t, "rhel")
	for _, want := range []string{"fedora", "centos", "rocky", "almalinux"} {
		found := false
		for _, id := range rhel.IDs {
			if id == want {
				found = true
			}
		}
		if !found {
			t.Errorf("rhel family missing alias %q", want)
		}
	}
}

// RHEL, Oracle Linux and Amazon Linux are deliberately NOT accepted: sandboxes
// boot E2B's kernel, so the kernel-level fidelity those images are chosen for
// isn't there. Rejecting them up front beats building something subtly wrong.
func TestKernelDependentIDsAreRejected(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"rhel", "ol", "amzn"} {
		for _, got := range SupportedIDs() {
			if got == id {
				t.Errorf("id %q must not be accepted: E2B supplies the guest kernel", id)
			}
		}
	}
}

// An id we don't know falls back to ID_LIKE with a warning instead of failing:
// switching provisioning to the declared id silently dropped every Debian
// derivative (Kali declares ID=kali ID_LIKE=debian) that used to work back when
// we probed for a package manager.
func TestUnknownIDFallsBackToIDLike(t *testing.T) {
	t.Parallel()
	sel := ShellSelector()
	if !strings.Contains(sel, "e2b_select_profile") {
		t.Error("selection must be a function so it can be retried per ID_LIKE token")
	}
	if !strings.Contains(sel, "for e2b_like in $E2B_ID_LIKE; do") {
		t.Error("selector must retry each ID_LIKE token")
	}
	if !strings.Contains(sel, "WARNING") {
		t.Error("an ID_LIKE match must warn, not pass silently")
	}
	// Nothing matched is still fatal — better than provisioning a guessed family.
	if !strings.Contains(sel, "unsupported base image distribution") {
		t.Error("an id matching neither ID nor ID_LIKE must still fail")
	}
	// An if-condition call runs the function body with errexit suppressed.
	if strings.Contains(sel, "if e2b_select_profile") || strings.Contains(sel, "if ! e2b_select_profile") {
		t.Error("e2b_select_profile must not be invoked as an if-condition (errexit suppression)")
	}
	if !strings.Contains(sel, "e2b_profile_matched=1") {
		t.Error("a matched profile arm must set e2b_profile_matched")
	}
}

// ID_LIKE must not re-admit the ids the rhel profile documents as out of scope:
// Oracle and Amazon Linux both declare ID_LIKE=fedora.
func TestRejectedIDsAreNotReachableViaIDLike(t *testing.T) {
	t.Parallel()
	sel := ShellSelector()
	guard := strings.Join(RejectedIDs, "|")
	if !strings.Contains(sel, guard) {
		t.Errorf("selector must guard rejected ids (%s) before the ID_LIKE fallback", guard)
	}
	// The guard has to come first, or ID_LIKE=fedora would match them.
	if strings.Index(sel, guard) > strings.Index(sel, "E2B_ID_LIKE") {
		t.Error("the rejected-id guard must precede the ID_LIKE fallback")
	}
}

// openEuler declares ID=openEuler (mixed case; provision.sh matches it verbatim
// without lowercasing) and no ID_LIKE, so it needs its own profile. Verified
// against openeuler/openeuler:22.03: it is RHEL-shaped except that its repos
// ship shadow, not shadow-utils. These assertions pin both facts so a future
// edit can't silently regress the case-sensitive id or the package name.
func TestOpenEulerProfile(t *testing.T) {
	t.Parallel()
	oe := profileByKey(t, "openeuler")

	if !slices.Contains(oe.IDs, "openEuler") {
		t.Errorf("openEuler id must be matched verbatim (mixed case), got IDs=%v", oe.IDs)
	}
	if slices.Contains(oe.Packages, "shadow-utils") {
		t.Error("openEuler repos have no shadow-utils; it must install shadow")
	}
	if !slices.Contains(oe.Packages, "shadow") {
		t.Error("openEuler must install shadow for the shadow-tools capability")
	}
	// RHEL-family shape: systemd path, dnf→microdnf→yum fallback, wheel admin.
	if oe.InitBinary != "/usr/lib/systemd/systemd" {
		t.Errorf("openEuler init path wrong: %s", oe.InitBinary)
	}
	for _, want := range []string{"dnf -y install --allowerasing", "|| microdnf -y install", "|| yum -y install"} {
		if !strings.Contains(oe.PkgInstall, want) {
			t.Errorf("openEuler install missing %q: %s", want, oe.PkgInstall)
		}
	}
	// The generated selector must carry an openEuler case arm, keyed on the
	// exact mixed-case id.
	if !strings.Contains(ShellSelector(), "openEuler)") {
		t.Error("selector must have an openEuler arm keyed on the verbatim id")
	}
}
