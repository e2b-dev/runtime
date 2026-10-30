// Package distro makes template-build provisioning distro-aware: it selects a
// declared per-family Profile by the base image's /etc/os-release ID rather than
// probing for a package manager. Supported: the systemd family (Debian/Ubuntu,
// Fedora/CentOS/Rocky/Alma, Arch) and Alpine on OpenRC. Derivatives are
// provisioned via ID_LIKE as a best-effort guess with a warning; kernel-dependent
// ids (RejectedIDs) and anything unmatched are rejected with a clear error.
package distro

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// Version forces a base-layer rebuild for provisioning changes the generated
// selector text can't otherwise capture; bump it when the contract changes.
const Version = "1"

// Fingerprint hashes the whole generated provisioning contract into the
// base-layer cache key, so any profile or init-setup change rebuilds the base.
func Fingerprint() string {
	sum := sha256.Sum256([]byte(Version + "\x00" + ShellSelector()))

	return hex.EncodeToString(sum[:])
}

// Profile is the declared, per-family provisioning contract. IDs are the
// /etc/os-release values that map to the family; PkgQueryBody, PkgInstall,
// CARefresh and Bootstrap are shell fragments spliced into the generated
// selector (Bootstrap, if set, runs first — for premade images with no FHS
// userland yet).
type Profile struct {
	Key          string
	Init         InitSystem
	IDs          []string
	Packages     []string
	PkgQueryBody string
	PkgInstall   string
	InitBinary   string
	TimeSyncUnit string
	SSHUnit      string
	AdminGroup   string
	CABundle     string
	CARefresh    string
	Bootstrap    string
}

var Profiles = []Profile{
	{
		Key:  "debian",
		Init: InitSystemd,
		IDs:  []string{"debian", "ubuntu"},
		Packages: []string{
			"systemd", "systemd-sysv", "openssh-server", "sudo", "chrony", "socat",
			"curl", "ca-certificates", "fuse3", "iptables", "git", "nfs-common",
			"less", "nftables", "iputils-ping", "jq", "iproute2",
		},
		PkgQueryBody: `dpkg-query -W -f='${Status}' "$1" 2>/dev/null | grep -q "install ok installed"`,
		PkgInstall:   `apt-get -q update && DEBIAN_FRONTEND=noninteractive DEBCONF_NOWARNINGS=yes apt-get -qq -o=Dpkg::Use-Pty=0 install -y --no-install-recommends "$@"`,
		InitBinary:   "/lib/systemd/systemd",
		TimeSyncUnit: "chrony",
		SSHUnit:      "ssh",
		AdminGroup:   "sudo",
		CABundle:     "/etc/ssl/certs/ca-certificates.crt",
		CARefresh:    "update-ca-certificates",
	},
	{
		Key:  "rhel", // Fedora, CentOS Stream, Rocky, Alma
		Init: InitSystemd,
		// Deliberately not rhel/ol/amzn. Sandboxes always boot E2B's kernel, so
		// /lib/modules is empty and SELinux is off — which is most of what RHEL
		// and Oracle Linux are chosen for (kABI, signed kmods, UEK), and
		// kernel-devel would resolve against a kernel that isn't running. RHEL's
		// UBI repos also carry neither chrony nor nfs-utils. Accepting those IDs
		// promised a fidelity this can't deliver; they now fail fast instead.
		IDs: []string{"fedora", "centos", "rocky", "almalinux"},
		// "iptables" (not iptables-nft) and "tar" cover yum-era CentOS 7, which
		// lacks the nft package and doesn't ship tar.
		Packages: []string{
			"systemd", "shadow-utils", "passwd", "openssh-server", "sudo", "chrony",
			"socat", "curl", "ca-certificates", "fuse3", "iptables", "git",
			"nfs-utils", "less", "nftables", "iputils", "jq", "bash", "tar",
			"iproute", "findutils",
		},
		PkgQueryBody: `rpm -q "$1" >/dev/null 2>&1`,
		// dnf → microdnf → yum spans the family (yum for CentOS 7); errors reach
		// the build log. --allowerasing goes AFTER the subcommand: dnf5 (Fedora
		// 41+, where /usr/bin/dnf IS dnf5) rejects it before one with "Unknown
		// argument" and exits 2, which put two errors in every modern Fedora
		// build log and quietly demoted the install to the flagless
		// microdnf/yum fallback, so the flag never applied. dnf4 accepts either
		// position.
		PkgInstall:   `dnf -y install --allowerasing "$@" || microdnf -y install "$@" || yum -y install "$@"`,
		InitBinary:   "/usr/lib/systemd/systemd",
		TimeSyncUnit: "chronyd",
		SSHUnit:      "sshd",
		AdminGroup:   "wheel",
		CABundle:     "/etc/ssl/certs/ca-certificates.crt",
		// update-ca-trust never writes ca-certificates.crt (a Debian name), so
		// link the bundle envd's ExecStartPre expects to the extracted PEM.
		CARefresh: `update-ca-trust extract && ln -sf /etc/pki/tls/certs/ca-bundle.crt "$E2B_CA_BUNDLE"`,
	},
	{
		// TencentOS Server, OpenCloudOS and OpenCloudOS Stream
		Key:  "tencentos",
		Init: InitSystemd,
		IDs:  []string{"tencentos", "opencloudos"},
		Packages: []string{
			"systemd", "shadow-utils", "passwd", "openssh-server", "sudo", "chrony",
			"socat", "curl", "ca-certificates", "fuse3", "iptables", "git",
			"nfs-utils", "less", "nftables", "iputils", "jq", "bash", "tar",
			"iproute", "findutils",
		},
		PkgQueryBody: `rpm -q "$1" >/dev/null 2>&1`,
		PkgInstall:   `dnf -y install --allowerasing "$@"`,
		InitBinary:   "/usr/lib/systemd/systemd",
		TimeSyncUnit: "chronyd",
		SSHUnit:      "sshd",
		AdminGroup:   "wheel",
		CABundle:     "/etc/ssl/certs/ca-certificates.crt",
		CARefresh:    `update-ca-trust extract && ln -sf /etc/pki/tls/certs/ca-bundle.crt "$E2B_CA_BUNDLE"`,
	},
	{
		Key:  "arch",
		Init: InitSystemd,
		IDs:  []string{"arch", "archarm"},
		Packages: []string{
			"systemd", "shadow", "openssh", "sudo", "chrony", "socat", "curl",
			"ca-certificates", "fuse3", "iptables", "git", "nfs-utils", "less",
			"nftables", "iputils", "jq", "bash",
		},
		PkgQueryBody: `pacman -Q "$1" >/dev/null 2>&1`,
		// -Syu (not -Sy then -S): Arch documents partial upgrades as unsupported.
		PkgInstall:   `pacman -Syu --noconfirm --needed "$@"`,
		InitBinary:   "/usr/lib/systemd/systemd",
		TimeSyncUnit: "chronyd",
		SSHUnit:      "sshd",
		AdminGroup:   "wheel",
		CABundle:     "/etc/ssl/certs/ca-certificates.crt",
		// Arch ships /etc/ssl/certs/ca-certificates.crt as a symlink to the p11-kit
		// bundle update-ca-trust extract regenerates — no manual link, unlike RHEL.
		CARefresh: "update-ca-trust extract",
	},
	{
		Key:  "alpine",
		Init: InitOpenRC,
		IDs:  []string{"alpine"},
		// shadow provides useradd/usermod (busybox's adduser takes different flags).
		// util-linux-misc provides ionice, which busybox does not ship: envd runs at
		// realtime IO (supervise-daemon --ionice 1:4) and resets each user process to
		// best-effort with it, so without the binary they'd inherit realtime IO.
		Packages: []string{
			"openrc", "shadow", "openssh", "sudo", "chrony", "socat", "curl",
			"ca-certificates", "fuse3", "iptables", "git", "nfs-utils", "less",
			"nftables", "iputils", "jq", "bash", "util-linux-misc", "iproute2",
		},
		PkgQueryBody: `apk info -e "$1" >/dev/null 2>&1`,
		PkgInstall:   `apk add --no-cache "$@"`,
		InitBinary:   "/bin/busybox",
		TimeSyncUnit: "chronyd",
		SSHUnit:      "sshd",
		AdminGroup:   "wheel",
		CABundle:     "/etc/ssl/certs/ca-certificates.crt",
		CARefresh:    "update-ca-certificates",
	},
	{
		Key:  "nixos",
		Init: InitNixOS,
		IDs:  []string{"nixos"},
		// Premade: packages and services are declared in the image's own NixOS
		// configuration, so there is no package manager to drive at build time.
		Packages:     nil,
		PkgQueryBody: "true",
		PkgInstall:   `echo "[provision] ERROR: NixOS images are premade — packages must be declared in the image's NixOS configuration" >&2; exit 1`,
		// Not $toplevel/init: from NixOS 25.05 that IS the systemd binary, and
		// activation moved into the systemd stage-1 initrd. We boot the rootfs
		// directly with no initrd, so PID 1 would start against an unpopulated
		// /etc and freeze on "Unit default.target not found". The image ships
		// this shim, which activates and then execs systemd — what the pre-25.05
		// stage-2 init did (nixos-base-image/build.sh).
		InitBinary:   "/sbin/e2b-nixos-init",
		TimeSyncUnit: "chronyd",
		// Left empty on purpose: services.openssh is declared in the image's
		// configuration, and the NixOS init setup never enables units.
		SSHUnit:    "",
		AdminGroup: "wheel",
		CABundle:   "/etc/ssl/certs/ca-certificates.crt",
		// The bundle appears at first activation; nothing to refresh pre-boot.
		CARefresh: `echo "NixOS: the CA bundle is provided by the image configuration at first activation; nothing to refresh at provision time"`,
		// No FHS userland pre-activation — put the baked busybox on PATH first.
		Bootstrap: `E2B_BB_DIR=/run/e2b-tools
    /usr/bin/busybox mkdir -p "$E2B_BB_DIR"
    /usr/bin/busybox --install -s "$E2B_BB_DIR"
    export PATH="$E2B_BB_DIR:$PATH"`,
	},
}

// SupportedIDs returns every os-release ID the selector accepts.
func SupportedIDs() []string {
	var ids []string
	for _, p := range Profiles {
		ids = append(ids, p.IDs...)
	}

	return ids
}

// RejectedIDs are distro ids we refuse even though their ID_LIKE names a
// family we do support. Oracle Linux and Amazon Linux both declare
// ID_LIKE=fedora, so without this the ID_LIKE fallback below would quietly
// re-admit exactly the images the rhel profile documents as out of scope.
var RejectedIDs = []string{"rhel", "ol", "amzn"}

// ShellSelector generates the POSIX-sh block provision.sh sources: it switches
// on the guest's $E2B_DISTRO_ID and defines the profile's packages, shell
// functions, init path, time-sync unit, admin group and CA handling.
//
// An unknown id retries each $E2B_ID_LIKE token in order and provisions the
// first matching family — best effort, with a customer-visible warning.
// RejectedIDs, ids matching nothing, and images without /etc/os-release exit 1.
func ShellSelector() string {
	var b strings.Builder

	// Selection lives in a function so it can be retried per ID_LIKE token.
	// Assignments and function definitions inside a POSIX-sh function are
	// global, so the caller sees the profile the same way it always has.
	// The match is reported via e2b_profile_matched, not the return status —
	// a function called as an if-condition runs with errexit suppressed,
	// which would swallow Bootstrap failures inside a matched arm.
	b.WriteString("e2b_select_profile() {\n")
	b.WriteString("  e2b_profile_matched=\n")
	b.WriteString(`  case "$1" in` + "\n")
	for _, p := range Profiles {
		fmt.Fprintf(&b, "  %s)\n", strings.Join(p.IDs, "|"))
		if p.Bootstrap != "" {
			fmt.Fprintf(&b, "    %s\n", p.Bootstrap)
		}
		fmt.Fprintf(&b, "    E2B_PACKAGES=%q\n", strings.Join(p.Packages, " "))
		fmt.Fprintf(&b, "    e2b_pkg_query() { %s; }\n", p.PkgQueryBody)
		fmt.Fprintf(&b, "    e2b_pkg_install() { %s; }\n", p.PkgInstall)
		fmt.Fprintf(&b, "    E2B_INIT_BIN=%q\n", p.InitBinary)
		fmt.Fprintf(&b, "    E2B_TIMESYNC_UNIT=%q\n", p.TimeSyncUnit)
		fmt.Fprintf(&b, "    E2B_SSH_UNIT=%q\n", p.SSHUnit)
		fmt.Fprintf(&b, "    E2B_ADMIN_GROUP=%q\n", p.AdminGroup)
		fmt.Fprintf(&b, "    E2B_CA_BUNDLE=%q\n", p.CABundle)
		fmt.Fprintf(&b, "    e2b_ca_refresh() { %s; }\n", p.CARefresh)
		fmt.Fprintf(&b, "    E2B_INIT_SYSTEM=%q\n", p.Init)
		fmt.Fprintf(&b, "    e2b_init_setup() {\n%s\n    }\n", indentBlock(initSetup[p.Init], "        "))
		fmt.Fprintf(&b, "    e2b_profile_matched=1\n")
		fmt.Fprintf(&b, "    ;;\n")
	}
	fmt.Fprintf(&b, "  *)\n    ;;\n")
	b.WriteString("  esac\n}\n\n")

	b.WriteString(`e2b_select_profile "$E2B_DISTRO_ID"` + "\n")
	b.WriteString(`if [ -z "$e2b_profile_matched" ]; then` + "\n")

	// Deliberate rejections are checked before the fallback, so they keep
	// failing fast with their own reason instead of being matched by ID_LIKE.
	fmt.Fprintf(&b, "  case \"$E2B_DISTRO_ID\" in\n")
	fmt.Fprintf(&b, "  %s)\n", strings.Join(RejectedIDs, "|"))
	fmt.Fprintf(&b, "    echo \"[provision] ERROR: base image distribution ID='$E2B_DISTRO_ID' is not supported.\" >&2\n")
	fmt.Fprintf(&b, "    echo \"[provision] Sandboxes boot E2B's kernel, so the kABI, signed modules and SELinux these images are chosen for are unavailable.\" >&2\n")
	fmt.Fprintf(&b, "    exit 1\n")
	fmt.Fprintf(&b, "    ;;\n")
	fmt.Fprintf(&b, "  esac\n")

	b.WriteString("  e2b_like_match=\n")
	b.WriteString(`  for e2b_like in $E2B_ID_LIKE; do` + "\n")
	b.WriteString(`    e2b_select_profile "$e2b_like"` + "\n")
	b.WriteString(`    if [ -n "$e2b_profile_matched" ]; then` + "\n")
	b.WriteString("      e2b_like_match=$e2b_like\n")
	b.WriteString("      break\n")
	b.WriteString("    fi\n")
	b.WriteString("  done\n")

	b.WriteString(`  if [ -z "$e2b_like_match" ]; then` + "\n")
	fmt.Fprintf(&b, "    echo \"[provision] ERROR: unsupported base image distribution: ID='${E2B_DISTRO_ID:-unknown}'.\" >&2\n")
	fmt.Fprintf(&b, "    echo \"[provision] E2B template builds support: %s.\" >&2\n", strings.Join(SupportedIDs(), ", "))
	fmt.Fprintf(&b, "    exit 1\n")
	b.WriteString("  fi\n")

	fmt.Fprintf(&b, "  echo \"[provision] WARNING: base image distribution ID='$E2B_DISTRO_ID' is not officially supported; provisioning it as '$e2b_like_match' from ID_LIKE. This is best effort and untested.\" >&2\n")
	b.WriteString("fi\n")

	return b.String()
}
