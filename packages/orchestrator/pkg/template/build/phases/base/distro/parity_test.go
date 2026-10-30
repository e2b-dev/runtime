package distro

import (
	"slices"
	"testing"
)

// capabilities is the canonical list of things a provisioned sandbox needs from
// its base image, mapped to the package name(s) that supply each one per family.
// It exists so package sets can't drift apart: adding a family means filling in
// a value for every capability below, and adding a package to a profile means
// declaring which capability it serves.
//
// An entry that is present but empty means the family's base image already ships
// the capability, so the profile deliberately doesn't install it — each one says
// why. A MISSING entry is what the tests here report.
var capabilities = []struct {
	name     string
	byFamily map[string][]string
}{
	{"init", map[string][]string{
		"debian":    {"systemd", "systemd-sysv"},
		"rhel":      {"systemd"},
		"tencentos": {"systemd"},
		"arch":      {"systemd"},
		"alpine":    {"openrc"},
	}},
	{"shell", map[string][]string{
		// bash is Essential on Debian and Ubuntu.
		"debian":    {},
		"rhel":      {"bash"},
		"tencentos": {"bash"},
		"arch":      {"bash"},
		"alpine":    {"bash"},
	}},
	{"shadow-tools", map[string][]string{
		// passwd (useradd/usermod) is priority required on Debian and Ubuntu.
		"debian":    {},
		"rhel":      {"shadow-utils", "passwd"},
		"tencentos": {"shadow-utils", "passwd"},
		"arch":      {"shadow"},
		"alpine":    {"shadow"},
	}},
	{"ssh-server", map[string][]string{
		"debian":    {"openssh-server"},
		"rhel":      {"openssh-server"},
		"tencentos": {"openssh-server"},
		"arch":      {"openssh"},
		"alpine":    {"openssh"},
	}},
	{"sudo", map[string][]string{
		"debian":    {"sudo"},
		"rhel":      {"sudo"},
		"tencentos": {"sudo"},
		"arch":      {"sudo"},
		"alpine":    {"sudo"},
	}},
	{"time-sync", map[string][]string{
		"debian":    {"chrony"},
		"rhel":      {"chrony"},
		"tencentos": {"chrony"},
		"arch":      {"chrony"},
		"alpine":    {"chrony"},
	}},
	{"ca-certificates", map[string][]string{
		"debian":    {"ca-certificates"},
		"rhel":      {"ca-certificates"},
		"tencentos": {"ca-certificates"},
		"arch":      {"ca-certificates"},
		"alpine":    {"ca-certificates"},
	}},
	{"fuse", map[string][]string{
		"debian":    {"fuse3"},
		"rhel":      {"fuse3"},
		"tencentos": {"fuse3"},
		"arch":      {"fuse3"},
		"alpine":    {"fuse3"},
	}},
	{"nfs-client", map[string][]string{
		"debian":    {"nfs-common"},
		"rhel":      {"nfs-utils"},
		"tencentos": {"nfs-utils"},
		"arch":      {"nfs-utils"},
		"alpine":    {"nfs-utils"},
	}},
	{"packet-filter", map[string][]string{
		"debian":    {"iptables", "nftables"},
		"rhel":      {"iptables", "nftables"},
		"tencentos": {"iptables", "nftables"},
		"arch":      {"iptables", "nftables"},
		"alpine":    {"iptables", "nftables"},
	}},
	{"git", map[string][]string{
		"debian":    {"git"},
		"rhel":      {"git"},
		"tencentos": {"git"},
		"arch":      {"git"},
		"alpine":    {"git"},
	}},
	{"jq", map[string][]string{
		"debian":    {"jq"},
		"rhel":      {"jq"},
		"tencentos": {"jq"},
		"arch":      {"jq"},
		"alpine":    {"jq"},
	}},
	{"socat", map[string][]string{
		"debian":    {"socat"},
		"rhel":      {"socat"},
		"tencentos": {"socat"},
		"arch":      {"socat"},
		"alpine":    {"socat"},
	}},
	{"curl", map[string][]string{
		"debian":    {"curl"},
		"rhel":      {"curl"},
		"tencentos": {"curl"},
		"arch":      {"curl"},
		"alpine":    {"curl"},
	}},
	{"pager", map[string][]string{
		"debian":    {"less"},
		"rhel":      {"less"},
		"tencentos": {"less"},
		"arch":      {"less"},
		"alpine":    {"less"},
	}},
	{"ping", map[string][]string{
		"debian":    {"iputils-ping"},
		"rhel":      {"iputils"},
		"tencentos": {"iputils"},
		"arch":      {"iputils"},
		"alpine":    {"iputils"},
	}},
	{"socket-stats", map[string][]string{
		// ss — Debian and RHEL slim images omit it, busybox ships netstat but
		// not ss; Arch's base metapackage already depends on iproute2.
		"debian":    {"iproute2"},
		"rhel":      {"iproute"},
		"tencentos": {"iproute"},
		"arch":      {},
		"alpine":    {"iproute2"},
	}},
	{"find", map[string][]string{
		// The finalize configuration script walks /etc/skel with find(1) when
		// copying skeleton files, so every provisioned image must ship it.
		// findutils is priority: required on Debian and Ubuntu, and a
		// dependency of Arch's base metapackage; busybox ships a find applet
		// with the -mindepth/-type/-print0 predicates the script uses. Only
		// the RPM family's minimal images (Rocky/Alma/UBI-style) omit it.
		"debian":    {},
		"rhel":      {"findutils"},
		"tencentos": {"findutils"},
		"arch":      {},
		"alpine":    {},
	}},
	{"archiver", map[string][]string{
		// Only the RPM family needs tar declared: yum-era CentOS 7 doesn't ship
		// it, while Debian, Arch and busybox all provide tar in the base image.
		"debian":    {},
		"rhel":      {"tar"},
		"tencentos": {"tar"},
		"arch":      {},
		"alpine":    {},
	}},
	{"io-priority", map[string][]string{
		// ionice comes with util-linux everywhere except Alpine, where busybox
		// omits it and envd needs it to reset user processes off realtime IO.
		"debian":    {},
		"rhel":      {},
		"tencentos": {},
		"arch":      {},
		"alpine":    {"util-linux-misc"},
	}},
}

// provisionedProfiles are the profiles whose packages this build installs. A
// profile with no declared package set provisions from a premade base image
// that pins its own packages, so there is nothing to hold to parity.
func provisionedProfiles() []Profile {
	var ps []Profile
	for _, p := range Profiles {
		if p.Packages == nil {
			continue
		}
		ps = append(ps, p)
	}

	return ps
}

// Every family covers every capability with a real package from its own repos.
func TestEveryFamilyCoversEveryCapability(t *testing.T) {
	t.Parallel()
	for _, p := range provisionedProfiles() {
		for _, c := range capabilities {
			pkgs, ok := c.byFamily[p.Key]
			if !ok {
				t.Errorf("capability %q has no entry for family %q", c.name, p.Key)

				continue
			}
			for _, pkg := range pkgs {
				if !slices.Contains(p.Packages, pkg) {
					t.Errorf("family %q claims %q for capability %q but doesn't install it", p.Key, pkg, c.name)
				}
			}
		}
	}
}

// Nothing lands in a package set without saying which capability it serves, so
// a package added to one family can't quietly go missing from the others.
func TestNoUnmappedPackages(t *testing.T) {
	t.Parallel()
	for _, p := range provisionedProfiles() {
		mapped := make(map[string]bool)
		for _, c := range capabilities {
			for _, pkg := range c.byFamily[p.Key] {
				mapped[pkg] = true
			}
		}
		for _, pkg := range p.Packages {
			if !mapped[pkg] {
				t.Errorf("family %q installs %q, which no capability claims", p.Key, pkg)
			}
		}
	}
}

// The capability table can't outlive the profile registry: a family listed here
// that no profile declares is a leftover nobody is checking.
func TestCapabilitiesTrackTheProfileRegistry(t *testing.T) {
	t.Parallel()
	known := make(map[string]bool)
	for _, p := range provisionedProfiles() {
		known[p.Key] = true
	}
	for _, c := range capabilities {
		for family := range c.byFamily {
			if !known[family] {
				t.Errorf("capability %q maps unknown family %q", c.name, family)
			}
		}
	}
}
