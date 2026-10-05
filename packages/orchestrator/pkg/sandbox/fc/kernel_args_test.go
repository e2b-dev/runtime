//go:build linux

package fc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testIPv4 = "169.254.0.21::169.254.0.22:255.255.255.252:instance:eth0:off:tap0"

// The default variant is the command line every team that does not opt in boots with, so
// it is pinned literally rather than derived — a test that rebuilt the expectation from
// the same map the code uses would pass through any change to that map. Changing the
// default is a reviewed edit of these strings.
func TestBuildKernelArgs_DefaultIsPinned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		options ProcessOptions
		want    string
	}{
		{
			name:    "production defaults",
			options: ProcessOptions{InitScriptPath: "/sbin/init"},
			want: "i8042.noaux i8042.nokbd init=/sbin/init ip=" + testIPv4 +
				" ipv6.autoconf=1 ipv6.disable=0 loglevel=1 panic=1 pci=off quiet" +
				" random.trust_cpu=on reboot=k rootflags=discard selinux=0",
		},
		{
			name:    "kvm clock",
			options: ProcessOptions{InitScriptPath: "/sbin/init", KvmClock: true},
			want: "clocksource=kvm-clock i8042.noaux i8042.nokbd init=/sbin/init ip=" + testIPv4 +
				" ipv6.autoconf=1 ipv6.disable=0 loglevel=1 panic=1 pci=off quiet" +
				" random.trust_cpu=on reboot=k rootflags=discard selinux=0",
		},
		{
			// Kernel logs drop `quiet` and raise the log level; asserted because the
			// variant overlay is applied after these adjustments.
			name:    "kernel logs",
			options: ProcessOptions{InitScriptPath: "/sbin/init", KernelLogs: true},
			want: "console=ttyS0 i8042.noaux i8042.nokbd init=/sbin/init ip=" + testIPv4 +
				" ipv6.autoconf=1 ipv6.disable=0 loglevel=5 panic=1 pci=off" +
				" random.trust_cpu=on reboot=k rootflags=discard selinux=0",
		},
		{
			name:    "systemd to kernel logs",
			options: ProcessOptions{InitScriptPath: "/sbin/init", SystemdToKernelLogs: true},
			want: "console=ttyS0 i8042.noaux i8042.nokbd init=/sbin/init ip=" + testIPv4 +
				" ipv6.autoconf=1 ipv6.disable=0 loglevel=5 panic=1 pci=off" +
				" random.trust_cpu=on reboot=k rootflags=discard selinux=0" +
				" systemd.journald.forward_to_console",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, buildKernelArgsFor("amd64", testIPv4, tt.options).String())
		})
	}
}

// An arm64 guest gets the same command line minus the two x86-only devices: no i8042
// controller to silence and no kvm-clock to select (the architected timer is the default).
// Pinned literally for the same reason as the amd64 defaults above.
func TestBuildKernelArgs_Arm64DropsX86OnlyParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		options ProcessOptions
		want    string
	}{
		{
			name:    "production defaults",
			options: ProcessOptions{InitScriptPath: "/sbin/init"},
			want: "init=/sbin/init ip=" + testIPv4 +
				" ipv6.autoconf=1 ipv6.disable=0 loglevel=1 panic=1 pci=off quiet" +
				" random.trust_cpu=on reboot=k rootflags=discard selinux=0",
		},
		{
			name:    "kvm clock requested is not applied",
			options: ProcessOptions{InitScriptPath: "/sbin/init", KvmClock: true},
			want: "init=/sbin/init ip=" + testIPv4 +
				" ipv6.autoconf=1 ipv6.disable=0 loglevel=1 panic=1 pci=off quiet" +
				" random.trust_cpu=on reboot=k rootflags=discard selinux=0",
		},
		{
			name:    "kernel logs still go to ttyS0",
			options: ProcessOptions{InitScriptPath: "/sbin/init", KernelLogs: true},
			want: "console=ttyS0 init=/sbin/init ip=" + testIPv4 +
				" ipv6.autoconf=1 ipv6.disable=0 loglevel=5 panic=1 pci=off" +
				" random.trust_cpu=on reboot=k rootflags=discard selinux=0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, buildKernelArgsFor("arm64", testIPv4, tt.options).String())
		})
	}
}

// buildKernelArgs itself must follow the host's architecture, not a fixed one: booted "on"
// arm64 it must drop the x86-only parameters. Not parallel — it swaps the package variable.
func TestBuildKernelArgs_UsesHostArch(t *testing.T) { //nolint:paralleltest
	saved := hostArch
	hostArch = "arm64"
	t.Cleanup(func() { hostArch = saved })

	line := buildKernelArgs(testIPv4, ProcessOptions{InitScriptPath: "/sbin/init", KvmClock: true}).String()
	assert.NotContains(t, line, "i8042")
	assert.NotContains(t, line, "clocksource")
	assert.Equal(t, buildKernelArgsFor("arm64", testIPv4, ProcessOptions{InitScriptPath: "/sbin/init", KvmClock: true}).String(), line)
}

// Supplied args must be overlaid exactly, changing nothing else. Asserted as a diff
// against the default rather than as another literal, so that a future change to the
// shared defaults cannot make this test silently stop checking the overlay.
func TestBuildKernelArgs_VariantArgsAreOverlaid(t *testing.T) {
	t.Parallel()

	options := ProcessOptions{InitScriptPath: "/sbin/init", KvmClock: true}
	base := buildKernelArgs(testIPv4, options)

	options.CmdlineArgs = map[string]string{"psi": "1"}
	withPSI := buildKernelArgs(testIPv4, options)

	require.Equal(t, "1", withPSI["psi"], "the supplied args must be applied")
	assert.Contains(t, withPSI.String(), " psi=1 ")

	delete(withPSI, "psi")
	assert.Equal(t, base, withPSI, "nothing outside the supplied args may change")
}

// A variant naming a reserved key must reach the guest as the default command line —
// not partially applied, and not as a boot failure. Dropping only the offending key
// would boot a command line nobody specified.
func TestBuildKernelArgs_ReservedKeyRejectsWholeVariant(t *testing.T) {
	t.Parallel()

	options := ProcessOptions{InitScriptPath: "/sbin/init"}
	want := buildKernelArgs(testIPv4, options).String()

	options.CmdlineArgs = map[string]string{"psi": "1", "init": "/evil"}
	got := buildKernelArgs(testIPv4, options)

	// Assert on the rendered command line, not on the map: testify's NotContains
	// iterates a map's KEYS, so asserting against the map would miss a bad value.
	assert.Equal(t, want, got.String(), "a rejected variant must leave the default cmdline")
	assert.NotContains(t, got.String(), "/evil")
	assert.NotContains(t, got.String(), "psi=1", "the valid key must not be applied either")
}

func TestParseCmdlineArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fragment string
		want     map[string]string
		wantErr  bool
	}{
		{name: "empty is nothing", fragment: "", want: nil},
		{name: "whitespace only is nothing", fragment: "   \t ", want: nil},
		{name: "one parameter", fragment: "psi=1", want: map[string]string{"psi": "1"}},
		{
			name: "several parameters", fragment: "psi=1 nokaslr",
			want: map[string]string{"psi": "1", "nokaslr": ""},
		},
		{
			// Extra whitespace is just separation, exactly as the kernel treats it.
			name: "runs of whitespace separate", fragment: "  psi=1   nokaslr  ",
			want: map[string]string{"psi": "1", "nokaslr": ""},
		},
		{
			// Split at the FIRST '=', so a value may itself contain one.
			name: "value containing equals", fragment: "foo=a=b",
			want: map[string]string{"foo": "a=b"},
		},
		{
			// This is the shape that bypassed the reserved check when the flag carried a
			// map: as a fragment it parses to the name "init", which the check then sees.
			name: "a reserved parameter parses to its real name", fragment: "init=/evil",
			want: map[string]string{"init": "/evil"},
		},
		{name: "empty name", fragment: "=x", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseCmdlineArgs(tt.fragment)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// Parsing and validation together are what protect the reserved parameters. Neither alone
// does: parsing alone would happily apply init=/evil, and validation alone was what a
// structured map could sneak a delimiter past.
func TestParseThenValidateRejectsReserved(t *testing.T) {
	t.Parallel()

	for _, fragment := range []string{"init=/evil", "psi=1 init=/evil", "clocksource=tsc"} {
		t.Run(fragment, func(t *testing.T) {
			t.Parallel()

			args, err := ParseCmdlineArgs(fragment)
			require.NoError(t, err)
			assert.Error(t, ValidateCmdlineArgs(args))
		})
	}
}

// Regression guard for policy-shipping base images (fedora + selinux-policy-targeted):
// the guest kernel defaults to SELinux, the OCI rootfs is unlabeled, and a boot without
// selinux=0 freezes PID 1 before envd starts. The parameter must survive every boot mode
// and every accepted overlay, and no overlay may take it away.
func TestBuildKernelArgs_SELinuxDisabledOnEveryBoot(t *testing.T) {
	t.Parallel()

	for name, options := range map[string]ProcessOptions{
		"defaults":               {InitScriptPath: "/sbin/init"},
		"kvm clock":              {InitScriptPath: "/sbin/init", KvmClock: true},
		"kernel logs":            {InitScriptPath: "/sbin/init", KernelLogs: true},
		"systemd to kernel logs": {InitScriptPath: "/sbin/init", SystemdToKernelLogs: true},
		"accepted overlay":       {InitScriptPath: "/sbin/init", CmdlineArgs: map[string]string{"psi": "1", "nokaslr": ""}},
		"rejected overlay":       {InitScriptPath: "/sbin/init", CmdlineArgs: map[string]string{"selinux": "1"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			args := buildKernelArgs(testIPv4, options)
			assert.Equal(t, "0", args["selinux"], "guest kernel must boot with SELinux disabled")
			assert.Contains(t, " "+args.String()+" ", " selinux=0 ")
		})
	}

	require.Error(t, ValidateCmdlineArgs(map[string]string{"selinux": "0"}),
		"even a matching value is rejected: the default owns this key")
}

func TestValidateCmdlineArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    map[string]string
		wantErr bool
	}{
		{name: "nil is fine", args: nil},
		{name: "empty is fine", args: map[string]string{}},
		{name: "a guest knob is fine", args: map[string]string{"psi": "1"}},
		{name: "a valueless knob is fine", args: map[string]string{"nokaslr": ""}},
		{name: "clocksource reserved", args: map[string]string{"clocksource": "tsc"}, wantErr: true},
		// Every reserved key, so adding one to the list without a test is not possible.
		{name: "init reserved", args: map[string]string{"init": "/x"}, wantErr: true},
		{name: "clocksource reserved", args: map[string]string{"clocksource": "tsc"}, wantErr: true},
		{name: "root reserved", args: map[string]string{"root": "/dev/x"}, wantErr: true},
		{name: "ip reserved", args: map[string]string{"ip": "1.2.3.4"}, wantErr: true},
		{name: "console reserved", args: map[string]string{"console": "ttyS0"}, wantErr: true},
		{name: "rootflags reserved", args: map[string]string{"rootflags": "ro"}, wantErr: true},
		{name: "panic reserved", args: map[string]string{"panic": "0"}, wantErr: true},
		{name: "reboot reserved", args: map[string]string{"reboot": "t"}, wantErr: true},
		{name: "loglevel reserved", args: map[string]string{"loglevel": "7"}, wantErr: true},
		{name: "quiet reserved", args: map[string]string{"quiet": ""}, wantErr: true},
		{name: "selinux reserved", args: map[string]string{"selinux": "1"}, wantErr: true},
		// Whitespace would split into extra arguments once rendered, letting one
		// key smuggle in another - including a reserved one.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateCmdlineArgs(tt.args)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
