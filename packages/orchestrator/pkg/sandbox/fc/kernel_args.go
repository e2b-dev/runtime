//go:build linux

package fc

import (
	"fmt"
	"maps"
	"runtime"
	"slices"
	"strings"
)

type KernelArgs map[string]string

// reservedCmdlineParams are the parameters the orchestrator sets itself and whose values the
// rest of the system depends on: the init binary, the root device and its mount flags, the
// guest's network configuration, the clocksource, and the console/verbosity settings that the
// log-capture options drive.
//
// A caller-supplied fragment may not set them. Not because the operator is untrusted — these
// come from an operator-set flag, not from a sandbox — but because overriding one produces a
// sandbox that fails indistinguishably from an orchestrator bug: a guest that never boots, has
// no network, writes its console where nobody reads it, or whose clock jumps after a resume.
// selinux is reserved for the same reason: the default disables it, and a fragment that
// re-arms it hangs a policy-shipping image before envd starts. Everything outside this set
// is guest-kernel tuning the guest alone lives with.
var reservedCmdlineParams = map[string]struct{}{
	"init":        {},
	"clocksource": {},
	"root":        {},
	"ip":          {},
	"console":     {},
	"rootflags":   {},
	"panic":       {},
	"reboot":      {},
	"loglevel":    {},
	"quiet":       {},
	"selinux":     {},
}

// ParseCmdlineArgs parses a guest kernel command line fragment the way the kernel itself does:
// parameters separated by whitespace, each split at its FIRST '=' into a name and a value, and
// a parameter with no '=' carrying an empty value.
//
// Parsing a fragment rather than accepting a structured map is what makes the reserved-name
// check below sound. A name here can never contain whitespace or '=', because those are the
// delimiters that produced it — so one entry cannot smuggle a second parameter past the check.
// A map of names to values has no such guarantee: {"init=/evil": ""} is a single entry whose
// name is not "init", and renders as a complete parameter of its own.
func ParseCmdlineArgs(fragment string) (map[string]string, error) {
	fields := strings.Fields(fragment)
	if len(fields) == 0 {
		return nil, nil
	}

	args := make(map[string]string, len(fields))
	for _, f := range fields {
		name, value, _ := strings.Cut(f, "=")
		if name == "" {
			return nil, fmt.Errorf("guest kernel cmdline parameter %q has an empty name", f)
		}

		args[name] = value
	}

	return args, nil
}

// ValidateCmdlineArgs reports whether parsed guest kernel arguments may be applied to a boot.
//
// Rejection is all-or-nothing on purpose. Dropping only the offending parameter would boot a
// command line nobody specified — neither what the operator asked for nor today's default —
// which is the hardest kind of configuration bug to reason about. A rejected fragment falls
// back to the default command line, a state that has been running in production all along.
func ValidateCmdlineArgs(args map[string]string) error {
	for _, name := range slices.Sorted(maps.Keys(args)) {
		if _, reserved := reservedCmdlineParams[name]; reserved {
			return fmt.Errorf("guest kernel cmdline parameter %q is reserved by the orchestrator", name)
		}
	}

	return nil
}

// hostArch is the architecture the guest kernel boots on. Firecracker never emulates, so it
// is the host's; a variable only so tests can boot the other one.
var hostArch = runtime.GOARCH

// buildKernelArgs assembles the guest kernel command line for a boot.
//
// ipv4 is the pre-formatted `ip=` value; everything else is derived from options.
func buildKernelArgs(ipv4 string, options ProcessOptions) KernelArgs {
	return buildKernelArgsFor(hostArch, ipv4, options)
}

func buildKernelArgsFor(arch string, ipv4 string, options ProcessOptions) KernelArgs {
	args := KernelArgs{
		// Disable kernel logs for production to speed the FC operations
		// https://github.com/firecracker-microvm/firecracker/blob/main/docs/prod-host-setup.md#logging-and-performance
		"quiet":    "",
		"loglevel": "1",

		// Define kernel init path
		"init": options.InitScriptPath,

		// Networking IPv4 and IPv6
		"ip":            ipv4,
		"ipv6.disable":  "0",
		"ipv6.autoconf": "1",

		// Wait 1 second before exiting FC after panic or reboot
		"panic": "1",

		"reboot":           "k",
		"pci":              "off",
		"random.trust_cpu": "on",

		"rootflags": ext4RootFlags,

		// The kernel defaults to SELinux; an image whose /etc/selinux/config says enforcing
		// would hang on the unlabeled rootfs before envd starts.
		"selinux": "0",
	}

	// The i8042 controller and kvm-clock exist only on x86; an arm64 guest has neither and
	// keeps the architected timer as its clocksource. The serial console is ttyS0 on both:
	// Firecracker exposes a 16550-compatible UART on arm64 too, and the arm64 guest kernel
	// is built with the 8250 driver.
	if arch == "amd64" {
		args["i8042.nokbd"] = ""
		args["i8042.noaux"] = ""

		if options.KvmClock {
			args["clocksource"] = "kvm-clock"
		}
	}

	if options.SystemdToKernelLogs {
		args["systemd.journald.forward_to_console"] = ""
	}

	if options.KernelLogs || options.SystemdToKernelLogs {
		// Forward kernel logs to the ttyS0, which will be picked up by the stdout of FC process
		delete(args, "quiet")
		args["console"] = "ttyS0"
		args["loglevel"] = "5" // KERN_NOTICE
	}

	// Applied last, and re-validated here rather than trusted. The boundary that reads the
	// flag validates too, but this is the only place the arguments actually reach a guest,
	// and a variant that got this far invalid must not be applied piecemeal — dropping the
	// whole overlay leaves the command line byte-identical to the default.
	if ValidateCmdlineArgs(options.CmdlineArgs) == nil {
		maps.Copy(args, options.CmdlineArgs)
	}

	return args
}

func (ka KernelArgs) String() string {
	args := make([]string, 0, len(ka))
	for k, v := range ka {
		if v == "" {
			args = append(args, k)
		} else {
			args = append(args, fmt.Sprintf("%s=%s", k, v))
		}
	}
	slices.Sort(args) // optional: for consistent output

	return strings.Join(args, " ")
}
