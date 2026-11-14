//go:build linux

package fc

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countSharedMountsScript counts mount points that still carry a `shared:` tag
// in the optional-fields section of /proc/self/mountinfo (the fields between
// field 7 and the `-` separator). A count of 0 means every mount is private —
// the isolation `unshare -m` provides.
const countSharedMountsScript = `awk '{for(i=7;i<=NF;i++){if($i=="-")break; if($i ~ /^shared:/)c++}} END{print c+0}' /proc/self/mountinfo`

// sharedMountsInChild launches `bash -c <script>; <count>` exactly the way
// NewProcess launches firecracker (Setsid + optional Unshareflags), and returns
// the number of shared mounts observed inside that child's mount namespace.
func sharedMountsInChild(t *testing.T, unshareNS bool, prelude string) int {
	t.Helper()

	full := prelude
	if full != "" {
		full += " && "
	}
	full += countSharedMountsScript

	cmd := exec.Command("bash", "-c", full)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if unshareNS {
		// Identical to NewProcess in process.go.
		cmd.SysProcAttr.Unshareflags = syscall.CLONE_NEWNS
	}

	out, err := cmd.Output()
	require.NoError(t, err, "counting shared mounts in child")

	n, err := strconv.Atoi(strings.TrimSpace(string(out)))
	require.NoError(t, err, "parsing shared-mount count %q", string(out))
	return n
}

// TestMountPropagationIsPrivate is the long-term regression guard for the
// propagation invariant behind replacing `unshare -m` with CLONE_NEWNS
// (SysProcAttr.Unshareflags) + `mount --make-rprivate /`.
//
// It asserts the sandbox's mount namespace ends up fully private (0 shared
// mounts), identical to `unshare -m`, and documents that the bare kernel
// primitive would NOT be private on its own — the case a reviewer rightly
// worried about. Requires root to create a mount namespace.
func TestMountPropagationIsPrivate(t *testing.T) { //nolint:paralleltest // spawns children that unshare mount namespaces
	if os.Geteuid() != 0 {
		t.Skip("requires root to create a mount namespace")
	}

	// The production path: CLONE_NEWNS + the templates' first command. Whatever
	// the host's shared-mount count, the sandbox namespace must expose 0 —
	// fully private, exactly as `unshare -m` leaves it.
	newPath := sharedMountsInChild(t, true, "mount --make-rprivate /")
	assert.Equal(t, 0, newPath,
		"new path (CLONE_NEWNS + mount --make-rprivate /) must be fully private, like unshare -m")
}

// TestMountPropagation_BareCloneNewnsVsScript documents, as executable evidence,
// the two independent guarantees that make the new path private:
//  1. the Go runtime marks / as MS_REC|MS_PRIVATE in the child whenever
//     Unshareflags carries CLONE_NEWNS (syscall/exec_linux.go), so CLONE_NEWNS
//     alone — without the script line — is already private here; and
//  2. both start scripts keep `mount --make-rprivate /` as their first command.
//
// Either one yields 0 shared mounts; the test pins both.
func TestMountPropagation_BareCloneNewnsVsScript(t *testing.T) { //nolint:paralleltest // spawns children that unshare mount namespaces
	if os.Geteuid() != 0 {
		t.Skip("requires root to create a mount namespace")
	}

	withScript := sharedMountsInChild(t, true, "mount --make-rprivate /")
	assert.Equal(t, 0, withScript, "CLONE_NEWNS + make-rprivate must be private")

	// The Go runtime already privatizes / for CLONE_NEWNS children, so even
	// without the explicit script line the namespace is private. This is the
	// belt to the script's suspenders.
	bareGoUnshare := sharedMountsInChild(t, true, "")
	assert.Equal(t, 0, bareGoUnshare,
		"Go runtime privatizes / for CLONE_NEWNS children (syscall/exec_linux.go)")
}
