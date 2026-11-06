//go:build linux

package network

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netns"
)

func TestDeleteNamespaceRemovesAnAnchorWithoutItsMount(t *testing.T) {
	t.Parallel()

	if os.Geteuid() != 0 {
		t.Skip("requires root for the netns directory")
	}

	name := fmt.Sprintf("e2b-test-anchor-%d", os.Getpid())
	anchor := filepath.Join(NetNamespacesDir, name)
	require.NoError(t, os.MkdirAll(NetNamespacesDir, 0o755))
	require.NoError(t, os.WriteFile(anchor, nil, 0o444))
	t.Cleanup(func() { _ = os.Remove(anchor) })

	require.Error(t, netns.DeleteNamed(name), "netns.DeleteNamed fails on an anchor with no mount")
	require.NoError(t, DeleteNamespace(name))
	_, err := os.Stat(anchor)
	require.True(t, os.IsNotExist(err), "the anchor is removed")
}

func TestDeleteNamespaceRemovesAMountedNamespace(t *testing.T) { //nolint:paralleltest // switches the thread's netns
	if os.Geteuid() != 0 {
		t.Skip("requires root for netns")
	}

	name := fmt.Sprintf("e2b-test-ns-%d", os.Getpid())
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	hostNS, err := netns.Get()
	require.NoError(t, err)
	defer hostNS.Close()
	ns, err := netns.NewNamed(name)
	require.NoError(t, err)
	require.NoError(t, netns.Set(hostNS))
	require.NoError(t, ns.Close())

	require.NoError(t, DeleteNamespace(name))
	_, err = os.Stat(filepath.Join(NetNamespacesDir, name))
	require.True(t, os.IsNotExist(err))
	require.True(t, isNamespaceNotExist(DeleteNamespace(name)), "a second delete reports the namespace absent")
}
