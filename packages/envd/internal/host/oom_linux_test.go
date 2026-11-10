//go:build linux

package host

import (
	"bytes"
	"context"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Userspace can write to /dev/kmsg, but the kernel must never give it a kernel priority.
func TestKernelLogMarksUserspaceRecords(t *testing.T) {
	t.Parallel()

	r, err := openKmsg(kmsgPath)
	if err != nil {
		t.Skipf("can't read %s: %v", kmsgPath, err)
	}

	w, err := os.OpenFile(kmsgPath, os.O_WRONLY, 0)
	if err != nil {
		r.Close()
		t.Skipf("can't write %s: %v", kmsgPath, err)
	}
	defer w.Close()

	marker := []byte("envd-kmsg-test-" + strconv.FormatInt(time.Now().UnixNano(), 10))
	_, err = w.Write(append(append([]byte("<3>"), marker...), '\n'))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	records := make(chan string, 1)
	go func() {
		defer r.Close()
		_ = followKernelLog(ctx, r, func(record []byte) {
			if bytes.Contains(record, marker) {
				records <- string(record)
			}
		}, func() {})
	}()

	select {
	case record := <-records:
		_, _, ok := parseKernelRecord(record)
		assert.False(t, ok, "a record written from userspace passed as the kernel's: %q", record)
	case <-time.After(10 * time.Second):
		t.Fatal("the written record never came back from the kernel log")
	}
}

// The watcher, run the way envd runs it, against the real kernel log.
func TestOOMWatcherOnKernelLog(t *testing.T) {
	t.Parallel()

	r, err := openKmsg(kmsgPath)
	if err != nil {
		t.Skipf("can't read %s: %v", kmsgPath, err)
	}
	r.Close()

	t.Run("catches up with the log", func(t *testing.T) {
		t.Parallel()

		waitCaughtUp(t, startWatcher(t))
	})

	t.Run("ignores a kill written from userspace", func(t *testing.T) {
		t.Parallel()

		kmsg, err := os.OpenFile(kmsgPath, os.O_WRONLY, 0)
		if err != nil {
			t.Skipf("can't write %s: %v", kmsgPath, err)
		}
		defer kmsg.Close()

		w := startWatcher(t)
		waitCaughtUp(t, w)

		spoof := "envd-spoof-" + strconv.FormatInt(time.Now().UnixNano(), 10)
		_, err = kmsg.WriteString("<3>Out of memory: Killed process 1 (" + spoof + ") total-vm:1kB, anon-rss:1kB\n")
		require.NoError(t, err)

		assert.Never(t, func() bool { return hasKill(w, spoof) }, 2*time.Second, 50*time.Millisecond,
			"a kill written from userspace was reported")
	})
}

func startWatcher(t *testing.T) *OOMWatcher {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	l := zerolog.Nop()
	w := NewOOMWatcher(&l)
	go w.Watch(ctx)

	return w
}

func waitCaughtUp(t *testing.T, w *OOMWatcher) {
	t.Helper()

	require.Eventually(t, func() bool {
		_, ok := w.Kills()

		return ok
	}, 10*time.Second, 50*time.Millisecond, "the watcher never read the whole log")
}

func hasKill(w *OOMWatcher, process string) bool {
	kills, _ := w.Kills()

	return slices.ContainsFunc(kills, func(k OOMKill) bool { return k.Process == process })
}
