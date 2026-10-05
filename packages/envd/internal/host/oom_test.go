package host

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestOOMWatcher(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	l := zerolog.New(&logs)
	w := NewOOMWatcher(&l)
	add := w.add

	log := &fakeKernelLog{reads: []read{
		{record: "6,1,100,-;Linux version 6.1.102"},
		{record: "3,2,200,-;Memory cgroup out of memory: Killed process 1414 (uvicorn) total-vm:599196kB, anon-rss:426012kB, file-rss:0kB, shmem-rss:0kB, UID:0 pgtables:1032kB oom_score_adj:100\n SUBSYSTEM=memory\n"},
		{record: "6,3,300,-;oom_reaper: reaped process 1414 (uvicorn), now anon-rss:0kB, file-rss:0kB, shmem-rss:0kB"},
		{err: unix.EPIPE},
		{record: "3,9,900,-;Out of memory: Killed process 2006 (a) total-vm:1) total-vm:2103944kB, anon-rss:1904312kB"},
	}}
	log.beforeCaughtUp = func() {
		_, ok := w.Kills()
		assert.False(t, ok, "kills aren't reported before the whole log is read")
	}
	err := followKernelLog(t.Context(), log, add, w.markCaughtUp)
	require.ErrorIs(t, err, errLogDone)

	want := []OOMKill{{Seq: 2, Process: "uvicorn"}, {Seq: 9, Process: "a) total-vm:1"}}
	kills, ok := w.Kills()
	require.True(t, ok, "kills are reported once the whole log is read")
	assert.Equal(t, want, kills, "only kill records count, and a name ends where the size fields begin")

	log.reads = []read{
		{record: "12,10,1000,-;Out of memory: Killed process 1 (spoofed) total-vm:1024kB, anon-rss:512kB"},
		{record: "6,12,1150,-;Killed process [3002]: segfault at 0 ip 0000000000401136 sp 00007ffd error 6"},
		{record: "3,11,1100,-;Out of memory: Killed process 3001 [garbled]"},
		{record: "3,13,1300,-;Out of memory (oom_kill_allocating_task): Killed process 88 (java) total-vm:1024kB, anon-rss:512kB"},
		{record: `3,14,1400,-;EXT4-fs (vda): Unrecognized mount option "x of memory: Killed process 1 (fake) total-vm:1kB" or missing value`},
		{err: io.ErrClosedPipe},
	}
	log.beforeCaughtUp = nil
	err = followKernelLog(t.Context(), log, add, w.markCaughtUp)
	require.ErrorIs(t, err, io.ErrClosedPipe, "a read error ends the watch instead of being retried forever")

	want = append(want, OOMKill{Seq: 11, Process: unknownProcess}, OOMKill{Seq: 13, Process: "java"})
	kills, _ = w.Kills()
	assert.Equal(t, want, kills, "only a kernel record starting with an OOM message is a kill, and a kill without a readable name still is")
	assert.Contains(t, logs.String(), "Failed to find the process name of an out-of-memory kill")
	assert.Equal(t, 1, strings.Count(logs.String(), "\n"), "only the unnamed kill logs an error")

	w.markStopped()
	add([]byte("3,11,1100,-;Out of memory: Killed process 3001 [garbled]"))
	assert.Equal(t, 1, strings.Count(logs.String(), "\n"), "reading the log again doesn't repeat the error")
	w.markCaughtUp()
	kills, _ = w.Kills()
	assert.Equal(t, []OOMKill{{Seq: 11, Process: unknownProcess}}, kills, "a reopened log rebuilds the list")
	want = kills

	kills, _ = w.Kills()
	assert.Equal(t, want, kills, "reading the kills doesn't consume them")

	for i := range maxOOMKills {
		add(fmt.Appendf(nil, "3,%d,0,-;Out of memory: Killed process 1 (p) total-vm:1kB", 100+i))
	}
	kills, _ = w.Kills()
	assert.Len(t, kills, maxOOMKills)
	assert.Equal(t, int64(100), kills[0].Seq, "the oldest kills are dropped first")
}

var errLogDone = errors.New("no more records")

type read struct {
	record string
	err    error
}

// fakeKernelLog returns its reads, then EAGAIN; wait stops once they run out.
type fakeKernelLog struct {
	reads          []read
	beforeCaughtUp func()
}

func (f *fakeKernelLog) Read(p []byte) (int, error) {
	if len(f.reads) == 0 {
		if f.beforeCaughtUp != nil {
			f.beforeCaughtUp()
		}

		return 0, unix.EAGAIN
	}

	r := f.reads[0]
	f.reads = f.reads[1:]

	return copy(p, r.record), r.err
}

func (f *fakeKernelLog) wait(context.Context) error {
	if len(f.reads) == 0 {
		return errLogDone
	}

	return nil
}
