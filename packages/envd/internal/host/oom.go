package host

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"
)

const (
	kmsgPath = "/dev/kmsg"

	// Cheap filter; a kill must also start with one of oomKillPrefixes.
	oomKillMarker = "Killed process "

	// Used when a kill record has no readable process name.
	unknownProcess = "Unknown"

	// How often to try opening the kernel log.
	kmsgOpenAttempts   = 5
	kmsgOpenRetryDelay = time.Second

	// Read failures in a row before the watcher gives up; a read that lasted kmsgHealthyRead
	// resets the count.
	kmsgMaxReadFailures = 5
	kmsgHealthyRead     = time.Minute

	// How long a watcher that gave up waits before it tries again.
	kmsgRecoveryDelay = 30 * time.Minute

	// Kills kept; the oldest are dropped first.
	maxOOMKills = 32

	// Largest kernel log record.
	kmsgRecordMax = 8192

	// How often a wait checks whether to stop.
	kmsgPollTimeoutMs = 1000
)

// OOMKill is a process the guest kernel killed for running out of memory.
type OOMKill struct {
	Seq     int64  `json:"seq"`     // Kernel log sequence number
	Process string `json:"process"` // Name of the killed process
}

// OOMWatcher keeps the latest OOM kills since the guest booted, read from the kernel log.
type OOMWatcher struct {
	logger *zerolog.Logger

	mu       sync.Mutex
	kills    []OOMKill
	caughtUp bool
	// Newest kill whose unreadable name was logged; a reopen reads older ones again.
	lastUnnamed int64
}

func NewOOMWatcher(l *zerolog.Logger) *OOMWatcher {
	return &OOMWatcher{logger: l}
}

// Watch collects kills until ctx is done. After a read error it reopens the log and reads it
// again from the start. If it gives up, it tries again every kmsgRecoveryDelay, so a watcher that
// failed doesn't stay off in every snapshot taken after it.
func (w *OOMWatcher) Watch(ctx context.Context) {
	gaveUp := false
	for {
		msg, quiet, err := w.watchRound(ctx, gaveUp)
		if ctx.Err() != nil {
			return
		}
		if !quiet {
			w.logger.Error().Err(err).Str("path", kmsgPath).Dur("retry_in", kmsgRecoveryDelay).Msg(msg)
		}
		gaveUp = true

		select {
		case <-ctx.Done():
			return
		case <-time.After(kmsgRecoveryDelay):
		}
	}
}

// watchRound reads the kernel log until it gives up, then returns why. After an earlier give-up it
// logs only a recovery, not each failure again, and returns quiet until a read lasts
// kmsgHealthyRead.
func (w *OOMWatcher) watchRound(ctx context.Context, recovering bool) (string, bool, error) {
	caughtUp := func() {
		// Once it says kills are reported again, a later failure is worth logging.
		if recovering {
			recovering = false
			w.logger.Info().Str("path", kmsgPath).Msg("Reading the kernel log again, out-of-memory kills are reported")
		}
		w.markCaughtUp()
	}

	failures := 0
	for {
		log, err := openKernelLog(ctx)
		if err != nil {
			return "Failed to open the kernel log, out-of-memory kills will not be reported", recovering, err
		}

		start := time.Now()
		err = followKernelLog(ctx, log, w.add, caughtUp)
		log.Close()
		// Later kills would be missed, so stop reporting the list until it's read again.
		w.markStopped()
		if time.Since(start) >= kmsgHealthyRead {
			failures = 0
			recovering = false
		}
		failures++

		switch {
		case ctx.Err() != nil:
			return "", recovering, ctx.Err()
		case failures >= kmsgMaxReadFailures:
			return "Stopped reading the kernel log, out-of-memory kills will not be reported", recovering, err
		}

		if !recovering {
			w.logger.Warn().Err(err).Str("path", kmsgPath).Msg("Failed to read the kernel log, reopening it")
		}
		select {
		case <-ctx.Done():
			return "", recovering, ctx.Err()
		case <-time.After(kmsgOpenRetryDelay):
		}
	}
}

// openKernelLog opens the kernel log, retrying a few times.
func openKernelLog(ctx context.Context) (*kmsg, error) {
	var err error
	for attempt := range kmsgOpenAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(kmsgOpenRetryDelay):
			}
		}

		var log *kmsg
		if log, err = openKmsg(kmsgPath); err == nil {
			return log, nil
		}
	}

	return nil, err
}

// Kills returns the latest kills, oldest first. It returns false until the whole log has been
// read, again after each reopen, and once the watcher stops.
func (w *OOMWatcher) Kills() ([]OOMKill, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.caughtUp {
		return nil, false
	}

	return append(make([]OOMKill, 0, len(w.kills)), w.kills...), true
}

func (w *OOMWatcher) markCaughtUp() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.caughtUp = true
}

// markStopped drops the list, which a reopened log rebuilds.
func (w *OOMWatcher) markStopped() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.caughtUp = false
	w.kills = nil
}

func (w *OOMWatcher) add(record []byte) {
	// Skip non-kill records cheaply.
	if !bytes.Contains(record, []byte(oomKillMarker)) {
		return
	}

	kill, ok, err := parseOOMKill(string(record))
	if !ok {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err != nil && kill.Seq > w.lastUnnamed {
		w.lastUnnamed = kill.Seq
		w.logger.Error().Err(err).Str("record", string(record)).Msg("Failed to find the process name of an out-of-memory kill")
	}

	w.kills = append(w.kills, kill)
	if len(w.kills) > maxOOMKills {
		w.kills = w.kills[len(w.kills)-maxOOMKills:]
	}
}

// kernelLog is /dev/kmsg: one record per read, EAGAIN when none are left.
type kernelLog interface {
	Read(p []byte) (int, error)
	// wait blocks until a record is readable or ctx is done.
	wait(ctx context.Context) error
}

// followKernelLog passes each record to record and calls caughtUp whenever the log is drained.
// It returns when a read fails or ctx is done. record must not keep the slice.
func followKernelLog(ctx context.Context, log kernelLog, record func([]byte), caughtUp func()) error {
	buf := make([]byte, kmsgRecordMax)
	for {
		n, err := log.Read(buf)
		switch {
		case err == nil:
			record(buf[:n])
		case errors.Is(err, unix.EAGAIN):
			caughtUp()
			if err := log.wait(ctx); err != nil {
				return err
			}
		case errors.Is(err, unix.EPIPE), errors.Is(err, unix.EINTR):
			// EPIPE: unread records were overwritten; keep reading.
		default:
			return err
		}
	}
}

// kmsg reads /dev/kmsg non-blocking, so the end of the log shows as EAGAIN.
type kmsg struct{ fd int }

func openKmsg(path string) (*kmsg, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}

	return &kmsg{fd: fd}, nil
}

func (k *kmsg) Read(p []byte) (int, error) {
	return unix.Read(k.fd, p)
}

func (k *kmsg) wait(ctx context.Context) error {
	fds := []unix.PollFd{{Fd: int32(k.fd), Events: unix.POLLIN}}
	for {
		n, err := unix.Poll(fds, kmsgPollTimeoutMs)
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err != nil && !errors.Is(err, unix.EINTR):
			return err
		case n > 0:
			return nil
		}
	}
}

func (k *kmsg) Close() error {
	return unix.Close(k.fd)
}

// parseOOMKill returns the kill an OOM kill record reports:
//
//	3,843,52404240,-;Out of memory: Killed process 475 (python3) total-vm:2443300kB, anon-rss:...
//
// Only kernel records (priority 0-7) count, so userspace can't fake a kill. A kill with no
// readable name is named unknownProcess and returned with an error.
func parseOOMKill(record string) (OOMKill, bool, error) {
	seq, msg, ok := parseKernelRecord(record)
	if !ok {
		return OOMKill{}, false, nil
	}

	msg, _, _ = strings.Cut(msg, "\n")
	victim, found := cutOOMKillPrefix(msg)
	if !found {
		return OOMKill{}, false, nil
	}

	name, err := parseVictimName(victim)
	if err != nil {
		return OOMKill{Seq: seq, Process: unknownProcess}, true, err
	}

	return OOMKill{Seq: seq, Process: name}, true, nil
}

// The kernel's OOM kill messages, as the start of the record; anchoring there stops text that
// userspace gets into other kernel messages from passing for a kill.
var oomKillPrefixes = []string{
	"Out of memory: ",
	"Out of memory (oom_kill_allocating_task): ",
	"Memory cgroup out of memory: ",
}

// cutOOMKillPrefix returns what follows "Killed process " in an OOM kill message.
func cutOOMKillPrefix(msg string) (string, bool) {
	for _, prefix := range oomKillPrefixes {
		if victim, found := strings.CutPrefix(msg, prefix+oomKillMarker); found {
			return victim, true
		}
	}

	return "", false
}

// parseVictimName reads the name from `475 (python3) total-vm:...`.
func parseVictimName(victim string) (string, error) {
	pid, victim, found := strings.Cut(victim, " (")
	if _, err := strconv.Atoi(pid); !found || err != nil {
		return "", errors.New("no process ID followed by a name")
	}

	// Names can contain ')', so cut at the size fields.
	end := strings.LastIndex(victim, ") total-vm:")
	if end < 0 {
		return "", errors.New("no end to the process name")
	}

	return victim[:end], nil
}

// parseKernelRecord returns the seq and message of a record the kernel wrote.
func parseKernelRecord(record string) (int64, string, bool) {
	prefix, msg, found := strings.Cut(record, ";")
	if !found {
		return 0, "", false
	}

	fields := strings.SplitN(prefix, ",", 3)
	if len(fields) < 3 {
		return 0, "", false
	}

	if p, err := strconv.Atoi(fields[0]); err != nil || p < 0 || p > 7 {
		return 0, "", false
	}

	seq, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, "", false
	}

	return seq, msg, true
}
