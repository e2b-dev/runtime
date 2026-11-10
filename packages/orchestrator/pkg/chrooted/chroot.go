//go:build linux

// Package chrooted confines filesystem operations to one directory tree, the
// way a process chrooted into that directory is confined: paths are
// interpreted relative to the tree's root, ".." cannot climb above it, and
// symbolic links, absolute ones included, resolve inside it.
//
// The kernel enforces the confinement. Every path is resolved by openat2(2)
// with RESOLVE_IN_ROOT, which treats the root directory's descriptor as "/"
// for the whole resolution, exactly as a chroot would, and RESOLVE_NO_MAGICLINKS,
// which refuses procfs magic links. The operation then acts on the descriptor
// that resolution produced. Nothing is pinned to an OS thread and no mount
// namespace is created, so a confined tree costs one file descriptor and
// operations on it run concurrently.
package chrooted

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// Chrooted is a filesystem confined to ActualRoot. It is safe for concurrent
// use.
type Chrooted struct {
	ActualRoot string
	Metadata   map[string]string

	// closed is set first by Close so that new operations fail fast even
	// while Close still waits for the ones in flight.
	closed atomic.Bool
	// mu guards rootFD against Close: readers hold it for the duration of an
	// operation so the descriptor cannot be closed, and its number reused by
	// an unrelated open, while an operation still resolves against it.
	mu     sync.RWMutex
	rootFD int

	// closeDone is published after the original root descriptor close finishes.
	// Waiters never initiate or retry that close.
	closeDone chan struct{}
	closeErr  error
}

type Option func(*Chrooted)

func WithMetadata(key, value string) Option {
	return func(fs *Chrooted) {
		if fs.Metadata == nil {
			fs.Metadata = make(map[string]string)
		}

		fs.Metadata[key] = value
	}
}

var (
	// ErrClosed is returned by operations on a Chrooted after Close.
	ErrClosed = errors.New("chrooted filesystem is closed")

	// ErrCloseTimeout is returned by Close when operations are still in
	// flight after closeDrainTimeout. The tree is closed to new operations;
	// its descriptor is released once the last one finishes.
	ErrCloseTimeout = errors.New("chrooted filesystem still has operations in flight")

	// ErrOpenat2Unsupported is returned when the kernel, or a seccomp
	// filter, does not provide openat2(2) with RESOLVE_IN_ROOT (Linux 5.6).
	ErrOpenat2Unsupported = errors.New("kernel does not support openat2 with RESOLVE_IN_ROOT")
)

// closeDrainTimeout bounds how long Close waits for operations in flight. A
// volume on a hard NFS mount can stall a call indefinitely; Close must not
// stall its caller with it.
const closeDrainTimeout = 30 * time.Second

var (
	supportOnce sync.Once
	errSupport  error
)

// CheckSupport reports whether this host can confine paths. It probes
// openat2 once; the orchestrator calls it at startup so an unsupported host
// fails at deploy rather than on the first volume request.
func CheckSupport() error {
	supportOnce.Do(func() {
		rootFD, err := retryEINTR(func() (int, error) {
			return unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		})
		if err != nil {
			errSupport = &os.PathError{Op: "open", Path: "/", Err: err}

			return
		}
		defer unix.Close(rootFD)

		probe, err := openat2(rootFD, "/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		if err != nil {
			errSupport = fmt.Errorf("%w: %w", ErrOpenat2Unsupported, err)

			return
		}
		_ = unix.Close(probe)
	})

	return errSupport
}

// Chroot confines a filesystem to the directory at source.
func Chroot(source string, opts ...Option) (*Chrooted, error) {
	if err := CheckSupport(); err != nil {
		return nil, err
	}

	rootFD, err := retryEINTR(func() (int, error) {
		return unix.Open(source, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	})
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: source, Err: err}
	}

	fs := &Chrooted{
		ActualRoot: source,
		rootFD:     rootFD,
		closeDone:  make(chan struct{}),
	}

	for _, opt := range opts {
		opt(fs)
	}

	return fs, nil
}

// acquire pins the root descriptor for one operation. The caller must call
// the returned release when done.
func (fs *Chrooted) acquire() (release func(), err error) {
	// Checked before taking the lock so that a pending Close, which blocks
	// new readers, does not park callers that should fail fast.
	if fs.closed.Load() {
		return nil, ErrClosed
	}

	fs.mu.RLock()
	// Checked again under the lock: a Close that got the write lock in
	// between has already released the descriptor.
	if fs.closed.Load() {
		fs.mu.RUnlock()

		return nil, ErrClosed
	}

	return fs.mu.RUnlock, nil
}

// Close stops accepting operations at once and releases the root directory
// once the operations in flight have finished. A second Close returns
// ErrClosed. If the drain takes longer than closeDrainTimeout, Close returns
// ErrCloseTimeout and the descriptor is released when the last operation
// completes.
func (fs *Chrooted) Close() error {
	return fs.closeWithTimeout(closeDrainTimeout)
}

// CloseAsync stops admission and starts the original descriptor close without
// waiting for in-flight operations. WaitClosed joins that close and its result.
func (fs *Chrooted) CloseAsync() error {
	if !fs.closed.CompareAndSwap(false, true) {
		return fmt.Errorf("%w: %q has already been closed", ErrClosed, fs.ActualRoot)
	}

	if fs.mu.TryLock() {
		result := fs.closeResult(unix.Close(fs.rootFD))
		fs.mu.Unlock()

		return result
	}

	go func() {
		fs.mu.Lock()
		defer fs.mu.Unlock()

		fs.closeResult(unix.Close(fs.rootFD))
	}()

	return nil
}

func (fs *Chrooted) closeWithTimeout(drainTimeout time.Duration) error {
	if err := fs.CloseAsync(); err != nil {
		return err
	}
	select {
	case <-fs.closeDone:
		return fs.closeErr
	default:
	}

	timeout := time.NewTimer(drainTimeout)
	defer timeout.Stop()

	select {
	case <-fs.closeDone:
		return fs.closeErr
	case <-timeout.C:
		return fmt.Errorf("%w: %q", ErrCloseTimeout, fs.ActualRoot)
	}
}

// WaitClosed waits for the original root descriptor close result. It does not
// initiate Close; cancellation only stops this caller's wait. A Close that
// returned ErrCloseTimeout may finish later, while a second Close still
// returns ErrClosed. This covers the root descriptor only, not open files,
// guest state, network slots, or durable volume deletion.
func (fs *Chrooted) WaitClosed(ctx context.Context) error { //nolint:contextcheck // nil is explicitly supported; non-nil callers keep their original context
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case <-fs.closeDone:
		return fs.closeErr
	default:
	}
	select {
	case <-fs.closeDone:
		return fs.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (fs *Chrooted) closeResult(err error) error {
	if err != nil {
		err = fmt.Errorf("failed to close root %q: %w", fs.ActualRoot, err)
	}
	fs.closeErr = err
	close(fs.closeDone)

	return err
}
