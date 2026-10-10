package handler

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"syscall"

	"connectrpc.com/connect"
)

// Sentinel errors returned by the stdin/pty input methods for expected process
// lifecycle states — as opposed to genuine I/O failures. They let the service
// layer map an input failure to a precise Connect code instead of collapsing
// everything to CodeInternal (see issue #3622).
var (
	// ErrStdinUnavailable means the process cannot accept stdin right now: it was
	// started with stdin disabled, or stdin has already been closed. This is an
	// expected precondition failure, not an envd fault.
	ErrStdinUnavailable = errors.New("stdin not enabled or closed")

	// ErrStdinOnPty means stdin was written to a PTY-backed process; input must
	// go to the pty instead. An expected precondition failure.
	ErrStdinOnPty = errors.New("tty assigned to process — input should be written to the pty, not the stdin")

	// ErrTtyUnavailable means a pty write targeted a process that has no tty.
	ErrTtyUnavailable = errors.New("tty not assigned to process — input should be written to the stdin, not the tty")

	// ErrCloseStdinOnPty means CloseStdin was called on a PTY-backed process.
	ErrCloseStdinOnPty = errors.New("cannot close stdin for PTY process — send Ctrl+D (0x04) instead")
)

// InputErrorCode maps a WriteStdin/WriteTty/CloseStdin failure to the Connect
// code the client should observe.
//
//   - Expected process-lifecycle states (stdin disabled/closed, wrong pipe for
//     the process type, or the child's read end already gone after exit) are
//     CodeFailedPrecondition: the request was well-formed but the process is not
//     in a state to accept it.
//   - Everything else is a real underlying I/O failure and stays CodeInternal.
//
// CodeInternal is thus reserved for genuine invariant failures, so a client can
// distinguish a normal process-state transition from an unexpected envd fault.
// The process's EndEvent remains the authoritative exit signal; an input error
// alone must not be treated as the terminal process result.
func InputErrorCode(err error) connect.Code {
	switch {
	// Explicit precondition sentinels from the input methods.
	case errors.Is(err, ErrStdinUnavailable),
		errors.Is(err, ErrStdinOnPty),
		errors.Is(err, ErrTtyUnavailable),
		errors.Is(err, ErrCloseStdinOnPty):
		return connect.CodeFailedPrecondition
	// The process exited and its stdin/pty read end is gone: the write end sees
	// EPIPE, or Go's file wrapper reports it was already closed (os.ErrClosed /
	// io.ErrClosedPipe). All are expected once the process is no longer running.
	case errors.Is(err, syscall.EPIPE),
		errors.Is(err, os.ErrClosed),
		errors.Is(err, io.ErrClosedPipe),
		errors.Is(err, fs.ErrClosed):
		return connect.CodeFailedPrecondition
	default:
		return connect.CodeInternal
	}
}
