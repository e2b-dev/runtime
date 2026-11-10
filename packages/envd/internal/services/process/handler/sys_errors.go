package handler

import "errors"

// errCancelledBeforeRun reports a child that a kill or the deadline reached
// before it was contained and resumed; it never ran.
var errCancelledBeforeRun = errors.New("the process was cancelled before it ran")

// unsupportedCommandError reports a request for a command this system cannot
// run. It is the caller's request that cannot run, not an envd failure.
type unsupportedCommandError string

func (e unsupportedCommandError) Error() string { return string(e) }
