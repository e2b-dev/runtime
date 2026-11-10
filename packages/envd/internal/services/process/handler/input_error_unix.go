//go:build unix

package handler

// isClosedPipe: a write to a pipe whose reader is gone is EPIPE here, which
// InputErrorCode already maps.
func isClosedPipe(error) bool { return false }
