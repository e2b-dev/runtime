package retry

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

// IsRetriable determines if an error is retriable.
// Returns true for transient errors that may succeed on retry.
func IsRetriable(err error) bool {
	if err == nil {
		return false
	}

	// Never retry context errors
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Check for PostgreSQL errors
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return isRetriablePgError(pgErr)
	}

	// Check for connection errors
	if _, ok := errors.AsType[*pgconn.ConnectError](err); ok {
		return true
	}

	// Check for network errors
	if _, ok := errors.AsType[net.Error](err); ok {
		return true
	}

	// Check for specific syscall errors
	if errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Check for connection closed errors by message
	errMsg := err.Error()
	if strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "connection reset") ||
		strings.Contains(errMsg, "broken pipe") ||
		strings.Contains(errMsg, "connection is closed") ||
		strings.Contains(errMsg, "closed network connection") ||
		strings.Contains(errMsg, "connection timed out") {
		return true
	}

	return false
}

// isRetriablePgError checks if a PostgreSQL error is retriable based on its code.
func isRetriablePgError(pgErr *pgconn.PgError) bool {
	code := pgErr.Code

	// Check error class (first two characters)
	switch {
	case pgerrcode.IsConnectionException(code): // Connection exceptions
		return true
	case pgerrcode.IsOperatorIntervention(code):
		return true
	}

	// Check specific error codes
	if code == pgerrcode.TooManyConnections {
		return true
	}

	return false
}
