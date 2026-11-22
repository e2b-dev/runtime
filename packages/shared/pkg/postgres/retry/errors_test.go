package retry

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestIsRetriable_NilError(t *testing.T) {
	t.Parallel()
	assert.False(t, IsRetriable(nil))
}

func TestIsRetriable_ContextErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
	}{
		{"context canceled", context.Canceled},
		{"context deadline exceeded", context.DeadlineExceeded},
		{"wrapped context canceled", errors.Join(errors.New("wrapped"), context.Canceled)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.False(t, IsRetriable(tt.err))
		})
	}
}

func TestIsRetriable_PostgreSQLErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		code      string
		retriable bool
	}{
		// Connection errors (Class 08) - should retry
		{"connection exception", "08000", true},
		{"connection failure", "08006", true},
		{"sqlclient unable to establish connection", "08001", true},

		// Transaction rollback (Class 40) - should NOT retry (handled at application level)
		{"serialization failure", "40001", false},
		{"deadlock detected", "40P01", false},
		{"transaction rollback", "40000", false},

		// Operator intervention (Class 57) - should retry
		{"admin shutdown", "57P01", true},
		{"crash shutdown", "57P02", true},
		{"cannot connect now", "57P03", true},

		// Insufficient resources - specific codes
		{"too many connections", "53300", true},

		// Constraint violations (Class 23) - should NOT retry
		{"unique violation", "23505", false},
		{"foreign key violation", "23503", false},
		{"not null violation", "23502", false},
		{"check violation", "23514", false},

		// Syntax errors (Class 42) - should NOT retry
		{"syntax error", "42601", false},
		{"undefined table", "42P01", false},
		{"undefined column", "42703", false},

		// Data exceptions (Class 22) - should NOT retry
		{"division by zero", "22012", false},
		{"numeric value out of range", "22003", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := &pgconn.PgError{Code: tt.code}
			assert.Equal(t, tt.retriable, IsRetriable(err), "expected IsRetriable(%s) = %v", tt.code, tt.retriable)
		})
	}
}

func TestIsRetriable_NetworkErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		err       error
		retriable bool
	}{
		{"connection reset", syscall.ECONNRESET, true},
		{"connection refused", syscall.ECONNREFUSED, true},
		{"broken pipe", syscall.EPIPE, true},
		{"EOF", io.EOF, true},
		{"unexpected EOF", io.ErrUnexpectedEOF, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.retriable, IsRetriable(tt.err))
		})
	}
}

func TestIsRetriable_ConnectError(t *testing.T) {
	t.Parallel()
	// pgconn.ConnectError wraps connection failures
	// We can't construct it directly since err is unexported,
	// but we can test that our error message matching catches it
	err := errors.New("failed to connect to `host=localhost`: connection refused")
	assert.True(t, IsRetriable(err))
}

func TestIsRetriable_NetError(t *testing.T) {
	t.Parallel()
	// Create a mock net.Error
	err := &net.OpError{
		Op:  "dial",
		Net: "tcp",
		Err: errors.New("connection refused"),
	}
	assert.True(t, IsRetriable(err))
}

func TestIsRetriable_ErrorMessages(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		msg       string
		retriable bool
	}{
		{"connection refused message", "dial tcp: connection refused", true},
		{"connection reset message", "read: connection reset by peer", true},
		{"broken pipe message", "write: broken pipe", true},
		{"connection closed", "connection is closed", true},
		{"closed network connection", "use of closed network connection", true},
		{"connection timed out", "connection timed out", true},
		{"generic error", "some other error", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := errors.New(tt.msg)
			assert.Equal(t, tt.retriable, IsRetriable(err))
		})
	}
}

func TestIsRetriable_WrappedErrors(t *testing.T) {
	t.Parallel()
	// Wrapped PostgreSQL error (connection error - retriable)
	pgErr := &pgconn.PgError{Code: "08006"} // connection failure
	wrappedPgErr := errors.Join(errors.New("query failed"), pgErr)
	assert.True(t, IsRetriable(wrappedPgErr))

	// Wrapped syscall error
	wrappedSyscall := errors.Join(errors.New("network error"), syscall.ECONNRESET)
	assert.True(t, IsRetriable(wrappedSyscall))
}
