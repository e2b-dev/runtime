package sandboxlogs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/logs"
)

func TestOrderSQL(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "ASC", orderSQL(SortOrderForward))
	assert.Equal(t, "DESC", orderSQL(SortOrderBackward))
	// Unknown values default to ascending.
	assert.Equal(t, "ASC", orderSQL(SortOrder(42)))
}

func TestAtLeastLevels(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"error"}, atLeastLevels(logs.LevelError))
	assert.Equal(t, []string{"warn", "error"}, atLeastLevels(logs.LevelWarn))
	assert.Equal(t, []string{"", "info", "warn", "error"}, atLeastLevels(logs.LevelInfo))
	assert.Equal(t, []string{"", "debug", "info", "warn", "error"}, atLeastLevels(logs.LevelDebug))
}

func TestRowToLogEntry(t *testing.T) {
	t.Parallel()

	ts := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	r := row{
		Timestamp: ts,
		Level:     "warn",
		Message:   "hello",
		Raw:       `{"raw":"line"}`,
		Fields:    `{"key":"value","n":"2"}`,
	}

	var parseErr error
	entry := r.toLogEntry(func(err error) { parseErr = err })

	require.NoError(t, parseErr)
	assert.Equal(t, ts, entry.Timestamp)
	assert.JSONEq(t, `{"raw":"line"}`, entry.Raw)
	assert.Equal(t, logs.LevelWarn, entry.Level)
	assert.Equal(t, "hello", entry.Message)
	assert.Equal(t, map[string]string{"key": "value", "n": "2"}, entry.Fields)
}

func TestRowToLogEntryEmptyFields(t *testing.T) {
	t.Parallel()

	r := row{Message: "m", Fields: ""}

	called := false
	entry := r.toLogEntry(func(error) { called = true })

	assert.False(t, called, "empty fields must not trigger a parse error")
	assert.Equal(t, map[string]string{}, entry.Fields)
}

func TestRowToLogEntryInvalidFieldsPreservesRaw(t *testing.T) {
	t.Parallel()

	r := row{
		Raw:    "original-raw",
		Fields: `{"broken": `,
	}

	var gotErr error
	entry := r.toLogEntry(func(err error) { gotErr = err })

	require.Error(t, gotErr)
	// Malformed fields fall back to an empty map but raw is preserved.
	assert.Equal(t, map[string]string{}, entry.Fields)
	assert.Equal(t, "original-raw", entry.Raw)
}

func TestRowToLogEntryNilCallbackDoesNotPanic(t *testing.T) {
	t.Parallel()

	r := row{Fields: `{"broken": `}

	assert.NotPanics(t, func() {
		entry := r.toLogEntry(nil)
		assert.Equal(t, map[string]string{}, entry.Fields)
	})
}
