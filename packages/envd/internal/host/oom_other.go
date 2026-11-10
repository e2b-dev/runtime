//go:build !linux

package host

import (
	"context"

	"github.com/rs/zerolog"
)

// OOMWatcher reports out-of-memory kills from the Linux kernel log. Other
// guests have no such log, so nothing is watched.
type OOMWatcher struct{}

// NewOOMWatcher returns nil: there is no kernel log to read, and a nil
// watcher is the "not watched" state the API already handles.
func NewOOMWatcher(_ *zerolog.Logger) *OOMWatcher {
	return nil
}

// Watch returns at once; there are no kills to collect.
func (*OOMWatcher) Watch(_ context.Context) {}

// Kills reports that no kills are known.
func (*OOMWatcher) Kills() ([]OOMKill, bool) {
	return nil, false
}
