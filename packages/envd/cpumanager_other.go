//go:build !linux

package main

import (
	"github.com/rs/zerolog"

	"github.com/e2b-dev/infra/packages/envd/internal/reaper"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cpus"
)

// newCPUManager returns the no-op manager: the guest has no sysfs to online CPUs through.
func newCPUManager(_ zerolog.Logger, _ *reaper.Registry) cpus.Manager {
	return cpus.NewNoopManager()
}
