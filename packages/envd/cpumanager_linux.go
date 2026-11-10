//go:build linux

package main

import (
	"github.com/rs/zerolog"

	"github.com/e2b-dev/infra/packages/envd/internal/reaper"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cpus"
)

// newCPUManager onlines guest CPUs through sysfs, which only a Linux guest has.
func newCPUManager(logger zerolog.Logger, helpers *reaper.Registry) cpus.Manager {
	return cpus.New(logger, helpers)
}
