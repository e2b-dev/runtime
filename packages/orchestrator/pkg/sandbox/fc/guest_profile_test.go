//go:build linux

package fc

import (
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
)

// foreignProfile is a guest profile other than the default one.
type foreignProfile struct {
	guest.Profile
}

func (foreignProfile) Name() string  { return "foreign" }
func (foreignProfile) Label() string { return "foreign" }
