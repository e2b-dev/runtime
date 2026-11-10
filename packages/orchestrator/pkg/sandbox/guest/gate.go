package guest

import (
	"context"
	"fmt"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Flags is the part of the feature-flag client that the gate reads.
type Flags interface {
	BoolFlag(ctx context.Context, flag featureflags.BoolFlag, contexts ...ldcontext.Context) bool
}

// NotEnabledError refuses a build or a sandbox of a non-default guest on a
// node whose featureflags.GuestProfilesFlag is off.
type NotEnabledError struct {
	// Label is the label of the refused profile.
	Label string
}

func (e *NotEnabledError) Error() string {
	return fmt.Sprintf("the %s guest profile is not enabled on this node: the %s flag is off", e.Label, featureflags.GuestProfilesFlag.Key())
}

// Serve returns p when it may serve a build or a sandbox start. The default
// profile always may, and no flag is read for it. Any other profile may only
// while featureflags.GuestProfilesFlag is on; the flag is read on every call,
// so it is a runtime switch, and a nil flags client reads as off.
func Serve(ctx context.Context, flags Flags, p Profile) (Profile, error) {
	if p.Name() == DefaultName {
		return p, nil
	}
	if utils.IsNil(flags) || !flags.BoolFlag(ctx, featureflags.GuestProfilesFlag) {
		return nil, &NotEnabledError{Label: p.Label()}
	}

	return p, nil
}
