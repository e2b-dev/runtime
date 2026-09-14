package auth

import (
	"errors"
	"fmt"

	"github.com/getkin/kin-openapi/openapi3filter"
)

// Prefixes carried by the errors ProcessSecurityErrors returns; error
// handlers match them to choose the response status.
const (
	SecurityErrPrefix  = "error in openapi3filter.SecurityRequirementsError: security requirements failed: "
	ForbiddenErrPrefix = "team forbidden: "
	BlockedErrPrefix   = "team blocked: "
)

// ProcessSecurityErrors picks the one client-facing error out of a failed
// security-requirements validation. A forbidden or blocked team verdict wins
// wherever it sits in the group list — that team authenticated, and its state
// is the answer. Otherwise the first scheme the caller actually attempted
// wins; an error matching ErrNoAuthHeader means the scheme's header was never
// sent. With nothing attempted, the first group's error stands.
func ProcessSecurityErrors(e *openapi3filter.SecurityRequirementsError) error {
	errs := e.Errors
	if len(errs) == 0 {
		return errors.New(SecurityErrPrefix + "authentication failed")
	}

	for _, err := range errs {
		if teamForbidden, ok := errors.AsType[*TeamForbiddenError](err); ok {
			return fmt.Errorf("%s%s", ForbiddenErrPrefix, teamForbidden.Error())
		}

		if teamBlocked, ok := errors.AsType[*TeamBlockedError](err); ok {
			return fmt.Errorf("%s%s", BlockedErrPrefix, teamBlocked.Error())
		}
	}

	selected := errs[0]
	for _, err := range errs {
		if !errors.Is(err, ErrNoAuthHeader) {
			selected = err

			break
		}
	}

	return fmt.Errorf("%s%s", SecurityErrPrefix, selected.Error())
}
