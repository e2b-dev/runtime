package redis

import redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"

// keyForm, formTeam, formSandbox, other and recordKey stand in for the key
// forms a later change introduces, so the remove-script and two-lock tests
// can name two keys of one sandbox. The second key only has to differ from
// the first; it is not the layout the later change picks.
type keyForm uint8

const (
	formTeam    keyForm = 1
	formSandbox keyForm = 2
)

func (f keyForm) other() keyForm {
	if f == formSandbox {
		return formTeam
	}

	return formSandbox
}

func recordKey(form keyForm, teamID, sandboxID string) string {
	if form == formSandbox {
		return redis_utils.CreateKey(GetTeamPrefix(teamID), "record", sandboxID)
	}

	return getSandboxKey(teamID, sandboxID)
}
