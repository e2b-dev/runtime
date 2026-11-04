package resumebuild

import "errors"

// skipEnvdWaitConflicts refuses -skip-envd-wait with the modes that need envd:
// a command, a shell and every pause run through the guest's envd, so on a
// guest whose envd was never verified they would fail late or persist a
// snapshot of an unverified guest.
func skipEnvdWaitConflicts(skipEnvdWait, cmdMode, pauseMode, shell bool) error {
	if skipEnvdWait && (cmdMode || pauseMode || shell) {
		return errors.New("-skip-envd-wait leaves envd unverified: it is incompatible with -cmd, -shell and the pause flags (-pause, -signal-pause, -cmd-pause, -cmd-signal-pause)")
	}

	return nil
}
