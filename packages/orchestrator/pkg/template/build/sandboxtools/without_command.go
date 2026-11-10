package sandboxtools

import (
	"errors"
	"strings"
)

// withoutCommand reports err with the command, which is the user's and may
// carry credentials, masked wherever envd repeated it: envd names the whole
// command line when a process cannot start, whether it refuses the request
// or reports on the stream.
func withoutCommand(err error, command string) error {
	if command == "" || !strings.Contains(err.Error(), command) {
		return err
	}

	return errors.New(strings.ReplaceAll(err.Error(), command, "<command>"))
}
