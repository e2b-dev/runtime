// Package reexec lets a package run part of itself in a child process. A helper is
// defined under a name; a binary started with that name as its first argument runs the
// helper instead of its usual main. Names never start with "-", so they coexist with the
// flags.
package reexec

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// helperEnv marks a child as started by Exec. Main then refuses to fall through to the
// usual main, so a child that cannot find its helper exits instead of running the program
// again, which would run Exec again.
const helperEnv = "E2B_REEXEC_HELPER"

var helpers = make(map[string]func(args []string) error)

type Command struct {
	name string
}

// Define registers run under name. It is meant for package-level variables; a repeated
// or flag-like name is a programming error and panics at init.
func Define(name string, run func(args []string) error) Command {
	if name == "" || strings.HasPrefix(name, "-") {
		panic(fmt.Sprintf("reexec: helper name %q would be read as a flag", name))
	}
	if _, dup := helpers[name]; dup {
		panic(fmt.Sprintf("reexec: helper %q defined twice", name))
	}
	helpers[name] = run

	return Command{name: name}
}

// Exec is the command that runs the helper in a child process with args. It execs
// /proc/self/exe, which names this binary even after a live upgrade unlinked the file it
// was started from.
func (c Command) Exec(args ...string) *exec.Cmd {
	cmd := exec.Command("/proc/self/exe", append([]string{c.name}, args...)...) //nolint:noctx // the caller owns the child through Wait; a context would only add a SIGKILL, which a helper blocked in the kernel ignores
	cmd.Env = append(os.Environ(), helperEnv+"="+c.name)

	return cmd
}

// Main runs the helper argv[1] names, if any, and exits with its outcome. It returns
// only when the binary was not started as a helper, and belongs first in main.
func Main() {
	var run func([]string) error
	if len(os.Args) > 1 {
		run = helpers[os.Args[1]]
	}
	if run == nil {
		if name, marked := os.LookupEnv(helperEnv); marked {
			fmt.Fprintf(os.Stderr, "reexec: started as helper %q but no such helper is defined\n", name)
			os.Exit(1)
		}

		return
	}
	if err := run(os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
