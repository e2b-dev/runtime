//go:build linux

// resume-build resumes or cold-boots a template build locally; see resumebuild.Main.
package main

import "github.com/e2b-dev/infra/packages/orchestrator/cmd/resume-build/resumebuild"

func main() {
	resumebuild.Main()
}
