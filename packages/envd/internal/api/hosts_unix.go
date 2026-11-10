//go:build unix

package api

// hostsFilePath is the file the events.e2b.local entry is written to.
func hostsFilePath() string { return "/etc/hosts" }
