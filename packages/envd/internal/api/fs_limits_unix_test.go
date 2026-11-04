//go:build unix

package api

import "testing"

func skipUnlessDirModeBlocksCreate(*testing.T) {}

func skipUnlessFileNameAllowed(*testing.T, string) {}
