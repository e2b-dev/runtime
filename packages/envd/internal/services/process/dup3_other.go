//go:build unix && !linux

package process

import "errors"

// dup3 is unsupported off Linux. The live upgrade only runs in the (Linux)
// guest; this stub keeps the package compiling on non-Linux dev machines.
func dup3(_, _, _ int) error {
	return errors.New("dup3 is only supported on linux")
}
