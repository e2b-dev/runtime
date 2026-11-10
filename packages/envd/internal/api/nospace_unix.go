//go:build unix

package api

import (
	"errors"
	"syscall"
)

func isNoSpace(err error) bool {
	return errors.Is(err, syscall.ENOSPC)
}
