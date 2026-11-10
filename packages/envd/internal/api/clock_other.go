//go:build unix && !linux

package api

import (
	"errors"
	"time"
)

func setSystemTime(_ time.Time) error {
	return errors.New("setting system time is only supported on Linux")
}
