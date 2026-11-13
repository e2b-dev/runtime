//go:build !linux

package memlimit

import "context"

func watchMemoryEvents(context.Context, func()) error { return errUnsupported }
