//go:build !linux

package host

import "errors"

// HotplugAdded is unknown on guests without Linux memory hotplug.
func HotplugAdded() (uint64, error) {
	return 0, errors.New("/proc/iomem is Linux only")
}
