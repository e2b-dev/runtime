//go:build linux

package host

import "os"

const iomemFile = "/proc/iomem"

// HotplugAdded is how much memory virtio-mem added to the guest, in bytes, from /proc/iomem.
func HotplugAdded() (uint64, error) {
	data, err := os.ReadFile(iomemFile)
	if err != nil {
		return 0, err
	}

	return parseHotplugAdded(data)
}
