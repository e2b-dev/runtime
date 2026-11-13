//go:build linux

package host

import (
	"bufio"
	"bytes"
	"os"
	"strconv"
	"strings"
)

const (
	onlinePolicyFile = "/sys/module/memory_hotplug/parameters/online_policy"
	autoOnlineFile   = "/sys/devices/system/memory/auto_online_blocks"
	zoneinfoFile     = "/proc/zoneinfo"
)

// ReadMemoryLayout reports the guest's memory online policy and its movable/kernel split.
func ReadMemoryLayout() MemoryLayout {
	return readMemoryLayout(onlinePolicyFile, autoOnlineFile, zoneinfoFile, uint64(os.Getpagesize()))
}

func readMemoryLayout(policyFile, autoOnlineFile, zoneinfo string, pageSize uint64) MemoryLayout {
	l := MemoryLayout{
		OnlinePolicy: readTrimmed(policyFile),
		AutoOnline:   readTrimmed(autoOnlineFile),
	}

	data, err := os.ReadFile(zoneinfo)
	if err != nil {
		return l
	}
	movable, kernel := zoneManagedPages(data)
	l.Movable, l.Kernel = movable*pageSize, kernel*pageSize

	return l
}

func readTrimmed(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(b))
}

// zoneManagedPages sums the "managed" pages of every zone in /proc/zoneinfo, ZONE_MOVABLE
// apart from the rest. Managed, not present: present counts a whole onlined memory block,
// including the sub-blocks virtio-mem left unplugged, and the kernel's reserved pages, so
// only managed adds up to MemTotal.
func zoneManagedPages(zoneinfo []byte) (movable, kernel uint64) {
	var zone string
	sc := bufio.NewScanner(bytes.NewReader(zoneinfo))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		switch {
		case len(f) == 4 && f[0] == "Node" && f[2] == "zone":
			zone = f[3]
		case len(f) == 2 && f[0] == "managed" && zone != "":
			n, err := strconv.ParseUint(f[1], 10, 64)
			if err != nil {
				continue
			}
			if zone == "Movable" {
				movable += n
			} else {
				kernel += n
			}
			zone = ""
		}
	}

	return movable, kernel
}
