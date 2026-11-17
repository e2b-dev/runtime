package host

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// virtioMemRAM is the /proc/iomem name of the memory blocks virtio-mem added to the guest.
const virtioMemRAM = "System RAM (virtio_mem)"

// ErrIomemZeroed is an /proc/iomem read without the rights to see addresses: the kernel then
// shows every range as 0-0.
var ErrIomemZeroed = errors.New("/proc/iomem shows zeroed addresses")

// parseHotplugAdded sums the sizes of the "System RAM (virtio_mem)" ranges in an
// /proc/iomem dump: the memory blocks virtio-mem added, whole 128 MiB blocks even where
// only part of one is plugged. Lines are "start-end : name", hexadecimal, end inclusive,
// indented by nesting depth. A line that does not have that shape is skipped unless it
// names a virtio-mem range, which makes the dump unusable.
func parseHotplugAdded(iomem []byte) (uint64, error) {
	var added uint64
	nonZero := false
	parsed := 0
	sc := bufio.NewScanner(bytes.NewReader(iomem))
	for sc.Scan() {
		rng, name, ok := strings.Cut(strings.TrimSpace(sc.Text()), " : ")
		if !ok {
			continue
		}
		startHex, endHex, ok := strings.Cut(rng, "-")
		start, startErr := strconv.ParseUint(startHex, 16, 64)
		end, endErr := strconv.ParseUint(endHex, 16, 64)
		if !ok || startErr != nil || endErr != nil || end < start {
			if name == virtioMemRAM {
				return 0, fmt.Errorf("malformed /proc/iomem range %q", rng)
			}

			continue
		}
		parsed++
		if end != 0 {
			nonZero = true
		}
		if name == virtioMemRAM {
			added += end - start + 1
		}
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	if parsed == 0 {
		return 0, errors.New("/proc/iomem has no ranges")
	}
	if !nonZero {
		return 0, ErrIomemZeroed
	}

	return added, nil
}
