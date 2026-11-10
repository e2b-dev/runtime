package cpus

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/e2b-dev/infra/packages/envd/internal/reaper"
	"github.com/e2b-dev/infra/packages/envd/internal/reexec"
)

const (
	sysfsCPUDir = "/sys/devices/system/cpu"

	// maxCPUs is the kernel's largest NR_CPUS. It bounds what a bogus mask can make
	// parseMask allocate.
	maxCPUs = 8192
	// maxMaskBytes bounds a mask read. The kernel prints at most one base page: 4 KiB on
	// x86, up to 64 KiB on arm64. Huge pages do not change it.
	maxMaskBytes = 64 << 10
)

var errInvalidCPU = fmt.Errorf("not a CPU number below %d", maxCPUs)

// writeOnline is the child that performs one hotplug write. The write blocks in the
// kernel until the CPU is up or down, and one the kernel never finishes cannot be
// interrupted or killed. In a child process it blocks only the worker waiting for it.
var writeOnline = reexec.Define("cpu-online", writeOnlineFile)

// writeOnlineFile takes the cpuN/online file and "0" or "1". It runs with the caller's
// privileges, so its checks guard against a mistaken caller.
func writeOnlineFile(args []string) error {
	if len(args) != 2 || (args[1] != "0" && args[1] != "1") {
		return errors.New("usage: cpu-online PATH 0|1")
	}
	// Only a cpuN/online attribute, wherever the caller's sysfs root is.
	dir, file := filepath.Split(filepath.Clean(args[0]))
	cpu, isCPU := strings.CutPrefix(filepath.Base(dir), "cpu")
	if _, err := parseCPU(cpu); err != nil || !isCPU || file != "online" {
		return fmt.Errorf("%.64q is not a cpuN/online file", args[0])
	}

	// No O_CREATE: the kernel made the attribute, or the path is wrong.
	f, err := os.OpenFile(args[0], os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return err
	}
	_, err = f.WriteString(args[1])

	return errors.Join(err, f.Close())
}

// cpuSysfs is the guest kernel's CPU hotplug interface.
type cpuSysfs interface {
	Possible() ([]int, error)
	Online() ([]int, error)
	// SetOnline brings one CPU up or down and returns once the kernel has finished.
	SetOnline(cpu int, online bool) error
}

type kernelCPUSysfs struct {
	dir      string
	children *reaper.Registry
}

func (k kernelCPUSysfs) Possible() ([]int, error) { return k.readMask("possible") }

// Online reads the aggregate mask. A CPU's own cpuN/online file takes that CPU's device
// lock, which a hotplug holds throughout, so reading it would wait behind the worker.
func (k kernelCPUSysfs) Online() ([]int, error) { return k.readMask("online") }

// SetOnline runs writeOnline in a child process and reports its error.
func (k kernelCPUSysfs) SetOnline(cpu int, online bool) error {
	value := "0"
	if online {
		value = "1"
	}
	cmd := writeOnline.Exec(filepath.Join(k.dir, "cpu"+strconv.Itoa(cpu), "online"), value)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := k.children.Run(cmd)
	if msg := bytes.TrimSpace(stderr.Bytes()); err != nil && len(msg) > 0 {
		return fmt.Errorf("%w: %s", err, msg)
	}

	return err
}

func (k kernelCPUSysfs) readMask(name string) ([]int, error) {
	f, err := os.Open(filepath.Join(k.dir, name))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	b, err := io.ReadAll(io.LimitReader(f, maxMaskBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxMaskBytes {
		return nil, fmt.Errorf("%s: longer than %d bytes", name, maxMaskBytes)
	}

	return parseMask(strings.TrimSpace(string(b)))
}

// parseMask reads the kernel's cpulist format, e.g. "0-3,8,10-11". Ranges must be in
// ascending order without overlap, as the kernel prints them, so the result is strictly
// ascending and holds at most maxCPUs entries.
func parseMask(s string) ([]int, error) {
	if s == "" {
		return nil, nil
	}
	var cpus []int
	next := 0
	for part := range strings.SplitSeq(s, ",") {
		lo, hi, isRange := strings.Cut(part, "-")
		start, err := parseCPU(lo)
		if err != nil {
			return nil, fmt.Errorf("cpu list range %.16q: %w", part, err)
		}
		end := start
		if isRange {
			if end, err = parseCPU(hi); err != nil {
				return nil, fmt.Errorf("cpu list range %.16q: %w", part, err)
			}
		}
		if start < next || end < start {
			return nil, fmt.Errorf("cpu list range %.16q: out of order", part)
		}
		for cpu := start; cpu <= end; cpu++ {
			cpus = append(cpus, cpu)
		}
		next = end + 1
	}

	return cpus, nil
}

// parseCPU accepts only a decimal CPU number below maxCPUs: no sign, spaces or prefix.
// Its errors leave the input out, since it may be arbitrarily long.
func parseCPU(s string) (int, error) {
	n, err := strconv.ParseUint(s, 10, 16)
	if err != nil || n >= maxCPUs {
		return 0, errInvalidCPU
	}

	return int(n), nil
}
