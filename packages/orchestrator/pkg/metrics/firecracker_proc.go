//go:build linux

package metrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/artifact"
)

type processIdentity struct {
	pid        int
	startTicks uint64
}

type firecrackerProcess struct {
	identity processIdentity
	socket   string
	age      time.Duration
}

type firecrackerProcReader struct {
	root     string
	clockHz  float64
	readFile func(string) ([]byte, error)
}

func (r firecrackerProcReader) scan(ctx context.Context) ([]firecrackerProcess, error) {
	uptimeData, err := r.readFile(filepath.Join(r.root, "uptime"))
	if err != nil {
		return nil, fmt.Errorf("reading uptime: %w", err)
	}
	fields := strings.Fields(string(uptimeData))
	if len(fields) == 0 {
		return nil, errors.New("empty process uptime")
	}
	uptime, err := strconv.ParseFloat(fields[0], 64)
	if err != nil || math.IsNaN(uptime) || math.IsInf(uptime, 0) || uptime < 0 || r.clockHz <= 0 {
		return nil, fmt.Errorf("invalid process clock: uptime=%q hz=%v", fields[0], r.clockHz)
	}
	entries, err := os.ReadDir(r.root)
	if err != nil {
		return nil, fmt.Errorf("reading process directory: %w", err)
	}
	var processes []firecrackerProcess
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 || !entry.IsDir() {
			continue
		}
		process, found, err := r.process(pid, uptime)
		if os.IsNotExist(err) || errors.Is(err, syscall.ESRCH) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("reading process %d: %w", pid, err)
		}
		if found {
			processes = append(processes, process)
		}
	}

	return processes, nil
}

func (r firecrackerProcReader) process(pid int, uptime float64) (firecrackerProcess, bool, error) {
	dir := filepath.Join(r.root, strconv.Itoa(pid))
	executable, err := os.Readlink(filepath.Join(dir, "exe"))
	if err != nil {
		return firecrackerProcess{}, false, err
	}
	// Shell wrappers mention Firecracker in argv; only its executable is counted.
	if filepath.Base(strings.TrimSuffix(executable, " (deleted)")) != artifact.FirecrackerBinaryName {
		return firecrackerProcess{}, false, nil
	}
	before, err := r.readFile(filepath.Join(dir, "stat"))
	if err != nil {
		return firecrackerProcess{}, false, err
	}
	start, state, err := processStart(before)
	if err != nil || state == "Z" || state == "X" {
		return firecrackerProcess{}, false, err
	}
	cmdline, err := r.readFile(filepath.Join(dir, "cmdline"))
	if err != nil {
		return firecrackerProcess{}, false, err
	}
	after, err := r.readFile(filepath.Join(dir, "stat"))
	if err != nil {
		return firecrackerProcess{}, false, err
	}
	current, state, err := processStart(after)
	if err != nil || current != start || state == "Z" || state == "X" {
		return firecrackerProcess{}, false, err
	}
	verifiedExecutable, err := os.Readlink(filepath.Join(dir, "exe"))
	if err != nil || strings.TrimSuffix(verifiedExecutable, " (deleted)") != strings.TrimSuffix(executable, " (deleted)") {
		return firecrackerProcess{}, false, err
	}
	age := max(0, uptime-float64(start)/r.clockHz)

	return firecrackerProcess{
		identity: processIdentity{pid: pid, startTicks: start},
		socket:   firecrackerSocket(cmdline),
		age:      time.Duration(age * float64(time.Second)),
	}, true, nil
}

func processStart(data []byte) (uint64, string, error) {
	// comm can contain spaces and parentheses; starttime is field 22 after its final ')'.
	end := bytes.LastIndexByte(data, ')')
	if end < 0 {
		return 0, "", errors.New("process stat has no comm field")
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 20 {
		return 0, "", errors.New("process stat has no starttime")
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("parsing process starttime: %w", err)
	}

	return start, fields[0], nil
}

func firecrackerSocket(cmdline []byte) string {
	args := strings.Split(strings.TrimRight(string(cmdline), "\x00"), "\x00")
	for i, arg := range args {
		if arg == "--api-sock" && i+1 < len(args) {
			return args[i+1]
		}
		if socket, ok := strings.CutPrefix(arg, "--api-sock="); ok {
			return socket
		}
	}

	return ""
}
