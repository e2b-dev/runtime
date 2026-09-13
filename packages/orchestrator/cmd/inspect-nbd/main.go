//go:build linux

// inspect-nbd lists the NBD devices that are currently connected on this host.
//
// Background: the nbd kernel module is loaded with nbds_max=4096, creating
// 4096 /dev/nbdX entries in the OS even when only a handful are in use.
// `lsblk | grep nbd` produces 4096 lines of noise. This tool reads the kernel's
// /sys/block/nbdX/pid sentinel — which only exists for connected devices — and
// prints a concise table of active slots.
//
// This is an operator debugging tool, not a service. It is not part of the
// orchestrator Nomad job and is not uploaded to GCS. To use it on a sandbox
// node, compile it locally and copy the binary:
//
//	GOOS=linux GOARCH=amd64 go build -o /tmp/inspect-nbd ./cmd/inspect-nbd
//	scp /tmp/inspect-nbd <node>:/tmp/inspect-nbd
//
// Usage:
//
//	inspect-nbd [-json]
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd"
)

func main() {
	jsonOut := flag.Bool("json", false, "output as JSON")
	flag.Parse()

	maxData, err := os.ReadFile("/sys/module/nbd/parameters/nbds_max")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: nbd module not loaded or /sys unavailable: %v\n", err)
		os.Exit(1)
	}
	nbdsMax, _ := strconv.ParseUint(strings.TrimSpace(string(maxData)), 10, 64)

	devices, err := nbd.ConnectedDevices()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to list connected devices: %v\n", err)
		os.Exit(1)
	}

	type deviceInfo struct {
		Slot   uint32 `json:"slot"`
		Path   string `json:"path"`
		SizeMB uint64 `json:"size_mb"`
		PID    string `json:"pid"`
	}

	infos := make([]deviceInfo, 0, len(devices))
	for _, slot := range devices {
		sizeRaw, _ := os.ReadFile(fmt.Sprintf("/sys/block/nbd%d/size", slot))
		sectors, _ := strconv.ParseUint(strings.TrimSpace(string(sizeRaw)), 10, 64)

		pidRaw, _ := os.ReadFile(fmt.Sprintf("/sys/block/nbd%d/pid", slot))

		infos = append(infos, deviceInfo{
			Slot:   slot,
			Path:   nbd.GetDevicePath(slot),
			SizeMB: (sectors * 512) / (1024 * 1024),
			PID:    strings.TrimSpace(string(pidRaw)),
		})
	}

	if *jsonOut {
		type output struct {
			NBDsMax   uint64       `json:"nbds_max"`
			Connected int          `json:"connected"`
			Devices   []deviceInfo `json:"devices"`
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(output{NBDsMax: nbdsMax, Connected: len(infos), Devices: infos})

		return
	}

	fmt.Printf("NBD devices: %d connected / %d configured\n", len(infos), nbdsMax)

	if len(infos) == 0 {
		fmt.Println("No connected NBD devices.")

		return
	}

	fmt.Println()
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SLOT\tDEVICE\tSIZE (MB)\tPID")
	for _, info := range infos {
		fmt.Fprintf(w, "%d\t%s\t%d\t%s\n", info.Slot, info.Path, info.SizeMB, info.PID)
	}
	_ = w.Flush()
}
