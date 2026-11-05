package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

func (p *processor) storeNfsStat() error {
	data, err := os.ReadFile("/proc/net/rpc/nfs")
	if err != nil {
		return fmt.Errorf("failed to read nfs stat: %w", err)
	}

	fname, err := os.CreateTemp("", "nfs-stat-*.txt")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer safeClose(fname)

	if _, err := fname.Write(data); err != nil {
		return fmt.Errorf("failed to write to temp file: %w", err)
	}

	p.nfsStatFile = fname.Name()

	return nil
}

func (p *processor) compareNfsStat(ctx context.Context) error {
	defer safeRemove(p.nfsStatFile)

	output, err := exec.CommandContext(ctx, "nfsstat", "--list", "--since", p.nfsStatFile, "/proc/net/rpc/nfs").Output()
	if err != nil {
		return fmt.Errorf("failed to compare nfs stat: %w", err)
	}

	stats, err := nfsstatParse(string(output))
	if err != nil {
		return fmt.Errorf("failed to parse nfs stat: %w", err)
	}

	summarizeNfsstat(stats)

	return nil
}

func summarizeNfsstat(stats []nfsstat) {
	for _, stat := range stats {
		fmt.Printf("%s:\t%s:\t%d\n", stat.category, stat.function, stat.count)
	}
}

type nfsstat struct {
	category, function string
	count              int
}

func nfsstatParse(s string) ([]nfsstat, error) {
	var stats []nfsstat
	lines := strings.SplitSeq(s, "\n")
	for line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "---") {
			continue
		}

		parts := strings.Split(line, ":")
		if len(parts) != 2 {
			continue
		}

		keyPart := strings.TrimSpace(parts[0])
		valuePart := strings.TrimSpace(parts[1])

		before, after, ok := strings.CutLast(keyPart, " ")
		if !ok {
			continue
		}

		category := strings.TrimSpace(before)
		function := strings.TrimSpace(after)

		var count int
		if _, err := fmt.Sscanf(valuePart, "%d", &count); err != nil {
			return nil, fmt.Errorf("failed to parse count for %s %s: %w", category, function, err)
		}

		stats = append(stats, nfsstat{
			category: category,
			function: function,
			count:    count,
		})
	}

	return stats, nil
}

func safeRemove(file string) {
	if err := os.Remove(file); err != nil {
		log.Println("failed to remove file", "error", err)
	}
}
