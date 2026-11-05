package templategc

import (
	"fmt"
	"io"
	"time"
)

// WriteOrphanMetrics writes the mark/sweep outcome as Prometheus text-exposition
// gauges, suitable for the node_exporter textfile collector (a batch job exposes
// a gauge by writing a .prom file, not by running a server). Scraping this file
// gives the fleet a live "orphaned storage" trend that was previously only
// visible by eyeballing a one-off JSON manifest.
//
// manifest is the (fresh) mark result; sweep may be nil for a mark-only run.
func WriteOrphanMetrics(w io.Writer, manifest *Manifest, sweep *SweepResult, now time.Time) error {
	var orphanBytes, tooNewBytes int64
	for _, c := range manifest.Candidates {
		orphanBytes += c.TotalBytes
	}
	for _, c := range manifest.TooNew {
		tooNewBytes += c.TotalBytes
	}

	write := func(name, help string, value int64) error {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", name, help, name, name, value); err != nil {
			return err
		}

		return nil
	}

	if err := write("template_gc_orphan_builds", "Build prefixes unreferenced by any live root and older than the grace period (deletion candidates).", int64(len(manifest.Candidates))); err != nil {
		return err
	}
	if err := write("template_gc_orphan_bytes", "Aggregate bytes of orphaned (candidate) build prefixes. Alert on sustained growth: online deletion is not keeping up.", orphanBytes); err != nil {
		return err
	}
	if err := write("template_gc_too_new_builds", "Unreferenced build prefixes held back by the grace window (possible in-flight upload).", int64(len(manifest.TooNew))); err != nil {
		return err
	}
	if err := write("template_gc_too_new_bytes", "Aggregate bytes held back by the grace window.", tooNewBytes); err != nil {
		return err
	}
	if err := write("template_gc_unknown_keys", "Object keys not under a UUID build prefix; surfaced for review, never deleted.", int64(len(manifest.UnknownKeys))); err != nil {
		return err
	}
	if err := write("template_gc_keep_builds", "Builds kept because a live root reaches them through its header closure.", int64(len(manifest.Keep))); err != nil {
		return err
	}

	if sweep != nil {
		if err := write("template_gc_swept_builds", "Build prefixes deleted (or that would be deleted in a dry run) by the last sweep.", int64(len(sweep.Deleted))); err != nil {
			return err
		}
		if err := write("template_gc_swept_bytes", "Bytes reclaimed by the last sweep.", sweep.ReclaimedBytes); err != nil {
			return err
		}
		if err := write("template_gc_sweep_skipped_changed", "Approved builds skipped because they became reachable or too-new since the mark (TOCTOU guard).", int64(len(sweep.SkippedChanged))); err != nil {
			return err
		}
		if err := write("template_gc_sweep_failed", "Approved builds whose deletion errored (possible partial delete; retried next sweep).", int64(len(sweep.Failed))); err != nil {
			return err
		}
		confirmed := int64(0)
		if sweep.Confirmed {
			confirmed = 1
		}
		if err := write("template_gc_sweep_confirmed", "1 if the last sweep actually deleted objects, 0 if it was a dry run.", confirmed); err != nil {
			return err
		}
	}

	return write("template_gc_last_run_timestamp_seconds", "Unix time of the last template-gc run.", now.Unix())
}
