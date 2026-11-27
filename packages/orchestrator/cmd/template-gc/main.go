// Command template-gc garbage-collects template-storage build artifacts.
//
// It has two phases, both of which re-derive liveness from ground truth at run
// time (a stale reverse-reference index is more dangerous than reading the
// headers — a dropped edge is permanent corruption):
//
//   - MARK (default): scan the bucket, expand each live root's header-mapping
//     ancestor closure, and write manifests of what to keep and what is an
//     unreferenced, old-enough deletion candidate. Deletes nothing.
//   - SWEEP (-sweep): re-run the mark fresh, intersect it with an
//     operator-approved candidates file, and delete only builds that are STILL
//     unreferenced and old enough. This is operator-gated: it is a DRY RUN
//     unless -confirm-delete is passed, so an approved manifest that went stale
//     cannot delete a build that became referenced since it was written.
//
// Either phase can write a Prometheus textfile (-metrics-file) so the fleet can
// trend orphaned-storage bytes instead of eyeballing a one-off JSON manifest.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/orchestrator/cmd/internal/cmdutil"
	"github.com/e2b-dev/infra/packages/orchestrator/internal/templategc"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func main() {
	storagePath := flag.String("storage", "", "template storage: local path or gs://bucket")
	rootsFile := flag.String("roots", "", "path to a JSON file: array of live-root build IDs (exported separately)")
	graceDays := flag.Int("grace-days", 60, "a build prefix is a candidate only if every object under it is older than this many days")
	outDir := flag.String("out", ".", "directory to write manifests")
	metricsFile := flag.String("metrics-file", "", "optional path to write Prometheus textfile gauges (node_exporter textfile collector)")

	sweep := flag.Bool("sweep", false, "sweep mode: delete the intersection of -approved and a fresh mark (DRY RUN unless -confirm-delete)")
	approvedFile := flag.String("approved", "", "sweep mode: path to the operator-approved candidates.json (as written by a prior mark run)")
	confirmDelete := flag.Bool("confirm-delete", false, "sweep mode: actually delete. Without this, sweep is a dry run and deletes nothing")
	flag.Parse()

	if *storagePath == "" || *rootsFile == "" {
		fmt.Fprintln(os.Stderr, "both -storage and -roots are required")
		os.Exit(2)
	}
	if *sweep && *approvedFile == "" {
		fmt.Fprintln(os.Stderr, "-sweep requires -approved <candidates.json>")
		os.Exit(2)
	}

	ctx := context.Background()
	if err := run(ctx, runOpts{
		storagePath:   *storagePath,
		rootsFile:     *rootsFile,
		graceDays:     *graceDays,
		outDir:        *outDir,
		metricsFile:   *metricsFile,
		sweep:         *sweep,
		approvedFile:  *approvedFile,
		confirmDelete: *confirmDelete,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "template-gc failed: %v\n", err)
		os.Exit(1)
	}
}

type runOpts struct {
	storagePath   string
	rootsFile     string
	graceDays     int
	outDir        string
	metricsFile   string
	sweep         bool
	approvedFile  string
	confirmDelete bool
}

func run(ctx context.Context, o runOpts) error {
	roots, err := readRoots(o.rootsFile)
	if err != nil {
		return fmt.Errorf("read roots: %w", err)
	}

	spec, err := cmdutil.TemplateSpec(o.storagePath)
	if err != nil {
		return fmt.Errorf("resolve storage spec: %w", err)
	}

	provider, err := storage.NewProvider(ctx, spec)
	if err != nil {
		return fmt.Errorf("storage provider: %w", err)
	}

	objects, err := storage.ListObjects(ctx, provider, "")
	if err != nil {
		return fmt.Errorf("list bucket (fail closed; cannot treat an unlistable bucket as empty): %w", err)
	}

	now := time.Now()
	manifest, err := templategc.Mark(ctx, templategc.StorageAncestorReader{Storage: provider}, templategc.MarkInput{
		LiveRoots: roots,
		Objects:   objects,
		Now:       now,
		Grace:     time.Duration(o.graceDays) * 24 * time.Hour,
	})
	if err != nil {
		return err
	}

	for name, v := range map[string]any{
		"keep.json":         manifest.Keep,
		"candidates.json":   manifest.Candidates,
		"too-new.json":      manifest.TooNew,
		"unknown-keys.json": manifest.UnknownKeys,
	} {
		if err := writeJSON(o.outDir, name, v); err != nil {
			return err
		}
	}

	var candBytes int64
	for _, c := range manifest.Candidates {
		candBytes += c.TotalBytes
	}

	fmt.Printf("mark complete (fresh, nothing deleted in this phase)\n")
	fmt.Printf("  live roots:      %d\n", len(roots))
	fmt.Printf("  keep builds:     %d\n", len(manifest.Keep))
	fmt.Printf("  candidates:      %d (%d bytes)\n", len(manifest.Candidates), candBytes)
	fmt.Printf("  held (too new):  %d\n", len(manifest.TooNew))
	fmt.Printf("  unknown keys:    %d\n", len(manifest.UnknownKeys))
	fmt.Printf("  manifests in:    %s\n", o.outDir)

	var sweepRes *templategc.SweepResult
	if o.sweep {
		sweepRes, err = runSweep(ctx, provider, o, manifest)
		if err != nil {
			return err
		}
	}

	if o.metricsFile != "" {
		if err := writeMetrics(o.metricsFile, manifest, sweepRes, now); err != nil {
			return fmt.Errorf("write metrics file: %w", err)
		}
	}

	return nil
}

func runSweep(ctx context.Context, provider storage.StorageProvider, o runOpts, fresh *templategc.Manifest) (*templategc.SweepResult, error) {
	approved, err := readApproved(o.approvedFile)
	if err != nil {
		return nil, fmt.Errorf("read approved candidates: %w", err)
	}

	res, err := templategc.Sweep(ctx, templategc.StoragePrefixDeleter{Storage: provider}, approved, fresh, o.confirmDelete)
	if err != nil {
		return nil, err
	}

	mode := "DRY RUN (pass -confirm-delete to actually delete)"
	if res.Confirmed {
		mode = "CONFIRMED — objects deleted"
	}
	fmt.Printf("sweep complete [%s]\n", mode)
	fmt.Printf("  approved:        %d\n", len(approved))
	fmt.Printf("  deleted:         %d (%d bytes)\n", len(res.Deleted), res.ReclaimedBytes)
	fmt.Printf("  skipped(changed):%d  (became referenced/too-new since mark; TOCTOU guard)\n", len(res.SkippedChanged))
	fmt.Printf("  failed:          %d\n", len(res.Failed))

	return res, nil
}

func writeMetrics(path string, manifest *templategc.Manifest, sweep *templategc.SweepResult, now time.Time) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return templategc.WriteOrphanMetrics(f, manifest, sweep, now)
}

func readRoots(path string) ([]uuid.UUID, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var raw []string
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("roots file must be a JSON array of build-ID strings: %w", err)
	}

	return parseIDs(raw)
}

// readApproved reads the operator-approved candidates file. It accepts the exact
// candidates.json a prior mark wrote (an array of {build_id,...} objects) so the
// operator reviews and approves the same artifact, not a re-typed list.
func readApproved(path string) ([]uuid.UUID, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var prefixes []templategc.BuildPrefix
	if err := json.Unmarshal(data, &prefixes); err == nil && len(prefixes) > 0 && prefixes[0].BuildID != uuid.Nil {
		ids := make([]uuid.UUID, 0, len(prefixes))
		for _, p := range prefixes {
			ids = append(ids, p.BuildID)
		}

		return ids, nil
	}

	// Fall back to a plain array of build-ID strings.
	var raw []string
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("approved file must be a candidates.json or a JSON array of build-ID strings: %w", err)
	}

	return parseIDs(raw)
}

func parseIDs(raw []string) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, 0, len(raw))
	for _, s := range raw {
		id, err := uuid.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("invalid build ID %q: %w", s, err)
		}
		ids = append(ids, id)
	}

	return ids, nil
}

func writeJSON(dir, name string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(dir+"/"+name, data, 0o644)
}
