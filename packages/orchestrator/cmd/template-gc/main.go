// Command template-gc is the DRY-RUN mark phase of template-storage garbage
// collection. It reads a list of live-root build IDs (exported separately from
// the control plane), expands each root's header-mapping ancestor closure,
// scans the template bucket, and writes two manifests: the builds to KEEP and
// the build prefixes that are unreferenced and older than the grace period and
// therefore safe to delete.
//
// It NEVER deletes anything. Physical deletion is a separate, operator-gated
// step that consumes the candidate manifest after quarantine and revalidation.
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
	outDir := flag.String("out", ".", "directory to write keep.json and candidates.json manifests")
	flag.Parse()

	if *storagePath == "" || *rootsFile == "" {
		fmt.Fprintln(os.Stderr, "both -storage and -roots are required")
		os.Exit(2)
	}

	ctx := context.Background()
	if err := run(ctx, *storagePath, *rootsFile, *graceDays, *outDir); err != nil {
		fmt.Fprintf(os.Stderr, "template-gc mark failed (no objects were deleted): %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, storagePath, rootsFile string, graceDays int, outDir string) error {
	roots, err := readRoots(rootsFile)
	if err != nil {
		return fmt.Errorf("read roots: %w", err)
	}

	spec, err := cmdutil.TemplateSpec(storagePath)
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

	manifest, err := templategc.Mark(ctx, templategc.StorageAncestorReader{Storage: provider}, templategc.MarkInput{
		LiveRoots: roots,
		Objects:   objects,
		Now:       time.Now(),
		Grace:     time.Duration(graceDays) * 24 * time.Hour,
	})
	if err != nil {
		return err
	}

	if err := writeJSON(outDir, "keep.json", manifest.Keep); err != nil {
		return err
	}
	if err := writeJSON(outDir, "candidates.json", manifest.Candidates); err != nil {
		return err
	}
	if err := writeJSON(outDir, "too-new.json", manifest.TooNew); err != nil {
		return err
	}
	if err := writeJSON(outDir, "unknown-keys.json", manifest.UnknownKeys); err != nil {
		return err
	}

	var candBytes int64
	for _, c := range manifest.Candidates {
		candBytes += c.TotalBytes
	}

	fmt.Printf("mark complete (DRY RUN, nothing deleted)\n")
	fmt.Printf("  live roots:      %d\n", len(roots))
	fmt.Printf("  keep builds:     %d\n", len(manifest.Keep))
	fmt.Printf("  candidates:      %d (%d bytes)\n", len(manifest.Candidates), candBytes)
	fmt.Printf("  held (too new):  %d\n", len(manifest.TooNew))
	fmt.Printf("  unknown keys:    %d\n", len(manifest.UnknownKeys))
	fmt.Printf("  manifests in:    %s\n", outDir)

	return nil
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

	roots := make([]uuid.UUID, 0, len(raw))
	for _, s := range raw {
		id, err := uuid.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("invalid root build ID %q: %w", s, err)
		}
		roots = append(roots, id)
	}

	return roots, nil
}

func writeJSON(dir, name string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(dir+"/"+name, data, 0o644)
}
