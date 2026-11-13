// Package templategc implements the mark phase of template-storage garbage
// collection. It is dry-run only: it computes which build prefixes are
// unreferenced and old enough to be deletion candidates, and NEVER deletes
// anything. Physical deletion is a separate, operator-gated step.
package templategc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// AncestorReader resolves a build's header-mapping ancestor closure (the build
// itself plus every build its memfile/rootfs headers reference). An error means
// the dependency set is unknown; the marker then fails closed.
type AncestorReader interface {
	AncestorBuilds(ctx context.Context, buildID uuid.UUID) ([]uuid.UUID, error)
}

// Manifest is the result of a mark run. It is advisory: Candidates lists the
// build prefixes that are safe to delete under the inputs given, but nothing is
// deleted here.
type Manifest struct {
	// Keep is every build ID still reachable from a live root's header closure.
	Keep []uuid.UUID
	// Candidates are build prefixes not in Keep and whose every object is older
	// than the grace period — safe to delete.
	Candidates []BuildPrefix
	// TooNew are unreferenced builds held back because at least one object under
	// the prefix is newer than the grace cutoff (possibly an in-flight upload).
	TooNew []BuildPrefix
	// UnknownKeys are object keys that do not sit under a UUID build prefix;
	// surfaced for human review, never deleted.
	UnknownKeys []string
}

// BuildPrefix is a build's object-storage prefix and its aggregate stats.
type BuildPrefix struct {
	BuildID      uuid.UUID
	ObjectCount  int
	TotalBytes   int64
	NewestObject time.Time
}

// MarkInput is everything the pure Mark computation needs.
type MarkInput struct {
	// LiveRoots are the build IDs that must be kept: the union of DB active
	// templates' builds, in-progress builds, and runtime-sandbox-pinned builds.
	// The caller collects these from all sources and fails the whole run if any
	// source could not be read.
	LiveRoots []uuid.UUID
	// Objects is the full, paginated listing of the template bucket.
	Objects []storage.ObjectInfo
	// Now and Grace define the age cutoff: a build is only a candidate when every
	// object under its prefix predates Now.Add(-Grace).
	Now   time.Time
	Grace time.Duration
}

// Mark computes the deletion manifest. It fails closed: if any live root's
// ancestor closure cannot be resolved, it returns an error and no candidates, so
// an unreadable header can never cause a reachable build to be marked deletable.
func Mark(ctx context.Context, reader AncestorReader, in MarkInput) (*Manifest, error) {
	keep := make(map[uuid.UUID]struct{})
	for _, root := range in.LiveRoots {
		closure, err := reader.AncestorBuilds(ctx, root)
		if err != nil {
			return nil, fmt.Errorf("resolve ancestor closure of live root %s: %w", root, err)
		}

		for _, b := range closure {
			keep[b] = struct{}{}
		}
	}

	cutoff := in.Now.Add(-in.Grace)

	// Group objects by build prefix (the first path segment, which must be a UUID).
	type agg struct {
		count  int
		bytes  int64
		newest time.Time
	}
	byBuild := make(map[uuid.UUID]*agg)
	var unknown []string

	for _, obj := range in.Objects {
		seg, _, ok := strings.Cut(obj.Path, "/")
		if !ok {
			unknown = append(unknown, obj.Path)

			continue
		}

		id, err := uuid.Parse(seg)
		if err != nil {
			unknown = append(unknown, obj.Path)

			continue
		}

		a := byBuild[id]
		if a == nil {
			a = &agg{}
			byBuild[id] = a
		}
		a.count++
		a.bytes += obj.Size
		if obj.LastModified.After(a.newest) {
			a.newest = obj.LastModified
		}
	}

	m := &Manifest{UnknownKeys: unknown}
	for b := range keep {
		m.Keep = append(m.Keep, b)
	}

	for id, a := range byBuild {
		if _, kept := keep[id]; kept {
			continue
		}

		bp := BuildPrefix{
			BuildID:      id,
			ObjectCount:  a.count,
			TotalBytes:   a.bytes,
			NewestObject: a.newest,
		}

		// Protect the whole prefix if any object is newer than the cutoff: a
		// fresh object may be an in-flight upload not yet reflected in the roots.
		if a.newest.After(cutoff) {
			m.TooNew = append(m.TooNew, bp)

			continue
		}

		m.Candidates = append(m.Candidates, bp)
	}

	return m, nil
}
