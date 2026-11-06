package handlers

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"

	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// Fail-closed reasons. These are LOW-cardinality and safe to use as a metric
// attribute so the fail_closed counter is alertable by cause. team_id and
// build_id are deliberately NOT metric attributes (high cardinality); they stay
// on the log line / trace instead.
var (
	// errReaderUnconfigured: no template-storage access, so the header closure
	// cannot be read at all. The deployment simply does not reclaim space.
	errReaderUnconfigured = errors.New("build ancestor reader not configured")
	// errLiveRootsQuery: the set of builds that could protect a candidate could
	// not be enumerated from the DB.
	errLiveRootsQuery = errors.New("live build roots query failed")
	// errHeaderUnreadable: a live root's header could not be read/parsed, so its
	// ancestor closure is unknown.
	errHeaderUnreadable = errors.New("live root header unreadable")
)

// failClosedReason maps a fail-closed error to a low-cardinality reason label.
func failClosedReason(err error) string {
	switch {
	case errors.Is(err, errReaderUnconfigured):
		return "reader_unconfigured"
	case errors.Is(err, errLiveRootsQuery):
		return "live_roots_query"
	case errors.Is(err, errHeaderUnreadable):
		return "header_unreadable"
	default:
		return "unknown"
	}
}

// buildAncestorReader resolves the set of ancestor build IDs that a given build
// actually references through its header block mappings (memfile + rootfs). A
// template build stores only the bytes it changed relative to its ancestors;
// the full image is reconstructed by following the header's BuildMap into
// parent, grandparent, ... builds. Those ancestor builds are NOT recorded in
// the database (intermediate cache layers never get an env_build_assignments
// row), so the only source of truth for "which builds does this one depend on"
// is the header mapping itself.
type buildAncestorReader interface {
	// AncestorBuilds returns every build ID referenced by buildID's memfile and
	// rootfs header mappings, including buildID itself. An error means the
	// dependency set could not be established and the caller must fail closed.
	AncestorBuilds(ctx context.Context, buildID uuid.UUID) ([]uuid.UUID, error)
}

// reachabilityKeepSet computes the set of builds that must NOT be deleted
// because a live root of the team still reaches them through its header
// closure. It fails closed (returns an error) when the reader is unavailable,
// the live-root set cannot be queried, or any live root's header cannot be
// read, so the caller deletes nothing rather than risk orphaning a shared
// ancestor. The returned error is classified by failClosedReason.
func (a *APIStore) reachabilityKeepSet(ctx context.Context, teamID uuid.UUID, templateID string, candidates []queries.GetExclusiveBuildsForTemplateDeletionRow) (map[uuid.UUID]struct{}, error) {
	if len(candidates) == 0 {
		return map[uuid.UUID]struct{}{}, nil
	}

	if a.buildAncestors == nil {
		return nil, errReaderUnconfigured
	}

	liveRoots, err := collectLiveRoots(ctx, a.liveRootSourcesOrDefault(), teamID, templateID)
	if err != nil {
		return nil, err
	}

	return reachableKeepSet(ctx, a.buildAncestors, liveRoots)
}

// liveRootSource yields a set of build IDs that may protect a deletion candidate
// by referencing it through their header closure. The reachability guard unions
// every source's roots and expands each root's ancestor closure. A source that
// returns an error makes the whole guard fail closed (the caller deletes
// nothing), so a source must return an error — never a silently empty slice —
// when it cannot authoritatively enumerate its roots.
type liveRootSource interface {
	// liveRoots returns the build IDs this source contributes as live roots for
	// the team, excluding the template being deleted. An error means the set is
	// unknown and the guard must fail closed.
	liveRoots(ctx context.Context, teamID uuid.UUID, templateID string) ([]uuid.UUID, error)
	// name is a short, low-cardinality label for logs/metrics.
	name() string
}

// dbLiveRootSource is the production live-root source. In one query it unions
// (1) other active templates' assigned builds, (2) the team's in-progress/
// pending builds, and (3) the team's snapshot builds (paused sandboxes /
// checkpoints) regardless of whether the snapshot's own env is still active —
// see get_live_build_roots.sql. Source (3) is what keeps a base template's
// builds protected while a checkpoint-derived snapshot still references them
// through its COW header mapping, even after the snapshot's env is soft-deleted.
type dbLiveRootSource struct {
	db *sqlcdb.Client
}

func (s dbLiveRootSource) name() string { return "db" }

func (s dbLiveRootSource) liveRoots(ctx context.Context, teamID uuid.UUID, templateID string) ([]uuid.UUID, error) {
	return s.db.GetLiveBuildRootsForTeamExcludingTemplate(ctx, queries.GetLiveBuildRootsForTeamExcludingTemplateParams{
		TeamID:     teamID,
		TemplateID: templateID,
	})
}

// collectLiveRoots unions the roots of every configured source. It fails closed
// on the FIRST source error: a partial root set is more dangerous than none,
// because a missing root means its ancestor closure is absent from the keep set
// and a still-referenced build could then be deleted. The error is wrapped as
// errLiveRootsQuery so the caller classifies it as a fail-closed reason.
func collectLiveRoots(ctx context.Context, sources []liveRootSource, teamID uuid.UUID, templateID string) ([]uuid.UUID, error) {
	seen := make(map[uuid.UUID]struct{})
	var roots []uuid.UUID
	for _, src := range sources {
		ids, err := src.liveRoots(ctx, teamID, templateID)
		if err != nil {
			return nil, fmt.Errorf("%w: source %s: %w", errLiveRootsQuery, src.name(), err)
		}
		for _, id := range ids {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			roots = append(roots, id)
		}
	}

	return roots, nil
}

// liveRootSourcesOrDefault returns the injected sources, or the default DB
// source when none were set (production). Tests inject stubs.
func (a *APIStore) liveRootSourcesOrDefault() []liveRootSource {
	if len(a.liveRootSources) > 0 {
		return a.liveRootSources
	}

	return []liveRootSource{dbLiveRootSource{db: a.sqlcDB}}
}

// reachableKeepSet unions the ancestor closure of every live root build into a
// single "keep" set. Any build in this set is still referenced by a live build
// of the team and must never be deleted. A read failure for any root is
// returned as an error (wrapped as errHeaderUnreadable) so the caller fails
// closed (deletes nothing) rather than risk deleting a build whose reachability
// could not be proven.
func reachableKeepSet(ctx context.Context, reader buildAncestorReader, liveRoots []uuid.UUID) (map[uuid.UUID]struct{}, error) {
	keep := make(map[uuid.UUID]struct{})

	for _, root := range liveRoots {
		ancestors, err := reader.AncestorBuilds(ctx, root)
		if err != nil {
			return nil, fmt.Errorf("%w: resolve ancestors of live root %s: %w", errHeaderUnreadable, root, err)
		}

		for _, anc := range ancestors {
			keep[anc] = struct{}{}
		}
	}

	return keep, nil
}

// partitionDeletableBuilds splits candidate builds (those the DB considers
// exclusive to the doomed template) into the ones that are safe to physically
// delete and the ones that must be protected because a live root's header
// mapping still references them. A candidate is deletable only if it is NOT in
// the keep set. This is the guard against the silent, permanent "Input/output
// error" corruption that results from deleting a build whose blocks another
// live build still reconstructs from.
func partitionDeletableBuilds(candidates []uuid.UUID, keep map[uuid.UUID]struct{}) (deletable, protected []uuid.UUID) {
	for _, b := range candidates {
		if _, kept := keep[b]; kept {
			protected = append(protected, b)

			continue
		}

		deletable = append(deletable, b)
	}

	return deletable, protected
}

// storageBuildAncestorReader is the production buildAncestorReader. It reads the
// two header sidecar objects of a build from template storage and unions the
// build IDs their block mappings reference.
type storageBuildAncestorReader struct {
	storage storage.StorageProvider
}

func newStorageBuildAncestorReader(s storage.StorageProvider) *storageBuildAncestorReader {
	return &storageBuildAncestorReader{storage: s}
}

func (r *storageBuildAncestorReader) AncestorBuilds(ctx context.Context, buildID uuid.UUID) ([]uuid.UUID, error) {
	paths := storage.Paths{BuildID: buildID.String()}

	seen := map[uuid.UUID]struct{}{buildID: {}}

	// Both the memfile and rootfs headers carry their own mapping; a build can
	// share a parent on one and not the other, so both must be read. A missing
	// or unparseable header means the dependency set is unknown -> error ->
	// caller fails closed.
	for _, hdrPath := range []string{paths.MemfileHeader(), paths.RootfsHeader()} {
		h, _, err := header.LoadHeader(ctx, r.storage, hdrPath)
		if err != nil {
			return nil, fmt.Errorf("load header %s: %w", hdrPath, err)
		}

		for _, b := range h.Mapping.Builds() {
			seen[b] = struct{}{}
		}
	}

	out := make([]uuid.UUID, 0, len(seen))
	for b := range seen {
		out = append(out, b)
	}

	return out, nil
}

// newTemplateBuildAncestorReader builds the production ancestor reader from the
// template storage destination in the environment (TEMPLATE_STORAGE_URL). It is
// best-effort: if the destination is unset or the provider cannot be built, it
// returns nil, and the deletion guard then fails closed (deletes nothing),
// which is no worse than leaving artifacts in place. Credentials are never
// echoed; only a build error (which may embed the bucket name, not secrets) is
// logged.
func newTemplateBuildAncestorReader(ctx context.Context) buildAncestorReader {
	raw := strings.TrimSpace(os.Getenv("TEMPLATE_STORAGE_URL"))
	if raw == "" {
		logger.L().Warn(ctx, "TEMPLATE_STORAGE_URL unset; template artifact cleanup will fail closed")

		return nil
	}

	spec, err := storage.ParseStorageURL(raw)
	if err != nil {
		logger.L().Warn(ctx, "could not parse TEMPLATE_STORAGE_URL; template artifact cleanup will fail closed", logger.WithError(err))

		return nil
	}

	provider, err := storage.NewProvider(ctx, spec)
	if err != nil {
		logger.L().Warn(ctx, "could not build template storage provider; template artifact cleanup will fail closed", logger.WithError(err))

		return nil
	}

	return newStorageBuildAncestorReader(provider)
}
