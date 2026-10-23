package handlers

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

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

// reachableKeepSet unions the ancestor closure of every live root build into a
// single "keep" set. Any build in this set is still referenced by a live
// template of the team and must never be deleted. A read failure for any root
// is returned as an error so the caller fails closed (deletes nothing) rather
// than risk deleting a build whose reachability could not be proven.
func reachableKeepSet(ctx context.Context, reader buildAncestorReader, liveRoots []uuid.UUID) (map[uuid.UUID]struct{}, error) {
	keep := make(map[uuid.UUID]struct{})

	for _, root := range liveRoots {
		ancestors, err := reader.AncestorBuilds(ctx, root)
		if err != nil {
			return nil, fmt.Errorf("resolve ancestors of live root %s: %w", root, err)
		}

		for _, a := range ancestors {
			keep[a] = struct{}{}
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
// live template still reconstructs from.
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
		logger.L().Warn(ctx, "Could not parse TEMPLATE_STORAGE_URL; template artifact cleanup will fail closed", zap.Error(err))

		return nil
	}

	provider, err := storage.NewProvider(ctx, spec)
	if err != nil {
		logger.L().Warn(ctx, "Could not build template storage provider; template artifact cleanup will fail closed", zap.Error(err))

		return nil
	}

	return newStorageBuildAncestorReader(provider)
}
