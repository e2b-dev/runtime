package templategc

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// StorageAncestorReader reads a build's memfile and rootfs headers from template
// storage and unions the build IDs their block mappings reference. It is the
// production AncestorReader. A missing or unparseable header returns an error so
// Mark fails closed.
type StorageAncestorReader struct {
	Storage storage.StorageProvider
}

func (r StorageAncestorReader) AncestorBuilds(ctx context.Context, buildID uuid.UUID) ([]uuid.UUID, error) {
	paths := storage.Paths{BuildID: buildID.String()}

	seen := map[uuid.UUID]struct{}{buildID: {}}
	for _, hdrPath := range []string{paths.MemfileHeader(), paths.RootfsHeader()} {
		h, _, err := header.LoadHeader(ctx, r.Storage, hdrPath)
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
