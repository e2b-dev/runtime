package templategc

import (
	"context"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

// StoragePrefixDeleter is the production PrefixDeleter. It removes every object
// under a build's prefix from template storage via DeleteObjectsWithPrefix.
type StoragePrefixDeleter struct {
	Storage storage.StorageProvider
}

func (d StoragePrefixDeleter) DeletePrefix(ctx context.Context, buildID uuid.UUID) error {
	// Pass the bare build ID, matching every other caller of
	// DeleteObjectsWithPrefix (delete_template.go, builder.go): the storage
	// layer validates object paths and rejects a trailing slash. A UUID prefix
	// matches only "{buildID}/..." keys — no sibling build can share the full
	// UUID as a string prefix.
	return d.Storage.DeleteObjectsWithPrefix(ctx, buildID.String())
}
