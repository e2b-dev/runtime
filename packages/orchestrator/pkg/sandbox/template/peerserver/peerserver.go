//go:build linux

package peerserver

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	tmpl "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerserver")

var (
	// ErrNotSupported is returned when a source type does not implement an operation.
	ErrNotSupported = errors.New("operation not supported")
	// ErrNotAvailable is returned when the requested build is not in the local peer cache.
	ErrNotAvailable = errors.New("not available in local peer cache")
)

// Sender sends file data representing chunks to a caller.
type Sender interface {
	Send(data []byte) error
}

// Cache is the subset of template.Cache the peerserver needs.
type Cache interface {
	LookupDiff(buildID string, diffType build.DiffType) (build.Diff, bool)
	// LookupPinned returns a cached template with a pin the caller must
	// release once it stops reading the template.
	LookupPinned(ctx context.Context, buildID string) (tmpl.Template, func(), bool)
}

// BlobSource serves whole-file reads and existence checks (snapfile, metadata, headers).
type BlobSource interface {
	Stream(ctx context.Context, sender Sender) error
	Exists(ctx context.Context) (bool, error)
}

// SeekableSource serves random-access reads with offset/length and size queries (memfile, rootfs).
// The requests need to be aligned to the block size.
type SeekableSource interface {
	Stream(ctx context.Context, offset, length int64, sender Sender) error
	Size(ctx context.Context) (int64, error)
}
