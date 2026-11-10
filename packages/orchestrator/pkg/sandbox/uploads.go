//go:build linux

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerclient"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var (
	errUploadInFlight  = errors.New("upload already in flight for build")
	ErrBuildNotInCache = errors.New("build not in template cache")
)

const (
	// futureTTL must outlive a parent upload's full retry window so a child's
	// in-memory Wait still finds the parent's future. Keep >= the upload retry
	// budget (server.uploadTotalBudget, 2h).
	futureTTL = 3 * time.Hour

	// refreshHeaderBudget bounds how long an upload Wait polls remote storage
	// for a parent's V4 header. Crosses orchestrators: A may still be uploading
	// on a remote orch when B's runV4 calls Wait(A) here. It must be >= the
	// parent's full retry window (server.uploadTotalBudget, 2h); otherwise the
	// poll's budget expiry returns a non-retryable "object does not exist" and
	// the child gives up while the parent is still retrying. The per-attempt
	// context (server.uploadTimeout) bounds the actual poll duration.
	refreshHeaderBudget = 2 * time.Hour

	// uploadDoneChannelPrefix is the Redis pub/sub channel prefix for per-build
	// upload-finished signals. Empty payload = success; non-empty = upload error.
	uploadDoneChannelPrefix = "orchestrator.upload.done." // followed by buildID String
)

type templateLookup interface {
	LookupPinned(ctx context.Context, buildID string) (template.Template, func(), bool)
	WasReleased(buildID string) bool
}

// Uploads is the in-flight upload table. Each entry's future fires when its
// build's V4 header has been swapped, gating child layers that depend on it.
//
// Cross-orch coordination uses Redis pub/sub on per-build channels: the
// uploader publishes on Finish, consumers subscribe inside Wait while polling
// remote storage. The Redis client is optional — nil falls back to ticker-only
// polling.
type Uploads struct {
	tc          templateLookup
	persistence storage.StorageProvider
	p2p         peerclient.Resolver
	redis       redis.UniversalClient
	ff          *featureflags.Client

	futures *ttlcache.Cache[uuid.UUID, *utils.ErrorOnce]
}

func NewUploads(tc *template.Cache, persistence storage.StorageProvider, p2p peerclient.Resolver, redisClient redis.UniversalClient, ff *featureflags.Client) *Uploads {
	futures := ttlcache.New(
		ttlcache.WithTTL[uuid.UUID, *utils.ErrorOnce](futureTTL),
	)
	go futures.Start()

	return &Uploads{tc: tc, persistence: persistence, p2p: p2p, redis: redisClient, ff: ff, futures: futures}
}

func (u *Uploads) Stop() {
	u.futures.Stop()
}

// Start replaces a finished future at the same key; rejects an in-flight one.
// Build IDs are unique per upload so concurrent Starts for the same key are
// not expected — the in-flight check only guards against accidental misuse.
func (u *Uploads) Start(buildID uuid.UUID) (*utils.ErrorOnce, error) {
	if existing := u.futures.Get(buildID); existing != nil {
		select {
		case <-existing.Value().Done():
		default:
			return nil, fmt.Errorf("%w: %s", errUploadInFlight, buildID)
		}
	}

	fut := utils.NewErrorOnce()
	u.futures.Set(buildID, fut, ttlcache.DefaultTTL)

	return fut, nil
}

// AncestorVerdict says which of Wait's paths resolved an ancestor.
type AncestorVerdict string

const (
	// verdictEntry: the header came from the local cache entry, after any wait
	// on the ancestor's upload future.
	verdictEntry AncestorVerdict = "entry"
	// verdictNoFuture: no local entry, no upload future and no peer mid-upload.
	verdictNoFuture AncestorVerdict = "no_future"
	// verdictFutureNoEntry: the upload future fired successfully but the local
	// cache entry is gone. The caller keeps an entry the child already carries
	// and otherwise heals from the stored header without LoadHeader's backfill.
	verdictFutureNoEntry AncestorVerdict = "future_no_entry"
	// verdictP2PPoll: the header was polled from remote storage while a peer,
	// or a local entry still pending its upload, finished it.
	verdictP2PPoll AncestorVerdict = "p2p_poll"
	// verdictError: Wait failed, and so does the child's upload.
	verdictError AncestorVerdict = "error"
)

// Wait returns the parent's post-upload header, or a nil header when the
// ancestor is not in the local cache and no peer is mid-upload — the caller
// usually carries its BuildData through srcHeader.Builds already, and
// appendAncestorBuilds recovers it from the build's stored header otherwise.
// The verdict names the path that produced the result. A future that fired
// successfully for an entry that is gone fails the wait unless
// SnapshotCacheAncestorStorageFallbackFlag is on, in which case it returns a
// nil header with verdictFutureNoEntry. A build this node's template cache
// released returns verdictFutureNoEntry there whatever the flag reads.
func (u *Uploads) Wait(ctx context.Context, buildID uuid.UUID, t build.DiffType) (*header.Header, AncestorVerdict, error) {
	h, verdict, _, err := u.wait(ctx, buildID, t)

	return h, verdict, err
}

// wait is Wait, also reporting whether this node's template cache released
// buildID. The release record is asked on every wait that misses the entry,
// before any verdict is taken, so it lives as long as the entry it replaced:
// that entry would have been touched by this lookup on whichever branch it
// took.
func (u *Uploads) wait(ctx context.Context, buildID uuid.UUID, t build.DiffType) (_ *header.Header, _ AncestorVerdict, released bool, _ error) {
	ctx, span := tracer.Start(ctx, "wait-for-parent-upload", trace.WithAttributes(
		telemetry.WithBuildID(buildID.String()),
		attribute.String("file_type", string(t)),
	))
	defer span.End()

	// The ancestor's entry stays pinned for the whole wait: the device read
	// below can come after a wait on the upload future or a remote poll.
	d, release, err := u.find(ctx, buildID, t)
	defer release()
	if err != nil && !errors.Is(err, ErrBuildNotInCache) {
		logger.L().Warn(ctx, "ancestor resolution failed from cached template",
			logger.WithBuildID(buildID.String()),
			zap.String("file_type", string(t)),
			zap.Error(err),
		)

		return nil, verdictError, false, err
	}

	released = d == nil && u.releasedHere(buildID)

	if item := u.futures.Get(buildID); item != nil {
		if err := item.Value().WaitWithContext(ctx); err != nil {
			return nil, verdictError, released, fmt.Errorf("wait for upload %s: %w", buildID, err)
		}
		if d == nil {
			// A build this node released has no entry by design: it heals
			// from storage whatever the fallback flag reads here, since the
			// release was decided under the flags as they read then.
			if released || u.ancestorStorageFallback(ctx) {
				return nil, verdictFutureNoEntry, released, nil
			}

			return nil, verdictError, released, fmt.Errorf("future fired but build %s not in template cache", buildID)
		}

		return d.Header(), verdictEntry, released, nil
	}

	if d != nil && !d.Header().IncompletePendingUpload {
		return d.Header(), verdictEntry, released, nil
	}

	if d == nil && !u.p2p.IsActive(buildID.String()) {
		return nil, verdictNoFuture, released, nil
	}

	// P2P mid-upload. Poll remote storage, then swap onto the local device.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	h, err := build.PollRemoteStorageForHeader(ctx, u.persistence, buildID, t, u.subscribe(ctx, buildID), refreshHeaderBudget)
	if err != nil {
		return nil, verdictError, released, err
	}
	if d != nil {
		d.SwapHeader(h)
	}

	return h, verdictP2PPoll, released, nil
}

// releasedHere reports whether this node's template cache released buildID's
// entry, and refreshes that record as the entry's own lookup would have.
func (u *Uploads) releasedHere(buildID uuid.UUID) bool {
	return u.tc != nil && u.tc.WasReleased(buildID.String())
}

// ancestorStorageFallback reads SnapshotCacheAncestorStorageFallbackFlag at
// the wait it decides; a nil client resolves to the flag's fallback.
func (u *Uploads) ancestorStorageFallback(ctx context.Context) bool {
	if u.ff == nil {
		return featureflags.SnapshotCacheAncestorStorageFallbackFlag.Fallback()
	}

	return u.ff.BoolFlag(ctx, featureflags.SnapshotCacheAncestorStorageFallbackFlag)
}

// find returns buildID's cached device of type t, with the release of the pin
// held on its template. release is never nil and must be called on every
// path, errors included, once the caller stops reading the device.
func (u *Uploads) find(ctx context.Context, buildID uuid.UUID, t build.DiffType) (block.ReadonlyDevice, func(), error) {
	tpl, release, ok := u.tc.LookupPinned(ctx, buildID.String())
	if !ok {
		return nil, release, fmt.Errorf("build %s: %w", buildID, ErrBuildNotInCache)
	}

	var (
		dev block.ReadonlyDevice
		err error
	)
	switch t {
	case build.Memfile:
		dev, err = tpl.Memfile(ctx)
	case build.Rootfs:
		dev, err = tpl.Rootfs()
	default:
		err = fmt.Errorf("unsupported file type: %s", t)
	}

	return dev, release, err
}

// --- Cross-orch upload-done signaling (Redis pub/sub on per-build channels) ---

func uploadDoneChannel(buildID uuid.UUID) string {
	return uploadDoneChannelPrefix + buildID.String()
}

// publishUploadDoneToRedis broadcasts an upload-finished signal so cross-orch waiters can stop
// polling. Best-effort; failures fall through to the ticker poll. Empty
// payload = success; non-empty = the upload error message.
func (u *Uploads) publishUploadDoneToRedis(ctx context.Context, buildID uuid.UUID, uploadErr error) {
	if u.redis == nil {
		return
	}

	payload := ""
	if uploadErr != nil {
		payload = uploadErr.Error()
	}

	if err := u.redis.Publish(ctx, uploadDoneChannel(buildID), payload).Err(); err != nil {
		logger.L().Warn(ctx, "failed to publish upload-done signal",
			logger.WithBuildID(buildID.String()),
			zap.Error(err),
		)
	}
}

// subscribe opens a per-call SUBSCRIBE on buildID's upload-done channel and
// returns a channel that fires once with the upload outcome. The subscription
// is torn down when ctx cancels (caller must use a derived context). Returns
// a nil channel when Redis is not configured — nil channels never fire, so
// LoadV4 cleanly degrades to ticker-only polling.
func (u *Uploads) subscribe(ctx context.Context, buildID uuid.UUID) <-chan error {
	if u.redis == nil {
		return nil
	}

	out := make(chan error, 1)

	go func() {
		ps := u.redis.Subscribe(ctx, uploadDoneChannel(buildID))
		defer ps.Close()

		msg, err := ps.ReceiveMessage(ctx)
		if err != nil {
			return // ctx cancelled or connection error: silent (ticker covers)
		}

		var uploadErr error
		if msg.Payload != "" {
			uploadErr = errors.New(msg.Payload)
		}

		select {
		case out <- uploadErr:
		case <-ctx.Done():
		}
	}()

	return out
}
