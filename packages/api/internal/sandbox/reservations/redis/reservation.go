package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	storage_redis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	resultTTL = 30 * time.Second

	// fallbackPollInterval is how often the waiter re-checks Redis when no
	// PubSub wakeup arrives
	fallbackPollInterval = 1 * time.Second

	// StaleTTL is the maximum age of a pending entry before it is considered stale
	// and cleaned up. This handles the case where an API instance crashes mid-creation.
	// 90 seconds is well beyond any realistic sandbox creation time.
	StaleTTL = 90 * time.Second

	// KillClaimTTL only has to outlive the snapshot delete: once the snapshot
	// is gone, a resume fails on its own.
	KillClaimTTL = 30 * time.Second
)

// ReapableAt is the earliest instant another reserver can purge a pending
// entry reserved at reservedAt: the entry's score and the purge cutoff are
// whole seconds and the purge is inclusive. Work that runs on for a caller
// that left (a resume create and its bookkeeping) must end before it.
func ReapableAt(reservedAt time.Time) time.Time {
	return reservedAt.Truncate(time.Second).Add(StaleTTL)
}

var _ sandboxtypes.ReservationStorage = (*ReservationStorage)(nil)

// Publish is fire-and-forget — drops on queue saturation are recovered by the waiter's fallback ticker.
type Notifier interface {
	Subscribe(routingKey string) (<-chan struct{}, func())
	Publish(ctx context.Context, routingKey string)
}

type ReservationStorage struct {
	redisClient redis.UniversalClient
	notifier    Notifier
}

func NewReservationStorage(redisClient redis.UniversalClient, notifier Notifier) *ReservationStorage {
	return &ReservationStorage{
		redisClient: redisClient,
		notifier:    notifier,
	}
}

func (s *ReservationStorage) Reserve(ctx context.Context, teamID uuid.UUID, sandboxID string, limit int) (finishStart func(sandboxtypes.Sandbox, error), waitForStart func(ctx context.Context) (sandboxtypes.Sandbox, error), err error) {
	teamIDStr := teamID.String()
	storageIndexKey := getStorageIndexKey(teamIDStr)
	pendingSetKey := getPendingSetKey(teamIDStr)
	resultKeyStr := getResultKey(teamIDStr, sandboxID)

	now := float64(time.Now().Unix())
	staleCutoff := float64(time.Now().Add(-StaleTTL).Unix())

	killClaimKey := getKillClaimKey(teamIDStr, sandboxID)

	result, err := storage_redis.RunScript(ctx, s.redisClient, "reserve", reserveScript,
		[]string{storageIndexKey, pendingSetKey, resultKeyStr, killClaimKey},
		sandboxID, limit, now, staleCutoff,
	).Int()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to run reserve script: %w", err)
	}

	switch result {
	case reserveResultReserved:
		return s.createFinishStart(ctx, teamID, sandboxID), nil, nil

	case reserveResultAlreadyInStorage:
		return nil, nil, sandboxtypes.ErrAlreadyExists

	case reserveResultAlreadyPending:
		return nil, s.createWaitForStart(teamID, sandboxID), nil

	case reserveResultLimitExceeded:
		return nil, nil, &sandboxtypes.LimitExceededError{TeamID: teamID}

	case reserveResultKilled:
		return nil, nil, sandboxtypes.ErrSandboxKilled

	default:
		return nil, nil, fmt.Errorf("unexpected reserve script result: %d", result)
	}
}

// ClaimKill blocks new reservations of a paused sandbox while its snapshot is
// deleted. It returns claimed=false when a resume is in flight or the sandbox
// is running again; otherwise claimID is what ReleaseKillClaim takes.
func (s *ReservationStorage) ClaimKill(ctx context.Context, teamID uuid.UUID, sandboxID string) (claimID string, claimed bool, err error) {
	teamIDStr := teamID.String()
	storageIndexKey := getStorageIndexKey(teamIDStr)
	pendingSetKey := getPendingSetKey(teamIDStr)
	killClaimKey := getKillClaimKey(teamIDStr, sandboxID)
	staleCutoff := float64(time.Now().Add(-StaleTTL).Unix())
	claimID = uuid.NewString()

	result, err := storage_redis.RunScript(ctx, s.redisClient, "claim_kill", claimKillScript,
		[]string{storageIndexKey, pendingSetKey, killClaimKey},
		sandboxID, int(KillClaimTTL.Seconds()), staleCutoff, claimID,
	).Int()
	if err != nil {
		return "", false, fmt.Errorf("failed to run claim-kill script: %w", err)
	}

	switch result {
	case claimKillResultClaimed:
		return claimID, true, nil
	case claimKillResultInFlight:
		return "", false, nil
	default:
		return "", false, fmt.Errorf("unexpected claim-kill script result: %d", result)
	}
}

func (s *ReservationStorage) ReleaseKillClaim(ctx context.Context, teamID uuid.UUID, sandboxID, claimID string) error {
	killClaimKey := getKillClaimKey(teamID.String(), sandboxID)

	if err := s.redisClient.SRem(ctx, killClaimKey, claimID).Err(); err != nil {
		return fmt.Errorf("failed to release kill claim: %w", err)
	}

	return nil
}

// WaitForStart returns once no start of the sandbox is pending. How that start
// ended is the starter's to report, so only a ctx error is returned.
func (s *ReservationStorage) WaitForStart(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	_, _ = s.createWaitForStart(teamID, sandboxID)(ctx)

	return ctx.Err()
}

func (s *ReservationStorage) Release(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	teamIDStr := teamID.String()
	pendingSetKey := getPendingSetKey(teamIDStr)
	resultKeyStr := getResultKey(teamIDStr, sandboxID)

	err := storage_redis.RunScript(ctx, s.redisClient, "release", releaseScript,
		[]string{pendingSetKey, resultKeyStr},
		sandboxID,
	).Err()
	if err != nil {
		return fmt.Errorf("failed to run release script: %w", err)
	}

	// Wake any in-process waiter so it checks the pending set immediately.
	s.notifier.Publish(ctx, getReservationRoutingKey(teamIDStr, sandboxID))

	return nil
}

// createFinishStart returns a callback that completes the reservation.
func (s *ReservationStorage) createFinishStart(ctx context.Context, teamID uuid.UUID, sandboxID string) func(sandboxtypes.Sandbox, error) {
	return func(sbx sandboxtypes.Sandbox, startErr error) {
		teamIDStr := teamID.String()
		pendingSetKey := getPendingSetKey(teamIDStr)
		resultKeyStr := getResultKey(teamIDStr, sandboxID)
		routingKey := getReservationRoutingKey(teamIDStr, sandboxID)

		bgCtx := context.WithoutCancel(ctx)

		resultData, encodeErr := encodeResult(sbx, startErr)
		if encodeErr != nil {
			logger.L().Error(ctx, "failed to encode reservation result",
				zap.Error(encodeErr),
				logger.WithSandboxID(sandboxID),
			)

			// Still try to remove from pending even if encoding fails.
			_ = s.redisClient.ZRem(bgCtx, pendingSetKey, sandboxID).Err()

			// Wake waiters so they can observe that the reservation is gone.
			s.notifier.Publish(bgCtx, routingKey)

			return
		}

		ttlSeconds := int(resultTTL.Seconds())
		err := storage_redis.RunScript(bgCtx, s.redisClient, "finish_start", finishStartScript,
			[]string{pendingSetKey, resultKeyStr},
			sandboxID, resultData, ttlSeconds,
		).Err()
		if err != nil {
			logger.L().Error(ctx, "failed to run finishStart script",
				zap.Error(err),
				logger.WithSandboxID(sandboxID),
			)

			return
		}

		// Wake any in-process waiter immediately. Drop-tolerant: the
		// fallback ticker covers a saturated publish queue.
		s.notifier.Publish(bgCtx, routingKey)
	}
}

// createWaitForStart returns a function that polls Redis for the result of a sandbox creation
// initiated by another instance.
func (s *ReservationStorage) createWaitForStart(teamID uuid.UUID, sandboxID string) func(ctx context.Context) (sandboxtypes.Sandbox, error) {
	return func(ctx context.Context) (sandboxtypes.Sandbox, error) {
		teamIDStr := teamID.String()
		resultKeyStr := getResultKey(teamIDStr, sandboxID)
		pendingSetKey := getPendingSetKey(teamIDStr)
		routingKey := getReservationRoutingKey(teamIDStr, sandboxID)

		ch, cleanup := s.notifier.Subscribe(routingKey)
		defer cleanup()

		// Initial probe: the producer may have finished before we subscribed,
		// or we may be a late waiter joining after the result was already set.
		if done, sbx, err := s.tryReadResult(ctx, resultKeyStr, pendingSetKey, sandboxID); done {
			return sbx, err
		}

		ticker := time.NewTicker(fallbackPollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return sandboxtypes.Sandbox{}, ctx.Err()
			case <-ch:
			case <-ticker.C:
			}

			if done, sbx, err := s.tryReadResult(ctx, resultKeyStr, pendingSetKey, sandboxID); done {
				return sbx, err
			}
		}
	}
}

// tryReadResult performs a single probe of the reservation state.
//
// Returns done=true when the wait is over:
//   - the result key holds an encoded terminal result, or
//   - the sandbox vanished from the pending set without a result, or
//   - the Redis call itself failed.
//
// Returns done=false when the reservation is still pending and the caller
// should wait for the next wakeup.
func (s *ReservationStorage) tryReadResult(
	ctx context.Context,
	resultKey, pendingSetKey, sandboxID string,
) (done bool, sbx sandboxtypes.Sandbox, err error) {
	data, getErr := s.redisClient.Get(ctx, resultKey).Bytes()
	if getErr == nil {
		sbx, err = decodeResult(data)

		return true, sbx, err
	}
	if !errors.Is(getErr, redis.Nil) {
		return true, sandboxtypes.Sandbox{}, fmt.Errorf("failed to check result key: %w", getErr)
	}

	// No result yet, so check whether another instance is still creating the sandbox.
	scoreErr := s.redisClient.ZScore(ctx, pendingSetKey, sandboxID).Err()
	if errors.Is(scoreErr, redis.Nil) {
		// Re-read the result in case finishStart or a new Release wrote it
		// between the initial GET and the legacy pending-set check.
		data, getErr = s.redisClient.Get(ctx, resultKey).Bytes()
		if getErr == nil {
			sbx, err = decodeResult(data)

			return true, sbx, err
		}
		if !errors.Is(getErr, redis.Nil) {
			return true, sandboxtypes.Sandbox{}, fmt.Errorf("failed to check result key: %w", getErr)
		}

		return true, sandboxtypes.Sandbox{}, fmt.Errorf("sandbox %s is no longer pending and has no result", sandboxID)
	}
	if scoreErr != nil {
		return true, sandboxtypes.Sandbox{}, fmt.Errorf("failed to check pending set: %w", scoreErr)
	}

	// Still pending, no result yet.
	return false, sandboxtypes.Sandbox{}, nil
}
