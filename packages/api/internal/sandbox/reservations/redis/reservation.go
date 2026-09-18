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
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	resultTTL = 30 * time.Second

	// fallbackPollInterval is how often the waiter re-checks Redis when no
	// PubSub wakeup arrives
	fallbackPollInterval = 1 * time.Second

	// staleTTL is the maximum age of a pending entry before it is considered stale
	// and cleaned up. This handles the case where an API instance crashes mid-creation.
	// 90 seconds is well beyond any realistic sandbox creation time.
	staleTTL = 90 * time.Second

	// killClaimTTL is how long a DELETE's kill-claim over a sandbox ID blocks a
	// resume's reservation. It only has to outlive the snapshot soft-delete
	// becoming durable — after that any resume fails when it fetches the
	// snapshot — so it is kept short. The claim is normally cleared explicitly
	// (ReleaseKillClaim) if the delete fails; this TTL is the backstop for a
	// crashed API instance.
	killClaimTTL = 30 * time.Second
)

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
	staleCutoff := float64(time.Now().Add(-staleTTL).Unix())

	killClaimKey := getKillClaimKey(teamIDStr, sandboxID)

	result, err := reserveScript.Run(ctx, s.redisClient,
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

// ClaimKill fences off a paused sandbox's ID against a concurrent resume before
// the caller soft-deletes its snapshot. It returns claimed=true when no resume
// is in flight and the caller may proceed with the delete; false when a resume
// is pending or the sandbox is already running again, in which case the caller
// must not delete the snapshot and should surface a retryable conflict.
//
// See claimKillScript for the ordering argument that makes an accepted kill
// irreversible against resume's lockless publication.
func (s *ReservationStorage) ClaimKill(ctx context.Context, teamID uuid.UUID, sandboxID string) (claimed bool, err error) {
	teamIDStr := teamID.String()
	storageIndexKey := getStorageIndexKey(teamIDStr)
	pendingSetKey := getPendingSetKey(teamIDStr)
	killClaimKey := getKillClaimKey(teamIDStr, sandboxID)

	result, err := claimKillScript.Run(ctx, s.redisClient,
		[]string{storageIndexKey, pendingSetKey, killClaimKey},
		sandboxID, int(killClaimTTL.Seconds()),
	).Int()
	if err != nil {
		return false, fmt.Errorf("failed to run claim-kill script: %w", err)
	}

	switch result {
	case claimKillResultClaimed:
		return true, nil
	case claimKillResultInFlight:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected claim-kill script result: %d", result)
	}
}

// ReleaseKillClaim drops a claim taken by ClaimKill. It is best-effort cleanup
// for when the snapshot delete fails after the claim was taken; the claim's TTL
// is the backstop if this never runs.
func (s *ReservationStorage) ReleaseKillClaim(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	killClaimKey := getKillClaimKey(teamID.String(), sandboxID)

	err := releaseKillClaimScript.Run(ctx, s.redisClient, []string{killClaimKey}).Err()
	if err != nil {
		return fmt.Errorf("failed to run release-kill-claim script: %w", err)
	}

	return nil
}

func (s *ReservationStorage) Release(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	teamIDStr := teamID.String()
	pendingSetKey := getPendingSetKey(teamIDStr)
	resultKeyStr := getResultKey(teamIDStr, sandboxID)

	err := releaseScript.Run(ctx, s.redisClient,
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
		err := finishStartScript.Run(bgCtx, s.redisClient,
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
