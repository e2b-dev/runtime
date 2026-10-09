package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var errOwnershipLost = errors.New("idempotency reservation ownership lost")

type recordState string

const (
	// This request reserved the key and may execute the operation.
	stateOwner recordState = "owner"
	// The request matches, with no recorded response yet and time left in its response window.
	statePending recordState = "pending"
	// No response was recorded by the expected deadline, or the deadline is unknown.
	stateOutcomeUnknown recordState = "outcome_unknown"
	// The request matches a recorded response, including errors, which can be replayed.
	stateComplete recordState = "complete"
	// The key already belongs to a different request fingerprint.
	stateMismatch recordState = "mismatch"
)

type cachedResponse struct {
	Status  int         `json:"status"`
	Body    []byte      `json:"body"`
	Headers http.Header `json:"headers,omitempty"`
}

type record struct {
	state    recordState
	response cachedResponse
}

type reservationResult struct {
	record record
	err    error
}

type store interface {
	reserve(ctx context.Context, key, fingerprint, owner string, retentionSeconds int, responseDeadline time.Time) (record, error)
	complete(ctx context.Context, key, owner string, result cachedResponse) error
	release(ctx context.Context, key, owner string) error
}

type redisStore struct {
	client redis.UniversalClient
}

var reserveScript = redis.NewScript(`
local now = redis.call('TIME')
local nowMillis = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
if redis.call('EXISTS', KEYS[1]) == 0 then
    local responseDeadline = nowMillis + tonumber(ARGV[4])
    redis.call('HSET', KEYS[1], 'fingerprint', ARGV[1], 'owner', ARGV[2],
        'expected_response_at', string.format('%.0f', responseDeadline))
    -- Expiry errors must not leave a reservation without a TTL.
    local expiry = redis.pcall('EXPIRE', KEYS[1], ARGV[3])
    if type(expiry) == 'table' and expiry.err then
        redis.call('DEL', KEYS[1])
        return expiry
    end
    return {'true', '', '', '', ''}
end
local values = redis.call('HMGET', KEYS[1], 'fingerprint', 'response', 'expected_response_at')
return {'false', values[1] or '', values[2] or '', values[3] or '', string.format('%.0f', nowMillis)}
`)

var completeScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1] then
    return 0
end
return redis.call('HSETNX', KEYS[1], 'response', ARGV[2])
`)

var releaseScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'owner') == ARGV[1] and redis.call('HEXISTS', KEYS[1], 'response') == 0 then
    return redis.call('DEL', KEYS[1])
end
return 0
`)

func recordKey(teamID, key string) string {
	digest := sha256.Sum256([]byte(key))

	return "api:idempotency:" + teamID + ":" + hex.EncodeToString(digest[:])
}

func (s *redisStore) reserve(ctx context.Context, key, fingerprint, owner string, retentionSeconds int, responseDeadline time.Time) (record, error) {
	if err := ctx.Err(); err != nil {
		return record{}, err
	}
	if retentionSeconds <= 0 {
		return record{}, errors.New("invalid idempotency retention")
	}
	// Redis stores the deadline and returns server time for classification in Go.
	remainingMillis := max(time.Until(responseDeadline).Milliseconds(), 0)
	// An unbuffered handoff leaves cleanup with the worker if the caller has returned.
	done := make(chan reservationResult)
	go func() {
		values, err := reserveScript.Run(ctx, s.client, []string{key}, fingerprint, owner, retentionSeconds, remainingMillis).StringSlice()
		result := reservationResult{err: err}
		if err == nil {
			result.record, result.err = decodeRecord(values, fingerprint)
		}
		if result.err != nil {
			releaseUnusedReservation(ctx, s, key, owner)
		}
		select {
		case done <- result:
		case <-ctx.Done():
			if result.err == nil && result.record.state == stateOwner {
				releaseUnusedReservation(ctx, s, key, owner)
			}
		}
	}()

	select {
	case <-ctx.Done():
		return record{}, ctx.Err()
	case result := <-done:
		return result.record, result.err
	}
}

func decodeRecord(values []string, fingerprint string) (record, error) {
	if len(values) != 5 {
		return record{}, errors.New("invalid idempotency record")
	}
	created, err := strconv.ParseBool(values[0])
	if err != nil {
		return record{}, fmt.Errorf("decode idempotency reservation: %w", err)
	}
	if created {
		return record{state: stateOwner}, nil
	}
	if values[1] != fingerprint {
		return record{state: stateMismatch}, nil
	}
	if values[2] != "" {
		result := record{state: stateComplete}
		if err := json.Unmarshal([]byte(values[2]), &result.response); err != nil {
			return record{}, fmt.Errorf("decode idempotency response: %w", err)
		}

		return result, nil
	}
	responseDeadline, err := strconv.ParseInt(values[3], 10, 64)
	if err != nil {
		return record{state: stateOutcomeUnknown}, nil //nolint:nilerr // Missing or unreadable deadlines mean the outcome is unknown.
	}
	nowMillis, err := strconv.ParseInt(values[4], 10, 64)
	if err != nil {
		return record{}, fmt.Errorf("decode Redis timestamp: %w", err)
	}
	if nowMillis >= responseDeadline {
		return record{state: stateOutcomeUnknown}, nil
	}

	return record{state: statePending}, nil
}

func (s *redisStore) complete(ctx context.Context, key, owner string, result cachedResponse) error {
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	cmd, err := s.run(ctx, completeScript, key, owner, encoded)
	if err != nil {
		return err
	}
	stored, err := cmd.Int()
	if err != nil {
		return err
	}
	if stored != 1 {
		return errOwnershipLost
	}

	return nil
}

func (s *redisStore) release(ctx context.Context, key, owner string) error {
	_, err := s.run(ctx, releaseScript, key, owner)

	return err
}

func (s *redisStore) run(ctx context.Context, script *redis.Script, key string, args ...any) (*redis.Cmd, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The shared Redis client can ignore context deadlines during socket I/O.
	done := make(chan *redis.Cmd, 1)
	go func() { done <- script.Run(ctx, s.client, []string{key}, args...) }()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case cmd := <-done:
		return cmd, cmd.Err()
	}
}
