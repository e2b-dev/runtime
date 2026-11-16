package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// recordRef names a stored record by the team that owns it and its sandbox.
type recordRef struct {
	TeamID, SandboxID string
}

// readRef is a ref the readers checked: keys can be built from its IDs, and
// its team is a UUID.
type readRef struct {
	ref  recordRef
	team uuid.UUID
}

// fetchRecords reads the records of refs: one pipeline of single GETs on
// their keys, each routed to its own slot on a cluster. A ref is found when
// its record decodes and names the ref's team, and absent when the GET
// answered nil. A ref the readers cannot use, or whose record exists but
// cannot be given to the ref's team, is in neither: no key can be built
// from its IDs, its team is not a UUID, its record does not decode, its
// record is another team's, or its record names another sandbox than its
// key. Each is logged once per call, and the callers leave it alone. Any
// failure aborts the whole read with nothing classified. The GETs are
// recorded once per call.
func (s *Storage) fetchRecords(ctx context.Context, refs []recordRef) (found map[recordRef]sandboxtypes.Sandbox, absent []recordRef, err error) {
	reads := readableRefs(ctx, refs)
	if len(reads) == 0 {
		return nil, nil, nil
	}

	defer s.listGets.Record(ctx, int64(len(reads)))

	found = make(map[recordRef]sandboxtypes.Sandbox, len(reads))
	missing, err := s.getRecords(ctx, reads, found)
	if err != nil {
		return nil, nil, err
	}
	for _, r := range missing {
		absent = append(absent, r.ref)
	}

	return found, absent, nil
}

// readableRefs returns the refs the readers can use and logs the others:
// index and expiration members and node reports reach the readers
// unchecked, so a ref no key can be built from, or whose team is not a
// UUID, is dropped here instead of left to the key builders' panic.
func readableRefs(ctx context.Context, refs []recordRef) []readRef {
	reads := make([]readRef, 0, len(refs))
	for _, ref := range refs {
		// uuid.Parse admits braced and URN forms, so both checks stay.
		team, err := uuid.Parse(ref.TeamID)
		if err != nil || !validID(ref.TeamID) || !validID(ref.SandboxID) {
			logger.L().Warn(ctx, "Skipping sandbox record ref no key can be built from",
				zap.Error(err),
				logger.WithTeamID(ref.TeamID),
				logger.WithSandboxID(ref.SandboxID),
			)

			continue
		}

		reads = append(reads, readRef{ref: ref, team: team})
	}

	return reads
}

// getRecords GETs the records of reads through one pipeline, decodes the
// values into found, and returns the reads the GET answered nil for. Exec
// reports only the first error, which a nil ahead of a failure would be, so
// every reply is classified on its own: a value, a nil, or a failure, which
// ends the read. A value the readers cannot give to the ref's team is left
// out of found.
func (s *Storage) getRecords(ctx context.Context, reads []readRef, found map[recordRef]sandboxtypes.Sandbox) (missing []readRef, err error) {
	pipe := s.redisClient.Pipeline()
	cmds := make([]*redis.StringCmd, len(reads))
	for i, r := range reads {
		cmds[i] = pipe.Get(ctx, recordKey(r.ref.SandboxID))
	}
	// An error that is no reply, the connection, a redirect that ran out, or
	// a hook that never ran the commands, ends the read here, before the
	// untouched empty values could be decoded. A reply refusing one GET, a
	// key of another type, concerns that command's sandbox alone.
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) && !refusedProbe(err) {
		recordRedisError(ctx, err)

		return nil, fmt.Errorf("failed to read sandbox records: %w", err)
	}

	for i, cmd := range cmds {
		r := reads[i]
		data, err := cmd.Bytes()
		if errors.Is(err, redis.Nil) {
			missing = append(missing, r)

			continue
		}
		if refusedProbe(err) {
			// Skipped, not absent. Counted as an error of its kind; warned,
			// not errored: every pass re-reads the same key.
			recordRedisError(ctx, err)
			logger.L().Warn(ctx, "Skipping sandbox record whose key the server refused to read",
				zap.Error(err),
				logger.WithTeamID(r.ref.TeamID),
				logger.WithSandboxID(r.ref.SandboxID),
			)

			continue
		}
		if err != nil {
			recordRedisError(ctx, err)

			return nil, fmt.Errorf("failed to read the record of sandbox %s: %w", r.ref.SandboxID, err)
		}
		if sbx, ok := s.decodeRecord(ctx, r, data); ok {
			found[r.ref] = sbx
		}
	}

	return missing, nil
}

// decodeRecord decodes data, the record read for r, and reports whether the
// readers can give it to r's team. A value that does not decode, a record of
// another team than the ref's, which the key cannot tell apart,
// or a record naming another sandbox than its key, from whose ID the callers
// would build keys, is logged and left out. Teams are compared as UUIDs,
// never as strings.
func (s *Storage) decodeRecord(ctx context.Context, r readRef, data []byte) (sandboxtypes.Sandbox, bool) {
	var sbx sandboxtypes.Sandbox
	if err := json.Unmarshal(data, &sbx); err != nil {
		s.scanMet.corruptRecords.Add(ctx, 1)
		// Warn, not Error: every scan pass re-reads the same record, so a
		// single corrupt one would otherwise page on a loop.
		logger.L().Warn(ctx, "Skipping sandbox record that does not decode",
			zap.Error(err),
			logger.WithTeamID(r.ref.TeamID),
			logger.WithSandboxID(r.ref.SandboxID),
		)

		return sandboxtypes.Sandbox{}, false
	}
	if sbx.TeamID != r.team {
		logger.L().Warn(ctx, "Skipping sandbox record of another team",
			logger.WithTeamID(r.ref.TeamID),
			logger.WithSandboxID(r.ref.SandboxID),
			zap.String("record_team_id", sbx.TeamID.String()),
		)

		return sandboxtypes.Sandbox{}, false
	}
	// One compare covers an empty, a braced and a wrong ID: every key a
	// caller builds for the record comes from the payload's.
	if sbx.SandboxID != r.ref.SandboxID {
		logger.L().Warn(ctx, "Skipping sandbox record naming another sandbox than its key",
			logger.WithTeamID(r.ref.TeamID),
			logger.WithSandboxID(r.ref.SandboxID),
			zap.String("record_sandbox_id", sbx.SandboxID),
		)

		return sandboxtypes.Sandbox{}, false
	}

	return sbx, true
}
