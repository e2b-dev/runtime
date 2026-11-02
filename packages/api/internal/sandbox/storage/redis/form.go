package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox/sandboxtypes"
)

// WritesPerSandbox reports whether new records are written in the sandbox
// form.
func (s *Storage) WritesPerSandbox() bool {
	return s.writeForm() == formSandbox
}

// SetPerSandboxWrites selects the sandbox form for new records when enabled
// and the team form otherwise. Existing records keep theirs.
func (s *Storage) SetPerSandboxWrites(enabled bool) {
	form := formTeam
	if enabled {
		form = formSandbox
	}
	s.setWriteForm(form)
}

// writeForm returns the key form new records are written with.
func (s *Storage) writeForm() keyForm {
	return keyForm(s.form.Load())
}

// setWriteForm selects the key form new records are written with. Existing
// records keep theirs.
func (s *Storage) setWriteForm(form keyForm) {
	s.form.Store(int32(form))
}

// resolveForm returns the form of a sandbox's keys: the form of the record
// that exists, or the write form when none does. It runs before any lock is
// taken and is driven by what exists, not by the write form, so replicas
// whose write forms differ still agree on an existing sandbox.
func (s *Storage) resolveForm(ctx context.Context, teamID, sandboxID string) (keyForm, error) {
	form, ok, err := s.formOfExisting(ctx, teamID, sandboxID)
	if err != nil {
		return 0, err
	}
	if !ok {
		return s.writeForm(), nil
	}

	return form, nil
}

// formOfExisting returns the form of the record that exists, the sandbox
// form first, and false when neither form holds one. Unlike resolveForm it
// never falls back to the write form. A sandbox-form key carries no team, so
// that record is read and its owner compared here, before any lock is taken
// on its key: another team's record is sandboxtypes.ErrNotFound, as it is
// after the lock, and the owner's lock is never taken for it.
func (s *Storage) formOfExisting(ctx context.Context, teamID, sandboxID string) (keyForm, bool, error) {
	data, err := s.redisClient.Get(ctx, recordKey(formSandbox, teamID, sandboxID)).Bytes()
	switch {
	case err == nil:
		var stored sandboxtypes.Sandbox
		if json.Unmarshal(data, &stored) == nil && stored.TeamID.String() != teamID {
			return 0, false, fmt.Errorf("sandbox %q: %w", sandboxID, sandboxtypes.ErrNotFound)
		}

		return formSandbox, true, nil
	case !errors.Is(err, redis.Nil):
		return 0, false, fmt.Errorf("failed to probe the record key: %w", err)
	}

	exists, err := s.redisClient.Exists(ctx, recordKey(formTeam, teamID, sandboxID)).Result()
	if err != nil {
		return 0, false, fmt.Errorf("failed to probe the record key: %w", err)
	}
	if exists > 0 {
		return formTeam, true, nil
	}

	return 0, false, nil
}
