package management

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
)

var (
	ErrProjectNotFound            = errors.New("project not found")
	ErrInvalidProjectMember       = errors.New("invalid project member")
	ErrDuplicateProjectIdentity   = errors.New("duplicate project identity")
	ErrProjectIdentityOwnedByUser = errors.New("project identity is linked to another user")
)

type ProjectMemberIdentity struct {
	Issuer  string
	Subject string
}

type ProjectMemberProjection struct {
	ProjectID  uuid.UUID
	UserID     uuid.UUID
	Revision   int64
	Present    bool
	IsDefault  bool
	Identities []ProjectMemberIdentity
}

func (s *Service) ApplyProjectMember(ctx context.Context, projection ProjectMemberProjection) error {
	if err := validateProjectMemberProjection(projection); err != nil {
		return err
	}

	if _, err := s.applyProjectMember(ctx, projection); err != nil {
		return err
	}
	s.cache.InvalidateTeamMemberCache(ctx, projection.UserID, projection.ProjectID.String())

	return nil
}

func (s *Service) applyProjectMember(ctx context.Context, projection ProjectMemberProjection) (bool, error) {
	txDB, tx, err := s.db.WithTx(ctx)
	if err != nil {
		return false, fmt.Errorf("start project member transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if _, err := txDB.LockManagedTeam(ctx, projection.ProjectID); err != nil {
		if dberrors.IsNotFoundError(err) {
			return false, ErrProjectNotFound
		}

		return false, fmt.Errorf("lock project: %w", err)
	}

	applied, err := txDB.ApplyProjectMemberProjection(ctx, authqueries.ApplyProjectMemberProjectionParams{
		ProjectID: projection.ProjectID,
		UserID:    projection.UserID,
		Revision:  projection.Revision,
		Present:   projection.Present,
	})
	if err != nil {
		return false, fmt.Errorf("advance project member projection: %w", err)
	}
	if !applied {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit stale project member projection: %w", err)
		}

		return false, nil
	}

	if projection.Present {
		if err := txDB.UpsertPublicUser(ctx, projection.UserID); err != nil {
			return false, fmt.Errorf("anchor project member: %w", err)
		}
		for _, identity := range projection.Identities {
			ownerID, err := txDB.UpsertPublicIdentity(ctx, authqueries.UpsertPublicIdentityParams{
				OidcIss: identity.Issuer,
				OidcSub: identity.Subject,
				UserID:  projection.UserID,
			})
			if err != nil {
				return false, fmt.Errorf("upsert project identity: %w", err)
			}
			if ownerID != projection.UserID {
				return false, ErrProjectIdentityOwnedByUser
			}
		}
		if err := txDB.UpsertTeamMember(ctx, authqueries.UpsertTeamMemberParams{
			TeamID:    projection.ProjectID,
			UserID:    projection.UserID,
			IsDefault: projection.IsDefault,
		}); err != nil {
			return false, fmt.Errorf("upsert project member: %w", err)
		}
	} else if err := txDB.DeleteTeamMember(ctx, authqueries.DeleteTeamMemberParams{
		TeamID: projection.ProjectID,
		UserID: projection.UserID,
	}); err != nil {
		return false, fmt.Errorf("delete project member: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit project member projection: %w", err)
	}

	return true, nil
}

func validateProjectMemberProjection(projection ProjectMemberProjection) error {
	if projection.ProjectID == uuid.Nil || projection.UserID == uuid.Nil || projection.Revision <= 0 {
		return ErrInvalidProjectMember
	}
	if !projection.Present {
		if len(projection.Identities) > 0 {
			return ErrInvalidProjectMember
		}

		return nil
	}
	if len(projection.Identities) == 0 {
		return ErrInvalidProjectMember
	}

	seenIdentities := make(map[string]struct{}, len(projection.Identities))
	seenIssuers := make(map[string]struct{}, len(projection.Identities))
	for _, identity := range projection.Identities {
		if strings.TrimSpace(identity.Issuer) == "" || strings.TrimSpace(identity.Subject) == "" {
			return ErrInvalidProjectMember
		}

		key := projectIdentityKey(identity)
		if _, exists := seenIdentities[key]; exists {
			return ErrDuplicateProjectIdentity
		}
		seenIdentities[key] = struct{}{}
		if _, exists := seenIssuers[identity.Issuer]; exists {
			return ErrInvalidProjectMember
		}
		seenIssuers[identity.Issuer] = struct{}{}
	}

	return nil
}

func projectIdentityKey(identity ProjectMemberIdentity) string {
	return identity.Issuer + "\x00" + identity.Subject
}
