package management

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.uber.org/zap"

	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const teamSlugUniqueConstraint = "teams_slug_unique"

var (
	// ErrProjectSlugTaken reports a slug already held on this cluster, possibly
	// by a project the caller has never heard of.
	ErrProjectSlugTaken = errors.New("project slug is already taken")

	// ErrProjectRaced reports an id inserted between the existence check and
	// this request's own insert.
	ErrProjectRaced = errors.New("project was created concurrently")
)

// Project is the set of properties the caller synchronizes. It sends all of
// them on every push, so a reconcile is a complete statement rather than a
// patch.
//
// Slug moves like the others, and the project's template aliases follow it in
// the same transaction: each one held under an earlier slug is also given under
// the new one. The old names stay, because rewriting them would break every
// reference a user has already scripted.
type Project struct {
	ClusterID *uuid.UUID
	ID        uuid.UUID
	Name      string
	Slug      string
	Email     string
}

// UpsertProject creates a project or reconciles an existing one, reporting
// which happened.
//
// One operation serves both because the caller cannot tell them apart: it
// retries, and a retry after a response it never saw has to land on the same
// state as the original.
func (s *Service) UpsertProject(ctx context.Context, project Project) (stored Project, created bool, err error) {
	stored, created, err = s.writeProject(ctx, project)
	if err != nil {
		return Project{}, false, err
	}

	// A reconcile changes the team, so every cached copy of it is stale — its
	// own entry, each API key's, each member's. A create has nothing cached.
	// Logged rather than returned: the row is committed, and a retry cannot
	// improve on a stale cache.
	if !created {
		if err := s.cache.InvalidateTeamCache(ctx, project.ID); err != nil {
			logger.L().Error(ctx, "invalidating team cache after project reconcile",
				logger.WithTeamID(project.ID.String()), zap.Error(err))
		}
	}

	return stored, created, nil
}

// writeProject branches on whether the project exists: a create inserts the
// row, a reconcile writes only what the caller synchronizes and follows a slug
// change with the template aliases.
func (s *Service) writeProject(ctx context.Context, project Project) (stored Project, created bool, err error) {
	txDB, tx, err := s.db.WithTx(ctx)
	if err != nil {
		return Project{}, false, fmt.Errorf("start project upsert transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	currentSlug, err := txDB.LockManagedTeam(ctx, project.ID)

	switch {
	case err == nil:
		stored, err = reconcileProject(ctx, txDB, project, currentSlug)
	case dberrors.IsNotFoundError(err):
		stored, err = createProject(ctx, txDB, project)
		created = true
	default:
		return Project{}, false, fmt.Errorf("look up project: %w", err)
	}

	if err != nil {
		return Project{}, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Project{}, false, fmt.Errorf("commit project upsert: %w", err)
	}

	return stored, created, nil
}

func createProject(ctx context.Context, txDB *authqueries.Queries, project Project) (Project, error) {
	inserted, err := txDB.InsertManagedTeam(ctx, authqueries.InsertManagedTeamParams{
		ClusterID: project.ClusterID,
		ID:        project.ID,
		Name:      project.Name,
		Slug:      project.Slug,
		Email:     project.Email,
	})

	switch {
	// ON CONFLICT DO NOTHING yields no row, which here means another request
	// inserted this id between the lock finding nothing and this statement.
	case dberrors.IsNotFoundError(err):
		return Project{}, ErrProjectRaced
	case dberrors.ConstraintName(err) == teamSlugUniqueConstraint:
		return Project{}, fmt.Errorf("%w: %q", ErrProjectSlugTaken, project.Slug)
	case err != nil:
		return Project{}, fmt.Errorf("create project: %w", err)
	}

	return Project{ClusterID: inserted.ClusterID, ID: project.ID, Name: inserted.Name, Slug: inserted.Slug, Email: inserted.Email}, nil
}

func reconcileProject(ctx context.Context, txDB *authqueries.Queries, project Project, currentSlug string) (Project, error) {
	updated, err := txDB.UpdateManagedTeam(ctx, authqueries.UpdateManagedTeamParams{
		ClusterID: project.ClusterID,
		ID:        project.ID,
		Name:      project.Name,
		Slug:      project.Slug,
		Email:     project.Email,
	})

	switch {
	case dberrors.ConstraintName(err) == teamSlugUniqueConstraint:
		return Project{}, fmt.Errorf("%w: %q", ErrProjectSlugTaken, project.Slug)
	case err != nil:
		return Project{}, fmt.Errorf("reconcile project: %w", err)
	}

	if updated.Slug != currentSlug {
		if err := copyAliasesToSlug(ctx, txDB, project.ID, currentSlug, updated.Slug); err != nil {
			return Project{}, err
		}
	}

	return Project{ClusterID: updated.ClusterID, ID: project.ID, Name: updated.Name, Slug: updated.Slug, Email: updated.Email}, nil
}

func copyAliasesToSlug(ctx context.Context, txDB *authqueries.Queries, projectID uuid.UUID, previousSlug, slug string) error {
	skipped, err := txDB.CopyTeamAliasesToNamespace(ctx, authqueries.CopyTeamAliasesToNamespaceParams{
		Namespace:         slug,
		TeamID:            projectID,
		PreviousNamespace: previousSlug,
	})
	if err != nil {
		return fmt.Errorf("copy template aliases to the new slug: %w", err)
	}

	if len(skipped) > 0 {
		logger.L().Warn(ctx, "template aliases not copied to the new slug, which gives them to another template",
			logger.WithTeamID(projectID.String()), zap.String("slug", slug), zap.Strings("aliases", skipped))
	}

	return nil
}
