package testutils

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	testqueries "github.com/e2b-dev/infra/packages/db/pkg/testutils/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
)

// CreateTestTeam creates a test team in the database.
// Uses the default tier 'base_v1' that is created in migrations.
func CreateTestTeam(t *testing.T, db *Database) uuid.UUID {
	t.Helper()
	teamID := uuid.New()
	// Generate a unique slug from the team ID (first 8 chars)
	slug := "test-team-" + teamID.String()[:8]

	err := db.TestQueries.InsertTestTeam(t.Context(), testqueries.InsertTestTeamParams{
		ID:    teamID,
		Name:  "Test Team " + teamID.String(),
		Tier:  "base_v1",
		Email: "test-" + teamID.String() + "@example.com",
		Slug:  slug,
	})
	require.NoError(t, err, "Failed to create test team")

	return teamID
}

// CreateTestTemplate creates a base env (template) row in the database
// (required by foreign key constraints in subsequent test inserts).
func CreateTestTemplate(t *testing.T, db *Database, teamID uuid.UUID) string {
	t.Helper()
	envID := "base-env-" + uuid.New().String()

	err := db.TestQueries.InsertTestEnv(t.Context(), testqueries.InsertTestEnvParams{
		ID:     envID,
		TeamID: teamID,
		Public: true,
		Source: "template",
	})
	require.NoError(t, err, "Failed to create test base env")

	return envID
}

func CreateTestTemplateAlias(t *testing.T, db *Database, templateID string) string {
	t.Helper()
	alias := "alias-" + uuid.New().String()

	// Insert alias without namespace (legacy behavior / promoted templates)
	err := db.SqlcClient.TestsRawSQL(t.Context(),
		"INSERT INTO public.env_aliases (alias, env_id, is_renamable) VALUES ($1, $2, $3)",
		alias, templateID, true,
	)
	require.NoError(t, err, "Failed to create test alias")

	return alias
}

// CreateTestTemplateAliasWithNamespace creates an alias with a specific namespace
func CreateTestTemplateAliasWithNamespace(t *testing.T, db *Database, templateID string, namespace *string) string {
	t.Helper()
	alias := "alias-" + uuid.New().String()

	err := db.SqlcClient.TestsRawSQL(t.Context(),
		"INSERT INTO public.env_aliases (alias, env_id, is_renamable, namespace) VALUES ($1, $2, $3, $4)",
		alias, templateID, true, namespace,
	)
	require.NoError(t, err, "Failed to create test alias with namespace")

	return alias
}

// CreateTestTemplateAliasWithName creates an alias with a specific name and optional namespace
func CreateTestTemplateAliasWithName(t *testing.T, db *Database, templateID, aliasName string, namespace *string) {
	t.Helper()

	err := db.SqlcClient.TestsRawSQL(t.Context(),
		"INSERT INTO public.env_aliases (alias, env_id, is_renamable, namespace) VALUES ($1, $2, $3, $4)",
		aliasName, templateID, true, namespace,
	)
	require.NoError(t, err, "Failed to create test alias with name")
}

// GetTeamSlug retrieves the slug for a team
func GetTeamSlug(t *testing.T, ctx context.Context, db *Database, teamID uuid.UUID) string {
	t.Helper()
	var slug string

	err := db.AuthDb.TestsRawSQLQuery(ctx,
		"SELECT slug FROM public.teams WHERE id = $1",
		func(rows pgx.Rows) error {
			if rows.Next() {
				return rows.Scan(&slug)
			}

			return nil
		},
		teamID,
	)
	require.NoError(t, err, "Failed to get team slug")

	return slug
}

func CreateTestTemplateWithAlias(t *testing.T, db *Database, teamID uuid.UUID) (string, string) {
	t.Helper()

	templateID := CreateTestTemplate(t, db, teamID)
	alias := CreateTestTemplateAlias(t, db, templateID)

	return templateID, alias
}

// CreateTestBuild creates a build for a template with the given status
func CreateTestBuild(t *testing.T, ctx context.Context, db *Database, templateID string, status string) uuid.UUID {
	t.Helper()
	buildID := uuid.New()

	err := db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.env_builds 
		(id, env_id, team_id, status, vcpu, ram_mb, free_disk_size_mb, kernel_version, firecracker_version, cluster_node_id, created_at, updated_at)
		VALUES ($1, $2, (SELECT team_id FROM public.envs WHERE id = $2), $3, 2, 2048, 512, '6.1.0', '1.4.0', 'test-node', NOW(), NOW())`,
		buildID, templateID, status,
	)
	require.NoError(t, err, "Failed to create test build")

	return buildID
}

// CreateTestBuildAssignment creates a build assignment with a specific tag
func CreateTestBuildAssignment(t *testing.T, ctx context.Context, db *Database, templateID string, buildID uuid.UUID, tag string) {
	t.Helper()

	err := db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.env_build_assignments (env_id, build_id, tag, source, created_at)
		VALUES ($1, $2, $3, 'app', NOW())`,
		templateID, buildID, tag,
	)
	require.NoError(t, err, "Failed to create build assignment")
}

// GetBuildStatus retrieves the status of a build
func GetBuildStatus(t *testing.T, ctx context.Context, db *Database, buildID uuid.UUID) string {
	t.Helper()
	var status string

	err := db.SqlcClient.TestsRawSQLQuery(ctx,
		"SELECT status FROM public.env_builds WHERE id = $1",
		func(rows pgx.Rows) error {
			if rows.Next() {
				return rows.Scan(&status)
			}

			return nil
		},
		buildID,
	)
	require.NoError(t, err, "Failed to get build status")

	return status
}

// DeleteTriggerBuildAssignment deletes a trigger-created build assignment
// This is useful for tests that need to create builds without the auto-assigned 'default' tag
func DeleteTriggerBuildAssignment(t *testing.T, ctx context.Context, db *Database, templateID string, buildID uuid.UUID, tag string) {
	t.Helper()

	err := db.SqlcClient.TestsRawSQL(ctx,
		`DELETE FROM public.env_build_assignments 
		WHERE env_id = $1 AND build_id = $2 AND tag = $3 AND source = 'trigger'`,
		templateID, buildID, tag,
	)
	require.NoError(t, err, "Failed to delete trigger build assignment")
}

// CreateSnapshotRecord creates just the snapshot record without creating a new build
func CreateSnapshotRecord(t *testing.T, ctx context.Context, db *Database, templateID, sandboxID string, teamID uuid.UUID, baseTemplateID string) {
	t.Helper()

	err := db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.snapshots 
		(sandbox_id, env_id, team_id, base_env_id, sandbox_started_at, metadata, origin_node_id)
		VALUES ($1, $2, $3, $4, NOW(), '{}'::jsonb, 'test-node')`,
		sandboxID, templateID, teamID, baseTemplateID,
	)
	require.NoError(t, err, "Failed to create snapshot record")
}

// UpsertTestSnapshot creates/updates a snapshot for testing with success status
func UpsertTestSnapshot(t *testing.T, ctx context.Context, db *Database, templateID, sandboxID string, teamID uuid.UUID, baseTemplateID string) queries.UpsertSnapshotRow {
	t.Helper()

	return UpsertTestSnapshotWithStatus(t, ctx, db, templateID, sandboxID, teamID, baseTemplateID, types.BuildStatusSuccess)
}

// UpsertTestSnapshotWithStatus creates/updates a snapshot with a specific status
func UpsertTestSnapshotWithStatus(t *testing.T, ctx context.Context, db *Database, templateID, sandboxID string, teamID uuid.UUID, baseTemplateID string, status types.BuildStatus) queries.UpsertSnapshotRow {
	t.Helper()

	totalDiskSize := int64(1024)
	envdVersion := "v1.0.0"
	allowInternet := true

	result, err := db.SqlcClient.UpsertSnapshot(ctx, queries.UpsertSnapshotParams{
		TemplateID:          templateID,
		TeamID:              teamID,
		SandboxID:           sandboxID,
		BaseTemplateID:      baseTemplateID,
		StartedAt:           pgtype.Timestamptz{Time: time.Now(), Valid: true},
		Vcpu:                2,
		RamMb:               2048,
		TotalDiskSizeMb:     &totalDiskSize,
		Metadata:            types.JSONBStringMap{},
		KernelVersion:       "6.1.0",
		FirecrackerVersion:  "1.4.0",
		EnvdVersion:         &envdVersion,
		Secure:              true,
		AllowInternetAccess: &allowInternet,
		AutoPause:           true,
		OriginNodeID:        "test-node",
		Status:              status,
	})
	require.NoError(t, err, "Failed to upsert test snapshot")

	return result
}

// GetSnapshotMetadata retrieves the metadata from a snapshot using raw SQL
func GetSnapshotMetadata(t *testing.T, ctx context.Context, db *Database, sandboxID string) types.JSONBStringMap {
	t.Helper()
	var metadata types.JSONBStringMap

	err := db.SqlcClient.TestsRawSQLQuery(ctx,
		"SELECT metadata FROM public.snapshots WHERE sandbox_id = $1",
		func(rows pgx.Rows) error {
			if !rows.Next() {
				return nil
			}

			return rows.Scan(&metadata)
		},
		sandboxID,
	)
	require.NoError(t, err, "Failed to query snapshot metadata")

	return metadata
}

// BuildAssignment represents a row from env_build_assignments
type BuildAssignment struct {
	ID      uuid.UUID
	EnvID   string
	BuildID uuid.UUID
	Tag     string
	Source  string
}

// GetBuildAssignments retrieves all build assignments for a given env_id
func GetBuildAssignments(t *testing.T, ctx context.Context, db *Database, envID string) []BuildAssignment {
	t.Helper()
	var assignments []BuildAssignment

	err := db.SqlcClient.TestsRawSQLQuery(ctx,
		"SELECT id, env_id, build_id, tag, source FROM public.env_build_assignments WHERE env_id = $1 ORDER BY created_at DESC",
		func(rows pgx.Rows) error {
			for rows.Next() {
				var a BuildAssignment
				if err := rows.Scan(&a.ID, &a.EnvID, &a.BuildID, &a.Tag, &a.Source); err != nil {
					return err
				}
				assignments = append(assignments, a)
			}

			return nil
		},
		envID,
	)
	require.NoError(t, err, "Failed to query build assignments")

	return assignments
}

// GetBuildAssignmentByBuildID retrieves a build assignment for a specific build_id
func GetBuildAssignmentByBuildID(t *testing.T, ctx context.Context, db *Database, buildID uuid.UUID) *BuildAssignment {
	t.Helper()
	var assignment *BuildAssignment

	err := db.SqlcClient.TestsRawSQLQuery(ctx,
		"SELECT id, env_id, build_id, tag, source FROM public.env_build_assignments WHERE build_id = $1",
		func(rows pgx.Rows) error {
			if rows.Next() {
				assignment = &BuildAssignment{}

				return rows.Scan(&assignment.ID, &assignment.EnvID, &assignment.BuildID, &assignment.Tag, &assignment.Source)
			}

			return nil
		},
		buildID,
	)
	require.NoError(t, err, "Failed to query build assignment by build_id")

	return assignment
}

// GetEnvByID retrieves an env by ID to verify it exists
func GetEnvByID(t *testing.T, ctx context.Context, db *Database, envID string) bool {
	t.Helper()
	var exists bool

	err := db.SqlcClient.TestsRawSQLQuery(ctx,
		"SELECT EXISTS(SELECT 1 FROM public.envs WHERE id = $1)",
		func(rows pgx.Rows) error {
			if rows.Next() {
				return rows.Scan(&exists)
			}

			return nil
		},
		envID,
	)
	require.NoError(t, err, "Failed to check if env exists")

	return exists
}

// GetEnvBuildByID retrieves an env_build by ID to verify it exists
func GetEnvBuildByID(t *testing.T, ctx context.Context, db *Database, buildID uuid.UUID) bool {
	t.Helper()
	var exists bool

	err := db.SqlcClient.TestsRawSQLQuery(ctx,
		"SELECT EXISTS(SELECT 1 FROM public.env_builds WHERE id = $1)",
		func(rows pgx.Rows) error {
			if rows.Next() {
				return rows.Scan(&exists)
			}

			return nil
		},
		buildID,
	)
	require.NoError(t, err, "Failed to check if env_build exists")

	return exists
}
