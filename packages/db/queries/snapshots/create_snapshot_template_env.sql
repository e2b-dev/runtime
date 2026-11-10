-- name: CreateSnapshotTemplateEnv :one
-- Creates a snapshot_template env entry with source='snapshot_template' and links it to an existing build
-- This is used after UpsertSnapshot to create a persistent snapshot template
--
-- The caller generates snapshot_id outside the pool's retry loop, so a replay
-- reuses it. The conflict guards match only a row this statement could have
-- written; anything else returns no row, which fails the dependent inserts.
WITH new_env AS (
    INSERT INTO "public"."envs" (id, public, created_by, team_id, updated_at, source, cluster_id)
    VALUES (@snapshot_id, FALSE, NULL, @team_id, now(), 'snapshot_template', @cluster_id)
    ON CONFLICT (id) DO UPDATE SET updated_at = now()
    WHERE envs.team_id = @team_id
      AND envs.source = 'snapshot_template'
      AND envs.deleted_at IS NULL
    RETURNING id
),

snapshot_template AS (
    INSERT INTO "public"."snapshot_templates" (env_id, sandbox_id, origin_node_id, build_id)
    VALUES (
        (SELECT id FROM new_env),
        @sandbox_id,
        @origin_node_id,
        @build_id
    )
    ON CONFLICT (env_id) DO NOTHING
    RETURNING env_id
),

build_assignment AS (
    INSERT INTO "public"."env_build_assignments" (env_id, build_id, tag)
    SELECT env_id, @build_id, @tag
    FROM snapshot_template
)

SELECT id AS snapshot_id FROM new_env;
