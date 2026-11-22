-- +goose Up
-- Soft-delete marker for envs: deleting a template/snapshot stamps deleted_at
-- instead of cascading its rows away. The active_envs view is the canonical
-- read path so callers don't repeat the deleted_at IS NULL filter.
ALTER TABLE "public"."envs" ADD COLUMN IF NOT EXISTS deleted_at timestamptz NULL;

CREATE OR REPLACE VIEW "public"."active_envs" AS
SELECT
    id,
    created_at,
    updated_at,
    public,
    build_count,
    spawn_count,
    last_spawned_at,
    team_id,
    created_by,
    cluster_id,
    source
FROM "public"."envs"
WHERE deleted_at IS NULL;

-- +goose Down
DROP VIEW IF EXISTS "public"."active_envs";
ALTER TABLE "public"."envs" DROP COLUMN IF EXISTS deleted_at;
