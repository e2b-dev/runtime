-- +goose Up
-- +goose NO TRANSACTION

-- +goose StatementBegin
-- Add source column to envs table to track where the env came from
-- 'template' = built (default)
-- 'snapshot' = created from pause/resume
ALTER TABLE "public"."envs"
    ADD COLUMN IF NOT EXISTS "source" text NOT NULL DEFAULT 'template';

-- Create trigger first so any concurrent snapshot inserts are handled
CREATE OR REPLACE FUNCTION sync_env_source_on_snapshot_insert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE "public"."envs" SET source = 'snapshot' WHERE id = NEW.env_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_env_source_on_snapshot ON "public"."snapshots";
CREATE TRIGGER trg_sync_env_source_on_snapshot
AFTER INSERT ON "public"."snapshots"
FOR EACH ROW EXECUTE FUNCTION sync_env_source_on_snapshot_insert();
-- +goose StatementEnd

-- Backfill existing snapshot envs in batches of 10k to avoid long-held locks
-- +goose StatementBegin
CREATE OR REPLACE PROCEDURE backfill_env_source() AS $$
DECLARE
    affected INT;
BEGIN
    LOOP
        UPDATE "public"."envs" e
        SET source = 'snapshot'
        FROM (
            SELECT e2.id
            FROM "public"."envs" e2
            JOIN "public"."snapshots" s ON s.env_id = e2.id
            WHERE e2.source != 'snapshot'
            LIMIT 10000
        ) sub
        WHERE e.id = sub.id;

        GET DIAGNOSTICS affected = ROW_COUNT;
        COMMIT;
        EXIT WHEN affected = 0;
        RAISE NOTICE 'backfill_env_source: updated % rows, sleeping 10s before next batch...', affected;
        PERFORM pg_sleep(10);
    END LOOP;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CALL backfill_env_source();
DROP PROCEDURE backfill_env_source();

-- +goose Down
-- +goose NO TRANSACTION

-- +goose StatementBegin
DROP TRIGGER IF EXISTS trg_sync_env_source_on_snapshot ON "public"."snapshots";
DROP FUNCTION IF EXISTS sync_env_source_on_snapshot_insert();
ALTER TABLE "public"."envs" DROP COLUMN IF EXISTS "source";
-- +goose StatementEnd
