-- +goose NO TRANSACTION
-- +goose Up

-- Set default first so new rows never get NULL metadata while backfill runs.
ALTER TABLE public.snapshots ALTER COLUMN metadata SET DEFAULT '{}'::jsonb;

-- Install a trigger that converts SQL NULL and JSON 'null' to '{}' on insert/update,
-- so concurrent writes during the backfill can't re-introduce NULLs.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION fix_snapshots_metadata_json_null()
RETURNS trigger AS $$
BEGIN
  IF NEW.metadata IS NULL OR NEW.metadata = 'null'::jsonb THEN
    NEW.metadata := '{}'::jsonb;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS trg_snapshots_fix_json_null_metadata ON public.snapshots;
CREATE TRIGGER trg_snapshots_fix_json_null_metadata
  BEFORE INSERT OR UPDATE OF metadata ON public.snapshots
  FOR EACH ROW
  EXECUTE FUNCTION fix_snapshots_metadata_json_null();

-- Backfill NULL metadata to empty jsonb in batches.
-- Each iteration picks an arbitrary batch of NULLs (no ordering on random UUIDs).
-- +goose StatementBegin
CREATE OR REPLACE PROCEDURE backfill_snapshots_metadata() AS $$
DECLARE
  batch_size INT := 50000;
  rows_updated INT;
BEGIN
  LOOP
    UPDATE public.snapshots
    SET metadata = '{}'::jsonb
    WHERE id IN (
      SELECT id FROM public.snapshots
      WHERE metadata IS NULL
      LIMIT batch_size
      FOR UPDATE SKIP LOCKED
    );

    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    EXIT WHEN rows_updated = 0;

    COMMIT;
    RAISE NOTICE 'backfill_snapshots_metadata: updated % rows, sleeping 10s...', rows_updated;
    PERFORM pg_sleep(10);
  END LOOP;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CALL backfill_snapshots_metadata();
DROP PROCEDURE backfill_snapshots_metadata();

ALTER TABLE public.snapshots ALTER COLUMN metadata SET NOT NULL;

CREATE EXTENSION IF NOT EXISTS btree_gin;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_snapshots_team_metadata_gin
    ON public.snapshots USING gin (team_id, metadata);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_snapshots_team_metadata_gin;

ALTER TABLE public.snapshots ALTER COLUMN metadata DROP NOT NULL;
ALTER TABLE public.snapshots ALTER COLUMN metadata DROP DEFAULT;

DROP TRIGGER IF EXISTS trg_snapshots_fix_json_null_metadata ON public.snapshots;
DROP FUNCTION IF EXISTS fix_snapshots_metadata_json_null();
