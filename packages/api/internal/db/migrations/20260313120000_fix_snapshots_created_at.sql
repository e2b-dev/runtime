-- +goose NO TRANSACTION
-- +goose Up

-- Set default first so new rows get created_at while backfill runs.
ALTER TABLE public.snapshots ALTER COLUMN created_at SET DEFAULT now();

-- Backfill NULL created_at from the corresponding envs.created_at in batches.
-- +goose StatementBegin
CREATE OR REPLACE PROCEDURE backfill_snapshots_created_at() AS $$
DECLARE
  batch_size INT := 50000;
  rows_updated INT;
BEGIN
  LOOP
    UPDATE public.snapshots s
    SET created_at = e.created_at
    FROM public.envs e
    WHERE s.env_id = e.id
      AND s.id IN (
        SELECT id FROM public.snapshots
        WHERE created_at IS NULL
        LIMIT batch_size
        FOR UPDATE SKIP LOCKED
      );

    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    EXIT WHEN rows_updated = 0;

    COMMIT;
    RAISE NOTICE 'backfill_snapshots_created_at: updated % rows, sleeping 10s...', rows_updated;
    PERFORM pg_sleep(10);
  END LOOP;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CALL backfill_snapshots_created_at();
DROP PROCEDURE backfill_snapshots_created_at();

-- +goose Down
ALTER TABLE public.snapshots ALTER COLUMN created_at DROP DEFAULT;
