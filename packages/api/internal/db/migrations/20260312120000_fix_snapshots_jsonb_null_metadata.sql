-- +goose NO TRANSACTION
-- +goose Up

-- Fix rows where metadata contains the JSON literal 'null' (not SQL NULL).
-- These are not caught by the previous IS NULL backfill but fail @> '{}' checks.
-- +goose StatementBegin
CREATE OR REPLACE PROCEDURE fix_snapshots_jsonb_null_metadata() AS $$
DECLARE
  batch_size INT := 50000;
  rows_updated INT;
BEGIN
  LOOP
    UPDATE public.snapshots
    SET metadata = '{}'::jsonb
    WHERE id IN (
      SELECT id FROM public.snapshots
      WHERE metadata = 'null'::jsonb
      LIMIT batch_size
      FOR UPDATE SKIP LOCKED
    );

    GET DIAGNOSTICS rows_updated = ROW_COUNT;
    EXIT WHEN rows_updated = 0;

    COMMIT;
    RAISE NOTICE 'fix_snapshots_jsonb_null_metadata: updated % rows, sleeping 10s...', rows_updated;
    PERFORM pg_sleep(10);
  END LOOP;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

CALL fix_snapshots_jsonb_null_metadata();
DROP PROCEDURE fix_snapshots_jsonb_null_metadata();

-- Add a trigger that silently converts JSON null to '{}' on insert/update,
-- so old code that still writes 'null'::jsonb won't break.
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

-- +goose Down
DROP TRIGGER IF EXISTS trg_snapshots_fix_json_null_metadata ON public.snapshots;
DROP FUNCTION IF EXISTS fix_snapshots_metadata_json_null();
