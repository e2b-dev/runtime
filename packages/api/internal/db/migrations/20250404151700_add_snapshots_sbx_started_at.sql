-- +goose Up
-- +goose StatementBegin

ALTER TABLE "public"."snapshots"
  ADD COLUMN IF NOT EXISTS "sandbox_started_at" timestamp with time zone NOT NULL 
  DEFAULT TIMESTAMP WITH TIME ZONE '1970-01-01 00:00:00 UTC';

-- Update records with actual data only if billing schema and table exist
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.schemata WHERE schema_name = 'billing'
  ) AND EXISTS (
    SELECT 1 FROM information_schema.tables 
    WHERE table_schema = 'billing' AND table_name = 'sandbox_logs'
  ) THEN
    UPDATE public.snapshots s
    SET sandbox_started_at = latest_starts.started_at
    FROM (
      SELECT sandbox_id, MAX(started_at) as started_at
      FROM billing.sandbox_logs
      GROUP BY sandbox_id
    ) latest_starts
    WHERE s.sandbox_id = latest_starts.sandbox_id;
  END IF;
END $$;

-- Remove the default constraint after populating data
ALTER TABLE "public"."snapshots"
ALTER COLUMN "sandbox_started_at" DROP DEFAULT;

COMMIT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
