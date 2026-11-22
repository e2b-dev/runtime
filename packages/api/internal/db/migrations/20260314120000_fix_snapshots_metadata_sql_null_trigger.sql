-- +goose Up

-- Update the trigger to also handle SQL NULL (not just JSON 'null' literal).
-- The NOT NULL constraint on metadata rejects SQL NULLs, but the BEFORE trigger
-- runs first and can convert them to '{}' before the constraint is checked.
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

-- +goose Down

-- Restore original trigger that only handles JSON null.
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
