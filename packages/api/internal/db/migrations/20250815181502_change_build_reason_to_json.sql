-- +goose Up
-- +goose StatementBegin

-- First, update existing TEXT values to JSON format
-- Convert existing text reasons to JSON with message field
UPDATE env_builds 
SET reason = json_build_object('message', reason)::jsonb
WHERE reason IS NOT NULL AND reason != '';

-- Now alter the column type from TEXT to JSONB
ALTER TABLE env_builds 
ALTER COLUMN reason TYPE jsonb 
USING CASE 
    WHEN reason IS NULL THEN NULL
    WHEN reason::text = '' THEN NULL
    ELSE reason::jsonb
END;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- Convert JSONB back to TEXT by extracting the message field
ALTER TABLE env_builds 
ALTER COLUMN reason TYPE text 
USING CASE 
    WHEN reason IS NULL THEN NULL
    WHEN reason->>'message' IS NOT NULL THEN reason->>'message'
    ELSE reason::text
END;

-- +goose StatementEnd
