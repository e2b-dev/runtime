-- +goose Up
-- +goose StatementBegin

-- Add new columns to team_api_keys table
ALTER TABLE team_api_keys
    ADD COLUMN IF NOT EXISTS api_key_hash TEXT UNIQUE,
    ADD COLUMN IF NOT EXISTS api_key_mask VARCHAR(44);

-- Add new columns to access_tokens table
ALTER TABLE access_tokens
    ADD COLUMN IF NOT EXISTS id UUID DEFAULT gen_random_uuid(),
    ADD COLUMN IF NOT EXISTS access_token_hash TEXT UNIQUE,
    ADD COLUMN IF NOT EXISTS access_token_mask TEXT,
    ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT 'Unnamed Access Token';

-- Mark sensitive columns as sensitive
COMMENT ON COLUMN team_api_keys.api_key_hash IS 'sensitive';
COMMENT ON COLUMN access_tokens.access_token_hash IS 'sensitive';

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
