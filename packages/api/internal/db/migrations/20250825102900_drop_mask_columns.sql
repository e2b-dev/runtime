-- +goose Up

-- Drop the mask columns as they're no longer necessary
ALTER TABLE public.team_api_keys DROP COLUMN IF EXISTS api_key_mask;
ALTER TABLE public.access_tokens DROP COLUMN IF EXISTS access_token_mask;

-- +goose Down

-- Recreate the mask columns that were dropped
ALTER TABLE public.team_api_keys ADD COLUMN IF NOT EXISTS api_key_mask character varying(44) NULL;
ALTER TABLE public.access_tokens ADD COLUMN IF NOT EXISTS access_token_mask text NULL;
