-- +goose Up
-- +goose StatementBegin

-- Modify "team_api_keys" table
ALTER TABLE "public"."team_api_keys" DROP CONSTRAINT IF EXISTS "team_api_keys_pkey";
ALTER TABLE "public"."team_api_keys" ADD COLUMN IF NOT EXISTS "id" uuid NOT NULL DEFAULT gen_random_uuid();
ALTER TABLE "public"."team_api_keys" ADD PRIMARY KEY ("id");

-- Create index if it doesn't exist
CREATE UNIQUE INDEX IF NOT EXISTS "team_api_keys_api_key_key" ON "public"."team_api_keys" ("api_key");

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
