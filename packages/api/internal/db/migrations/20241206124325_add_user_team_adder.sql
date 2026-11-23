-- +goose Up
-- +goose StatementBegin

-- Add column to "users_teams" table
ALTER TABLE "public"."users_teams"
    ADD COLUMN IF NOT EXISTS "added_by" uuid NULL;

COMMIT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
