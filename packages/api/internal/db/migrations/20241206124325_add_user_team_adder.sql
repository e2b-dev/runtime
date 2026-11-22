-- +goose Up
-- +goose StatementBegin

-- Add column to "users_teams" table
ALTER TABLE "public"."users_teams"
    ADD COLUMN IF NOT EXISTS "added_by" uuid NULL;

-- Add constraint
ALTER TABLE "public"."users_teams"
    ADD CONSTRAINT "users_teams_added_by_user"
        FOREIGN KEY ("added_by")
            REFERENCES "auth"."users" ("id")
            ON UPDATE NO ACTION
            ON DELETE SET NULL;

COMMIT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
