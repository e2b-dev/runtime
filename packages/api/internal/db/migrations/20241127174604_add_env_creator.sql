-- +goose Up
-- +goose StatementBegin

-- Modify "envs" table
ALTER TABLE "public"."envs"
    ADD COLUMN IF NOT EXISTS "created_by" uuid NULL;

-- Add constraint
ALTER TABLE "public"."envs"
    ADD CONSTRAINT "envs_users_created_envs"
        FOREIGN KEY ("created_by")
            REFERENCES "auth"."users" ("id")
            ON UPDATE NO ACTION
            ON DELETE SET NULL;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
