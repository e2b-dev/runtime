-- +goose Up
-- +goose StatementBegin
-- Add unique constraint on sandbox_id
ALTER TABLE "public"."snapshots"
    ADD CONSTRAINT snapshots_sandbox_id_unique UNIQUE (sandbox_id);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE "public"."snapshots"
    DROP CONSTRAINT IF EXISTS snapshots_sandbox_id_unique;
-- +goose StatementEnd