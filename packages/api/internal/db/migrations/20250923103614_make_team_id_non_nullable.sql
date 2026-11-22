-- +goose Up
-- +goose StatementBegin
UPDATE "public"."snapshots" SET team_id = e.team_id FROM "public"."envs" e WHERE e.id = snapshots.env_id;
ALTER TABLE "public"."snapshots" ALTER COLUMN "team_id" SET NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE "public"."snapshots" ALTER COLUMN "team_id" DROP NOT NULL;
-- +goose StatementEnd
