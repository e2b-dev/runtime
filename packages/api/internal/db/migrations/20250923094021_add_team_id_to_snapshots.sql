-- +goose Up
-- +goose StatementBegin
ALTER TABLE "public"."snapshots" ADD COLUMN "team_id" uuid NULL;
UPDATE "public"."snapshots" SET team_id = e.team_id FROM "public"."envs" e WHERE e.id = snapshots.env_id;
ALTER TABLE "public"."snapshots"
    ADD CONSTRAINT fk_snapshots_team
            FOREIGN KEY (team_id)
            REFERENCES teams(id)
            ON UPDATE NO ACTION ON DELETE NO ACTION;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE "public"."snapshots" DROP CONSTRAINT IF EXISTS fk_snapshots_team;
ALTER TABLE "public"."snapshots" DROP COLUMN "team_id";
-- +goose StatementEnd
