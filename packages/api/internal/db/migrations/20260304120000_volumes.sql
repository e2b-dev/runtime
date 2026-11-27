-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS volumes (
    id          UUID                        PRIMARY KEY     DEFAULT gen_random_uuid(),
    team_id     UUID                        NOT NULL,
    name        VARCHAR(250)                NOT NULL,
    volume_type VARCHAR(250)                NOT NULL,
    created_at  TIMESTAMP WITH TIME ZONE    NOT NULL        DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_volumes_teams
        FOREIGN KEY (team_id)
        REFERENCES teams(id),

    CONSTRAINT volumes_teams_uq
        UNIQUE (team_id, name)
);
-- +goose StatementEnd

-- +goose StatementBegin
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS volumes;
-- +goose StatementEnd
