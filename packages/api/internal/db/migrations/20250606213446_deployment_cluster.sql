-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS clusters (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint      TEXT NOT NULL,
    endpoint_tls  BOOLEAN NOT NULL DEFAULT TRUE,
    token         TEXT NOT NULL
);

ALTER TABLE teams
    ADD COLUMN IF NOT EXISTS cluster_id UUID NULL
    REFERENCES clusters(id);

CREATE INDEX IF NOT EXISTS teams_cluster_id_uq
    ON teams (cluster_id)
    WHERE cluster_id IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS teams_cluster_id_uq;

ALTER TABLE teams DROP COLUMN IF EXISTS cluster_id;

DROP TABLE IF EXISTS clusters CASCADE;
-- +goose StatementEnd
