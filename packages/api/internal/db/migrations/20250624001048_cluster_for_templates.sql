-- +goose Up
-- +goose StatementBegin
ALTER TABLE envs
    ADD COLUMN IF NOT EXISTS cluster_id UUID NULL
    REFERENCES clusters(id);

CREATE INDEX IF NOT EXISTS envs_cluster_id
    ON envs (cluster_id)
    WHERE cluster_id IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS envs_cluster_id;

ALTER TABLE envs DROP COLUMN IF EXISTS cluster_id;
-- +goose StatementEnd
