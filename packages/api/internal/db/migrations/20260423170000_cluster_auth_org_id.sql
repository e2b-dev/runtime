-- +goose Up
-- +goose StatementBegin
ALTER TABLE clusters
    ADD COLUMN auth_org_id TEXT;

CREATE UNIQUE INDEX clusters_auth_org_id_idx
    ON clusters (auth_org_id)
    WHERE auth_org_id IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX clusters_auth_org_id_idx;

ALTER TABLE clusters
    DROP COLUMN auth_org_id;
-- +goose StatementEnd
