-- +goose Up
-- +goose StatementBegin
-- Deleting a cluster soft-deletes every template and snapshot on it, so a
-- cluster can only be deleted once its protection is turned off. Every
-- existing cluster starts protected. A constant default is a catalog-only
-- change, so the table is not rewritten.
ALTER TABLE public.clusters
    ADD COLUMN deletion_protection boolean NOT NULL DEFAULT true;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE public.clusters DROP COLUMN deletion_protection;
-- +goose StatementEnd
