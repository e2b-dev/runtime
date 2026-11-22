-- +goose Up
-- +goose StatementBegin
-- A caller that deletes a cluster's teams before the cluster records the
-- deletion first. From then on the cluster cannot be protected again, so the
-- final cluster delete is never refused after its teams are gone.
ALTER TABLE public.clusters
    ADD COLUMN deletion_requested_at timestamptz,
    ADD CONSTRAINT clusters_deletion_requested_unprotected
        CHECK (deletion_requested_at IS NULL OR NOT deletion_protection);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE public.clusters
    DROP CONSTRAINT clusters_deletion_requested_unprotected,
    DROP COLUMN deletion_requested_at;
-- +goose StatementEnd
