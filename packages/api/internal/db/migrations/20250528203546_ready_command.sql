-- +goose Up
-- +goose StatementBegin
ALTER TABLE public.env_builds
    ADD COLUMN ready_cmd      TEXT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE public.env_builds
DROP COLUMN ready_cmd;
-- +goose StatementEnd