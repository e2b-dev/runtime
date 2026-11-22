-- +goose Up
-- +goose StatementBegin
ALTER TABLE public.users_teams ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE public.users_teams DROP COLUMN created_at;
-- +goose StatementEnd
