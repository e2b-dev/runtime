-- +goose Up
ALTER TABLE public.users_teams ADD COLUMN role text NOT NULL DEFAULT 'member';

-- +goose Down
ALTER TABLE public.users_teams DROP COLUMN role;
