-- +goose Up
-- +goose StatementBegin

-- teams.profile_picture_url was declared only by the separate dashboard
-- migration set, which no migrator ever applied; production databases carry
-- the column from history. The dashboard stopped using it and every query
-- stopped naming it in the previous release, so nothing reads or writes it any
-- more. IF EXISTS keeps databases that never had it unaffected.
--
-- DROP COLUMN takes an ACCESS EXCLUSIVE lock on teams. The migrator sets only a
-- statement timeout, so bound the lock wait here: better to fail and retry than
-- to queue behind a long transaction while every writer to teams waits on us.
SET LOCAL lock_timeout = '5s';
ALTER TABLE public.teams DROP COLUMN IF EXISTS profile_picture_url;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

-- Re-adds the column empty; the dropped values are not restored.
SET LOCAL lock_timeout = '5s';
ALTER TABLE public.teams ADD COLUMN IF NOT EXISTS profile_picture_url TEXT;

-- +goose StatementEnd
