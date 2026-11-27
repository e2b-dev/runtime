-- +goose Up
-- +goose StatementBegin
UPDATE public.env_builds
    SET reason = '{}'::jsonb
    WHERE reason IS NULL;

ALTER TABLE public.env_builds
    ALTER COLUMN env_id SET NOT NULL,
    ALTER COLUMN reason SET DEFAULT '{}'::jsonb,
    ALTER COLUMN reason SET NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE public.env_builds
    ALTER COLUMN env_id DROP NOT NULL,
    ALTER COLUMN reason DROP NOT NULL,
    ALTER COLUMN reason DROP DEFAULT;
-- +goose StatementEnd
