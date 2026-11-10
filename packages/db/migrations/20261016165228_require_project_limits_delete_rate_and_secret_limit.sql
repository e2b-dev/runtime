-- +goose Up
-- +goose StatementBegin
SET LOCAL lock_timeout = '5s';

-- A row not pushed since these columns were added keeps what team_limits
-- resolves for it now.
UPDATE public.project_limits pl
SET api_team_rps_delete = COALESCE(pl.api_team_rps_delete, tl.api_team_rps_delete),
    max_secrets = COALESCE(pl.max_secrets, tl.max_secrets)
FROM public.team_limits tl
WHERE tl.id = pl.team_id
  AND (pl.api_team_rps_delete IS NULL OR pl.max_secrets IS NULL);

ALTER TABLE public.project_limits
    ALTER COLUMN api_team_rps_delete SET NOT NULL,
    ALTER COLUMN max_secrets SET NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SET LOCAL lock_timeout = '5s';

ALTER TABLE public.project_limits
    ALTER COLUMN api_team_rps_delete DROP NOT NULL,
    ALTER COLUMN max_secrets DROP NOT NULL;
-- +goose StatementEnd
