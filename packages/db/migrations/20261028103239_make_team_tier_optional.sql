-- +goose Up
-- +goose StatementBegin
SET LOCAL lock_timeout = '5s';

-- The team lookups resolve limits from project_limits and default_limits, so a
-- team needs no tier. The legacy team_limits view inner-joins tiers and omits
-- a team without one.
ALTER TABLE public.teams ALTER COLUMN tier DROP NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SET LOCAL lock_timeout = '5s';

-- Held from before the backfill, so a team inserted without a tier after the
-- UPDATE's snapshot cannot fail the SET NOT NULL below.
LOCK TABLE public.teams IN ACCESS EXCLUSIVE MODE;
UPDATE public.teams SET tier = 'base_v1' WHERE tier IS NULL;
ALTER TABLE public.teams ALTER COLUMN tier SET NOT NULL;
-- +goose StatementEnd
