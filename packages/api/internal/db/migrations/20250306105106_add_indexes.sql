-- +goose Up
-- +goose StatementBegin

CREATE INDEX IF NOT EXISTS idx_envs_builds_envs ON public.env_builds (env_id);
CREATE INDEX IF NOT EXISTS idx_envs_envs_aliases ON public.env_aliases (env_id);
CREATE INDEX IF NOT EXISTS idx_users_access_tokens ON public.access_tokens (user_id);
CREATE INDEX IF NOT EXISTS idx_teams_envs ON public.envs (team_id);
CREATE INDEX IF NOT EXISTS idx_team_team_api_keys ON public.team_api_keys (team_id);
CREATE INDEX IF NOT EXISTS idx_teams_user_teams ON public.users_teams (team_id);
CREATE INDEX IF NOT EXISTS idx_users_user_teams ON public.users_teams (user_id);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- +goose StatementEnd
