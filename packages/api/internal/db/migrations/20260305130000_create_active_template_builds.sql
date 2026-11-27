-- +goose Up

CREATE TABLE IF NOT EXISTS public.active_template_builds (
    build_id uuid PRIMARY KEY,
    team_id uuid NOT NULL,
    template_id text NOT NULL,
    tags text[] NOT NULL,
    created_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_active_template_builds_team_created_at
    ON public.active_template_builds (team_id, created_at DESC);


-- +goose Down

DROP TABLE IF EXISTS public.active_template_builds;
