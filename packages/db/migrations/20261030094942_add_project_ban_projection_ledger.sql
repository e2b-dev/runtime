-- +goose Up
SET LOCAL lock_timeout = '5s';

CREATE TABLE projection.project_bans (
    project_id uuid PRIMARY KEY REFERENCES public.teams(id) ON DELETE CASCADE,
    revision bigint NOT NULL CHECK (revision > 0),
    decided_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
SET LOCAL lock_timeout = '5s';

DROP TABLE projection.project_bans;
