-- +goose Up
CREATE SCHEMA IF NOT EXISTS projection;

CREATE TABLE projection.project_members (
    project_id uuid NOT NULL REFERENCES public.teams(id) ON DELETE CASCADE,
    user_id uuid NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0),
    present boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, user_id)
);

-- +goose Down
DROP TABLE projection.project_members;
DROP SCHEMA projection;
