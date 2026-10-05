-- +goose Up
CREATE SCHEMA IF NOT EXISTS projection;

CREATE TABLE projection.project_blocks (
    project_id uuid PRIMARY KEY REFERENCES public.teams(id) ON DELETE CASCADE,
    revision bigint NOT NULL CHECK (revision > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE projection.project_blocks;
