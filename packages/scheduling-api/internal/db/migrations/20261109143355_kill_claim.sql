-- +goose Up
ALTER TABLE public.scheduling_actor_run
    ADD COLUMN envd_refused_until timestamptz,
    ADD COLUMN envd_refused_since timestamptz;

CREATE TABLE public.scheduling_kill_claim (
    actor_id uuid NOT NULL,
    project_id uuid NOT NULL,
    claim_id uuid NOT NULL,
    until timestamptz NOT NULL,
    PRIMARY KEY (project_id, actor_id, claim_id)
);

CREATE INDEX scheduling_kill_claim_until_idx ON public.scheduling_kill_claim (until);

-- +goose Down
DROP TABLE public.scheduling_kill_claim;
ALTER TABLE public.scheduling_actor_run
    DROP COLUMN envd_refused_until,
    DROP COLUMN envd_refused_since;
