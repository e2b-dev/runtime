-- +goose Up
CREATE SCHEMA IF NOT EXISTS projection;

-- How far the pushed limits for a project have got, kept apart from the limits
-- themselves: public.project_limits is the answer every reader wants, and this
-- is the bookkeeping that decides which delivery gets to write it.
--
-- The push is at-least-once over a network, so two deliveries can be in flight
-- at once and arrive in either order. The caller can fence what it sends but
-- not what arrives, so the older one has to be refused where it lands. A
-- delivery is applied only when it carries a revision above the one here, and
-- the row it would have written is left alone.
--
-- Advanced in the same transaction as the values, which is what keeps the two
-- from disagreeing: a revision recorded without its values would make every
-- retry a duplicate this side drops, leaving the project on the old limits for
-- good.
--
-- Same shape and the same reasoning as projection.project_members.
CREATE TABLE projection.project_limits (
    project_id uuid PRIMARY KEY REFERENCES public.teams(id) ON DELETE CASCADE,
    revision bigint NOT NULL CHECK (revision > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE projection.project_limits;
