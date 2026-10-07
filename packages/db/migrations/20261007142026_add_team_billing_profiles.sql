-- +goose Up
-- +goose StatementBegin

SET LOCAL lock_timeout = '5s';

-- Delivery ledger for the pushed billing profile, same shape and reasoning as
-- projection.project_limits: the push is at-least-once over a network, so the
-- older of two in-flight deliveries has to be refused where it lands, in the
-- same transaction as the values it would have written.
CREATE TABLE projection.billing_profiles (
    project_id uuid PRIMARY KEY REFERENCES public.teams(id) ON DELETE CASCADE,
    revision bigint NOT NULL CHECK (revision > 0),
    decided_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- The billing facts a plane cannot know on its own once a team's billing lives
-- on the global plane: whether the workspace can be charged and which plan is
-- in force. Regional readers (bot-detection's payment signal, the LaunchDarkly
-- tier context, tier labels) prefer this row over the legacy regional sources;
-- a team without a row has an unknown profile, never a defaulted one.
CREATE TABLE public.team_billing_profiles (
    team_id uuid PRIMARY KEY REFERENCES public.teams(id) ON DELETE CASCADE,
    has_payment_method boolean NOT NULL,
    enterprise boolean NOT NULL,
    plan text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin

SET LOCAL lock_timeout = '5s';
DROP TABLE public.team_billing_profiles;
DROP TABLE projection.billing_profiles;

-- +goose StatementEnd
