-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS public.user_identities (
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    oidc_iss text NOT NULL,
    oidc_sub text NOT NULL,
    user_id uuid NOT NULL,
    PRIMARY KEY (oidc_iss, oidc_sub),
    FOREIGN KEY (user_id) REFERENCES public.users(id) ON UPDATE NO ACTION ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS user_identities_user_id_idx ON public.user_identities (user_id);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS public.user_identities;
-- +goose StatementEnd
