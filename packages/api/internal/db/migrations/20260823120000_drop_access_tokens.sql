-- +goose Up

-- E2B user access tokens (sk_e2b_) are removed: nothing issues, validates,
-- or purges them anymore. The remaining rows are hashes of revoked
-- credentials.
DROP TABLE IF EXISTS public.access_tokens;
DROP FUNCTION IF EXISTS public.generate_access_token();

-- +goose Down
-- +goose StatementBegin

CREATE OR REPLACE FUNCTION public.generate_access_token()
    RETURNS TEXT
    LANGUAGE plpgsql
AS $generate_access_token$
DECLARE
    access_token_prefix TEXT := 'sk_e2b_';
    generated_token TEXT;
BEGIN
    generated_token := encode(extensions.gen_random_bytes(20), 'hex');
    RETURN access_token_prefix || generated_token;
END
$generate_access_token$ SECURITY DEFINER SET search_path = public;

CREATE TABLE IF NOT EXISTS public.access_tokens
(
    id                       uuid        NOT NULL DEFAULT gen_random_uuid(),
    user_id                  uuid        NOT NULL,
    created_at               timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    access_token_hash        text        NOT NULL,
    name                     text        NOT NULL DEFAULT 'Unnamed Access Token',
    access_token_prefix      varchar(10) NOT NULL,
    access_token_length      integer     NOT NULL,
    access_token_mask_prefix varchar(5)  NOT NULL,
    access_token_mask_suffix varchar(5)  NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT access_tokens_access_token_hash_key UNIQUE (access_token_hash),
    CONSTRAINT access_tokens_users_access_tokens FOREIGN KEY (user_id)
        REFERENCES public.users (id) ON UPDATE NO ACTION ON DELETE CASCADE
);

COMMENT ON COLUMN public.access_tokens.access_token_hash IS 'sensitive';

CREATE INDEX IF NOT EXISTS idx_users_access_tokens ON public.access_tokens (user_id);

-- +goose StatementEnd
