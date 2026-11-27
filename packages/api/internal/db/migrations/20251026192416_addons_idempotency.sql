-- +goose Up
-- +goose StatementBegin
ALTER TABLE public.addons ADD COLUMN idempotency_key text;
CREATE UNIQUE INDEX addons_idempotency_key_uidx
    ON public.addons(idempotency_key) WHERE idempotency_key IS NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX addons_idempotency_key_uidx;
ALTER TABLE public.addons DROP COLUMN idempotency_key;
-- +goose StatementEnd
