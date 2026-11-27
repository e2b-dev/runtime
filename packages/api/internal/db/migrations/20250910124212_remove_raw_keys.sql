-- +goose Up
-- +goose StatementBegin
ALTER TABLE "public"."access_tokens" DROP COLUMN "access_token";
ALTER TABLE "public"."team_api_keys" DROP COLUMN "api_key";
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE "public"."access_tokens" ADD COLUMN "access_token" TEXT;
ALTER TABLE "public"."team_api_keys" ADD COLUMN "api_key" TEXT;
-- +goose StatementEnd
