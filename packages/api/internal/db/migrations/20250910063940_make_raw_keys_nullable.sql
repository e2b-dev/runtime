-- +goose Up
-- +goose StatementBegin
ALTER TABLE "public"."access_tokens" DROP CONSTRAINT access_tokens_pkey;
ALTER TABLE "public"."access_tokens" ADD PRIMARY KEY (id);
ALTER TABLE "public"."access_tokens" ALTER COLUMN "access_token" DROP NOT NULL;
ALTER TABLE "public"."team_api_keys" ALTER COLUMN "api_key" DROP NOT NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE "public"."access_tokens" DROP CONSTRAINT access_tokens_pkey;
UPDATE TABLE "public"."access_tokens" SET "access_token" = 'unknown' WHERE "access_token" IS NULL;
ALTER TABLE "public"."access_tokens" ALTER COLUMN "access_token" SET NOT NULL;
ALTER TABLE "public"."access_tokens" ADD PRIMARY KEY (access_token);
UPDATE TABLE "public"."team_api_keys" SET "api_key" = 'unknown' WHERE "api_key" IS NULL;
ALTER TABLE "public"."team_api_keys" ALTER COLUMN "api_key" SET NOT NULL;
-- +goose StatementEnd
