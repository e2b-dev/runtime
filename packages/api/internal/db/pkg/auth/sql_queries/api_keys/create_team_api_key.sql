-- name: CreateTeamAPIKey :one
INSERT INTO "public"."team_api_keys" (
    team_id,
    created_by,
    updated_at,
    api_key_hash,
    api_key_prefix,
    api_key_length,
    api_key_mask_prefix,
    api_key_mask_suffix,
    name,
    created_at
) VALUES (
    @team_id,
    @created_by,
    NOW(),
    @api_key_hash,
    @api_key_prefix,
    @api_key_length,
    @api_key_mask_prefix,
    @api_key_mask_suffix,
    @name,
    NOW()
) RETURNING *;
