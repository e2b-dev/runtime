-- name: GetUserIdentity :one
SELECT oidc_iss, oidc_sub, user_id, created_at, updated_at
FROM public.user_identities
WHERE oidc_iss = sqlc.arg(oidc_iss)::text
  AND oidc_sub = sqlc.arg(oidc_sub)::text;
