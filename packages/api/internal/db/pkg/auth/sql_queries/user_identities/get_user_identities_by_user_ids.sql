-- name: GetUserIdentitiesByUserIDsAndIssuers :many
SELECT oidc_iss, oidc_sub, user_id
FROM public.user_identities
WHERE oidc_iss = ANY(sqlc.arg(oidc_issuers)::text[])
  AND user_id = ANY(sqlc.arg(user_ids)::uuid[])
ORDER BY user_id, oidc_iss, oidc_sub;
