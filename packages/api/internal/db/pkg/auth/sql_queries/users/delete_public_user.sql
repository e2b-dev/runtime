-- name: DeletePublicUser :exec
DELETE FROM public.users
WHERE id = sqlc.arg(id)::uuid;
