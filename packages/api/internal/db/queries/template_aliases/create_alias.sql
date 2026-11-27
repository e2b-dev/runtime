-- name: CreateTemplateAlias :exec
INSERT INTO "public"."env_aliases" (alias, env_id, is_renamable, namespace)
VALUES (@alias, @template_id, TRUE, sqlc.narg(namespace));