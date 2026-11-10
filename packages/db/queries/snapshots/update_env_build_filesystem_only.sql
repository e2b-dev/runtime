-- name: UpdateEnvBuildFilesystemOnly :exec
-- Records the kind a node captured when it differs from the kind the build was
-- created as: a powered-off guest has no memory to keep.
UPDATE "public"."env_builds"
SET filesystem_only = @filesystem_only
WHERE id = @build_id;
