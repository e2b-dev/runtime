-- name: MarkSnapshotBuildEnvdUnresponsive :exec
-- Records that the build's memory snapshot was taken while the sandbox guest
-- agent did not answer the pause's health probe. Written on success only,
-- together with the build's terminal status.
UPDATE "public"."env_builds"
SET envd_unresponsive = TRUE
WHERE id = @build_id;
