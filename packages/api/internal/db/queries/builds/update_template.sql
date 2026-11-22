-- name: UpdateTemplateBuild :exec
UPDATE "public"."env_builds"
SET
    start_cmd = @start_cmd,
    ready_cmd = @ready_cmd,
    dockerfile = @dockerfile,
    cluster_node_id = @cluster_node_id,
    cpu_architecture = @cpu_architecture,
    cpu_family = @cpu_family,
    cpu_model = @cpu_model,
    cpu_model_name = @cpu_model_name,
    cpu_flags = @cpu_flags
WHERE
    id = @build_uuid;