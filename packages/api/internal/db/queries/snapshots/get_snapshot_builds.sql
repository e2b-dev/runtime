-- name: GetSnapshotBuilds :many
SELECT s.env_id as template_id, eb.id as build_id, eb.cluster_node_id as build_cluster_node_id FROM  "public"."snapshots" s
JOIN "public"."active_envs" e ON e.id = s.env_id
LEFT JOIN "public"."env_build_assignments" eba ON eba.env_id = s.env_id AND eba.tag = 'default'
LEFT JOIN "public"."env_builds" eb ON eb.id = eba.build_id
WHERE s.sandbox_id = @sandbox_id
AND s.team_id = @team_id;
