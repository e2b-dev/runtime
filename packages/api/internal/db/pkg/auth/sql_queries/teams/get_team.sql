-- Each lookup resolves the team and its limits in one statement: the pushed
-- project_limits row, else default_limits, which always holds one row. The
-- limit columns repeat across these queries on purpose, so a new limit needs no
-- migration recreating a view or function; TestTeamLookupsReturnEveryDefaultLimit
-- in pkg/tests fails when a lookup misses one.
-- name: GetTeamByAPIKey :one
SELECT t.id, t.created_at, t.is_blocked, t.name, t.email, t.is_banned, t.blocked_reason,
    t.cluster_id, t.sandbox_scheduling_labels, t.sso_organization_id, t.sso_auto_join,
    t.deleted_at, t.slug,
    ps.sandbox_default_allow_public_traffic,
    COALESCE(pl.max_length_hours, d.max_length_hours) AS max_length_hours,
    COALESCE(pl.concurrent_sandboxes, d.concurrent_sandboxes) AS concurrent_sandboxes,
    COALESCE(pl.concurrent_template_builds, d.concurrent_template_builds) AS concurrent_template_builds,
    COALESCE(pl.max_vcpu, d.max_vcpu) AS max_vcpu,
    COALESCE(pl.max_ram_mb, d.max_ram_mb) AS max_ram_mb,
    COALESCE(pl.disk_mb, d.disk_mb) AS disk_mb,
    COALESCE(pl.events_ttl_days, d.events_ttl_days) AS events_ttl_days,
    COALESCE(pl.default_free_disk_size_mb, d.default_free_disk_size_mb) AS default_free_disk_size_mb,
    COALESCE(pl.max_disk_size_mb, d.max_disk_size_mb) AS max_disk_size_mb,
    COALESCE(pl.max_disk_size_mb, d.max_disk_size_mb) AS max_free_disk_size_mb,
    COALESCE(pl.api_team_rps_list, d.api_team_rps_list) AS api_team_rps_list,
    COALESCE(pl.api_team_rps_delete, d.api_team_rps_delete) AS api_team_rps_delete,
    COALESCE(pl.max_secrets, d.max_secrets) AS max_secrets
FROM "public"."team_api_keys" tak
JOIN "public"."teams" t ON tak.team_id = t.id
CROSS JOIN "public"."default_limits" d
LEFT JOIN "public"."project_limits" pl ON pl.team_id = t.id
LEFT JOIN projection.project_settings ps ON ps.project_id = t.id
WHERE tak.api_key_hash = $1;

-- name: GetTeamByTeamAndUser :one
SELECT t.id, t.created_at, t.is_blocked, t.name, t.email, t.is_banned, t.blocked_reason,
    t.cluster_id, t.sandbox_scheduling_labels, t.sso_organization_id, t.sso_auto_join,
    t.deleted_at, t.slug,
    ut.role,
    ps.sandbox_default_allow_public_traffic,
    COALESCE(pl.max_length_hours, d.max_length_hours) AS max_length_hours,
    COALESCE(pl.concurrent_sandboxes, d.concurrent_sandboxes) AS concurrent_sandboxes,
    COALESCE(pl.concurrent_template_builds, d.concurrent_template_builds) AS concurrent_template_builds,
    COALESCE(pl.max_vcpu, d.max_vcpu) AS max_vcpu,
    COALESCE(pl.max_ram_mb, d.max_ram_mb) AS max_ram_mb,
    COALESCE(pl.disk_mb, d.disk_mb) AS disk_mb,
    COALESCE(pl.events_ttl_days, d.events_ttl_days) AS events_ttl_days,
    COALESCE(pl.default_free_disk_size_mb, d.default_free_disk_size_mb) AS default_free_disk_size_mb,
    COALESCE(pl.max_disk_size_mb, d.max_disk_size_mb) AS max_disk_size_mb,
    COALESCE(pl.max_disk_size_mb, d.max_disk_size_mb) AS max_free_disk_size_mb,
    COALESCE(pl.api_team_rps_list, d.api_team_rps_list) AS api_team_rps_list,
    COALESCE(pl.api_team_rps_delete, d.api_team_rps_delete) AS api_team_rps_delete,
    COALESCE(pl.max_secrets, d.max_secrets) AS max_secrets
FROM "public"."teams" t
JOIN "public"."users_teams" ut ON ut.team_id = t.id
CROSS JOIN "public"."default_limits" d
LEFT JOIN "public"."project_limits" pl ON pl.team_id = t.id
LEFT JOIN projection.project_settings ps ON ps.project_id = t.id
WHERE ut.user_id = $1 AND t.id = $2;

-- name: GetTeamByTeamID :one
SELECT t.id, t.created_at, t.is_blocked, t.name, t.email, t.is_banned, t.blocked_reason,
    t.cluster_id, t.sandbox_scheduling_labels, t.sso_organization_id, t.sso_auto_join,
    t.deleted_at, t.slug,
    ps.sandbox_default_allow_public_traffic,
    COALESCE(pl.max_length_hours, d.max_length_hours) AS max_length_hours,
    COALESCE(pl.concurrent_sandboxes, d.concurrent_sandboxes) AS concurrent_sandboxes,
    COALESCE(pl.concurrent_template_builds, d.concurrent_template_builds) AS concurrent_template_builds,
    COALESCE(pl.max_vcpu, d.max_vcpu) AS max_vcpu,
    COALESCE(pl.max_ram_mb, d.max_ram_mb) AS max_ram_mb,
    COALESCE(pl.disk_mb, d.disk_mb) AS disk_mb,
    COALESCE(pl.events_ttl_days, d.events_ttl_days) AS events_ttl_days,
    COALESCE(pl.default_free_disk_size_mb, d.default_free_disk_size_mb) AS default_free_disk_size_mb,
    COALESCE(pl.max_disk_size_mb, d.max_disk_size_mb) AS max_disk_size_mb,
    COALESCE(pl.max_disk_size_mb, d.max_disk_size_mb) AS max_free_disk_size_mb,
    COALESCE(pl.api_team_rps_list, d.api_team_rps_list) AS api_team_rps_list,
    COALESCE(pl.api_team_rps_delete, d.api_team_rps_delete) AS api_team_rps_delete,
    COALESCE(pl.max_secrets, d.max_secrets) AS max_secrets
FROM "public"."teams" t
CROSS JOIN "public"."default_limits" d
LEFT JOIN "public"."project_limits" pl ON pl.team_id = t.id
LEFT JOIN projection.project_settings ps ON ps.project_id = t.id
WHERE t.id = $1;

-- name: GetUserTeamsWithLimits :many
SELECT t.id, t.created_at, t.is_blocked, t.name, t.email, t.is_banned, t.blocked_reason,
    t.cluster_id, t.sandbox_scheduling_labels, t.sso_organization_id, t.sso_auto_join,
    t.deleted_at, t.slug,
    ut.is_default, ut.role,
    ps.sandbox_default_allow_public_traffic,
    COALESCE(pl.max_length_hours, d.max_length_hours) AS max_length_hours,
    COALESCE(pl.concurrent_sandboxes, d.concurrent_sandboxes) AS concurrent_sandboxes,
    COALESCE(pl.concurrent_template_builds, d.concurrent_template_builds) AS concurrent_template_builds,
    COALESCE(pl.max_vcpu, d.max_vcpu) AS max_vcpu,
    COALESCE(pl.max_ram_mb, d.max_ram_mb) AS max_ram_mb,
    COALESCE(pl.disk_mb, d.disk_mb) AS disk_mb,
    COALESCE(pl.events_ttl_days, d.events_ttl_days) AS events_ttl_days,
    COALESCE(pl.default_free_disk_size_mb, d.default_free_disk_size_mb) AS default_free_disk_size_mb,
    COALESCE(pl.max_disk_size_mb, d.max_disk_size_mb) AS max_disk_size_mb,
    COALESCE(pl.max_disk_size_mb, d.max_disk_size_mb) AS max_free_disk_size_mb,
    COALESCE(pl.api_team_rps_list, d.api_team_rps_list) AS api_team_rps_list,
    COALESCE(pl.api_team_rps_delete, d.api_team_rps_delete) AS api_team_rps_delete,
    COALESCE(pl.max_secrets, d.max_secrets) AS max_secrets
FROM "public"."teams" t
JOIN "public"."users_teams" ut ON ut.team_id = t.id
CROSS JOIN "public"."default_limits" d
LEFT JOIN "public"."project_limits" pl ON pl.team_id = t.id
LEFT JOIN projection.project_settings ps ON ps.project_id = t.id
WHERE ut.user_id = $1;
