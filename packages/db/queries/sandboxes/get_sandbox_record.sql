-- name: GetSandboxRecordByTeamAndSandboxID :one
SELECT
    sl.sandbox_id,
    COALESCE(s.base_env_id, sl.env_id) AS template_id,
    sl.vcpu,
    sl.ram_mb,
    sl.total_disk_size_mb,
    sl.started_at,
    sl.stopped_at,
    c.sandbox_proxy_domain AS domain,
    COALESCE(template_alias.alias, '') AS alias
FROM billing.sandbox_logs sl
LEFT JOIN public.teams t ON t.id = sl.team_id
LEFT JOIN public.clusters c ON c.id = t.cluster_id
LEFT JOIN public.snapshots s
    ON s.sandbox_id = sl.sandbox_id
   AND s.team_id = sl.team_id
LEFT JOIN LATERAL (
    SELECT ea.alias
    FROM public.env_aliases ea
    WHERE ea.env_id = COALESCE(s.base_env_id, sl.env_id)
    ORDER BY ea.alias
    LIMIT 1
) template_alias ON TRUE
WHERE sl.team_id = sqlc.arg(team_id)::uuid
  AND sl.sandbox_id = sqlc.arg(sandbox_id)::text
ORDER BY sl.started_at DESC
LIMIT 1;
