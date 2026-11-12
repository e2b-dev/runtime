-- name: UpsertSnapshot :one
WITH new_template AS (
    INSERT INTO "public"."envs" (id, public, created_by, team_id, updated_at, source, cluster_id)
    SELECT @template_id, FALSE, NULL, @team_id, now(), 'snapshot', @cluster_id
    WHERE NOT EXISTS (
        SELECT id
        FROM "public"."snapshots" s
        WHERE s.sandbox_id = @sandbox_id
    ) RETURNING id
),

-- Create a new snapshot or update an existing one
snapshot as (
    INSERT INTO "public"."snapshots" (
       sandbox_id,
       base_env_id,
       team_id,
       env_id,
       metadata,
       sandbox_started_at,
       env_secure,
       allow_internet_access,
       origin_node_id,
       auto_pause,
       config,
       created_at
    )
    VALUES (
            @sandbox_id,
            @base_template_id,
            @team_id,
            -- If snapshot already exists, new_template id will be null, env_id can't be null, so use placeholder ''
            COALESCE((SELECT id FROM new_template), ''),
            @metadata,
            @started_at,
            @secure,
            @allow_internet_access,
            @origin_node_id,
            @auto_pause,
            @config,
            now()
   )
    ON CONFLICT (sandbox_id) DO UPDATE SET
        metadata = excluded.metadata,
        sandbox_started_at = excluded.sandbox_started_at,
        allow_internet_access = COALESCE(excluded.allow_internet_access, snapshots.allow_internet_access),
        origin_node_id = excluded.origin_node_id,
        auto_pause = excluded.auto_pause,
        config = excluded.config,
        -- An inserted row points at the env created above, which is live, so
        -- env_deleted_at stays NULL. An existing row keeps its env, and a soft
        -- delete of that env stamped it; this only fills a row the backfill
        -- has not reached. COALESCE keeps a stamp committed after this
        -- statement's snapshot was taken, which the subquery cannot see.
        env_deleted_at = COALESCE(
            snapshots.env_deleted_at,
            (SELECT e.deleted_at FROM "public"."envs" e WHERE e.id = snapshots.env_id)
        )
    RETURNING env_id as template_id
),

-- Create a new build for the snapshot, copying CPU info from the source build so
-- a pause keeps the snapshot's CPU compatibility pinned to the original build
-- instead of the node the pause happened to run on. Scalar subqueries are used so
-- the row is always inserted (CPU info is NULL if the source build is missing).
--
-- build_filesystem_only is the kind this build is taken as. It is separate from
-- config, whose kind describes the row's previous ready build until this one
-- finishes.
new_build as (
    INSERT INTO "public"."env_builds" (
        env_id,
        team_id,
        vcpu,
        ram_mb,
        free_disk_size_mb,
        kernel_version,
        firecracker_version,
        envd_version,
        status_group,
        status,
        cluster_node_id,
        total_disk_size_mb,
        updated_at,
        cpu_architecture,
        cpu_family,
        cpu_model,
        cpu_model_name,
        cpu_flags,
        max_vcpu,
        filesystem_only
    )
    VALUES (
        (SELECT template_id FROM snapshot),
        @team_id,
        @vcpu,
        @ram_mb,
        @free_disk_size_mb,
        @kernel_version,
        @firecracker_version,
        @envd_version,
        @status_group,
        -- Mirrors status_group until the status column is dropped.
        CASE @status_group
            WHEN 'pending' THEN 'waiting'
            WHEN 'in_progress' THEN 'building'
            WHEN 'ready' THEN 'uploaded'
            WHEN 'failed' THEN 'failed'
        END,
        @origin_node_id,
        @total_disk_size_mb,
        now(),
        (SELECT eb.cpu_architecture FROM "public"."env_builds" eb WHERE eb.id = @source_build_id),
        (SELECT eb.cpu_family FROM "public"."env_builds" eb WHERE eb.id = @source_build_id),
        (SELECT eb.cpu_model FROM "public"."env_builds" eb WHERE eb.id = @source_build_id),
        (SELECT eb.cpu_model_name FROM "public"."env_builds" eb WHERE eb.id = @source_build_id),
        (SELECT eb.cpu_flags FROM "public"."env_builds" eb WHERE eb.id = @source_build_id),
        (SELECT eb.max_vcpu FROM "public"."env_builds" eb WHERE eb.id = @source_build_id),
        @build_filesystem_only
    )
    RETURNING id as build_id
),

-- Create the build assignment edge (explicit, not relying on trigger)
build_assignment as (
    INSERT INTO "public"."env_build_assignments" (env_id, build_id, tag)
    VALUES (
        (SELECT template_id FROM snapshot),
        (SELECT build_id FROM new_build),
        'default'
    )
    RETURNING build_id, env_id as template_id
)

SELECT build_id, template_id FROM build_assignment;
