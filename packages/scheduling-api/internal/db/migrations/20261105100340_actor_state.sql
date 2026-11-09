-- +goose Up
CREATE TYPE public.scheduling_actor_state AS ENUM ('starting', 'running', 'pausing', 'killing', 'snapshotting', 'gone');

-- Written when an actor starts, deleted when it ends. Only record changes
-- in place.
CREATE TABLE public.scheduling_actor (
    actor_id                   uuid        PRIMARY KEY,
    project_id                 uuid        NOT NULL,
    execution_id               text        NOT NULL,
    start_time                 timestamptz NOT NULL,
    node_id                    text        NOT NULL,
    cluster_id                 uuid        NOT NULL,
    base_template_id           text        NOT NULL,
    template_id                text        NOT NULL,
    max_instance_length        interval    NOT NULL,
    auto_pause                 boolean     NOT NULL,
    auto_pause_filesystem_only boolean     NOT NULL,
    metadata                   jsonb       NOT NULL,
    record                     jsonb       NOT NULL
)
WITH (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_insert_scale_factor = 0,
    autovacuum_vacuum_insert_threshold = 10000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 50000,
    autovacuum_vacuum_cost_delay = 0,
    vacuum_truncate = off
);

CREATE INDEX scheduling_actor_project_list_idx ON public.scheduling_actor (project_id, start_time DESC, actor_id DESC);
CREATE INDEX scheduling_actor_node_idx ON public.scheduling_actor (node_id);

-- The mutable half of an actor. The row exists from the reservation on,
-- before the scheduling_actor row, so it has no foreign key.
--
-- Index only actor_id and end_time, so that state changes stay HOT updates.
--
-- transition_action: 1 pause, 2 kill, 3 snapshot.
-- counter_slot names the scheduling_project_live_count row that counts this
-- actor.
CREATE TABLE public.scheduling_actor_run (
    actor_id          uuid        PRIMARY KEY,
    project_id        uuid        NOT NULL,
    state             public.scheduling_actor_state NOT NULL,
    end_time          timestamptz NOT NULL,
    revision          bigint      NOT NULL DEFAULT 1,
    reservation_id    uuid        NOT NULL,
    reserved_at       timestamptz NOT NULL,
    transition_id     uuid,
    transition_action smallint,
    transition_at     timestamptz,
    failure_message   text,
    failure_code      integer,
    gone_at           timestamptz,
    refused_until     timestamptz,
    refused_since     timestamptz,
    counter_slot      smallint    NOT NULL,
    CONSTRAINT scheduling_actor_run_transition_action_check CHECK (transition_action BETWEEN 1 AND 3),
    CONSTRAINT scheduling_actor_run_counter_slot_check CHECK (counter_slot BETWEEN 0 AND 63),
    CONSTRAINT scheduling_actor_run_gone_at_check CHECK ((state = 'gone') = (gone_at IS NOT NULL)),
    CONSTRAINT scheduling_actor_run_transition_check CHECK (num_nonnulls(transition_id, transition_action, transition_at) IN (0, 3)),
    CONSTRAINT scheduling_actor_run_in_flight_check CHECK (state NOT IN ('pausing', 'killing', 'snapshotting') OR transition_id IS NOT NULL)
)
WITH (
    fillfactor = 50,
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_insert_scale_factor = 0,
    autovacuum_vacuum_insert_threshold = 10000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 50000,
    autovacuum_vacuum_cost_delay = 0,
    vacuum_truncate = off
);

CREATE INDEX scheduling_actor_run_end_time_idx ON public.scheduling_actor_run (end_time);

-- Live actors of a project, split over slots. The project total is the sum
-- of its rows. slot has the same range as scheduling_actor_run.counter_slot.
CREATE TABLE public.scheduling_project_live_count (
    project_id uuid     NOT NULL,
    slot       smallint NOT NULL,
    live       integer  NOT NULL,
    PRIMARY KEY (project_id, slot),
    CONSTRAINT scheduling_project_live_count_slot_check CHECK (slot BETWEEN 0 AND 63),
    CONSTRAINT scheduling_project_live_count_live_check CHECK (live >= 0)
)
WITH (
    fillfactor = 20,
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_vacuum_cost_delay = 0,
    vacuum_truncate = off
);

-- +goose Down
DROP TABLE public.scheduling_project_live_count;
DROP TABLE public.scheduling_actor_run;
DROP TABLE public.scheduling_actor;
DROP TYPE public.scheduling_actor_state;
