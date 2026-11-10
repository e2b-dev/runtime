-- +goose Up
-- NULL max_vcpu marks a fixed-size template. Otherwise max_vcpu is the VM size
-- and vcpu the number of CPUs online at start.
SET LOCAL lock_timeout = '2s';

ALTER TABLE public.env_builds ADD COLUMN max_vcpu bigint;

-- +goose Down
SET LOCAL lock_timeout = '2s';

ALTER TABLE public.env_builds DROP COLUMN max_vcpu;
