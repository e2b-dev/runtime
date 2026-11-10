-- +goose Up
-- River's job tables live in their own schema, out of public. River's own
-- migrator creates them; this only gives it a place to put them.
CREATE SCHEMA river;

-- +goose Down
-- CASCADE because River migrates into this schema on its own stream, which
-- goose cannot unwind. Dropping the schema without its tables would fail once
-- River has run.
DROP SCHEMA river CASCADE;
