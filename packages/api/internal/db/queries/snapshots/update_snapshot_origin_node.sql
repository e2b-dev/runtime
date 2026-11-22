-- name: UpdateSnapshotOriginNode :exec
-- Repoints a snapshot's origin node, used on resume to pin a retry to the node
-- whose local cache is warming after a previous resume attempt timed out.
UPDATE "public"."snapshots"
SET origin_node_id = @origin_node_id
WHERE sandbox_id = @sandbox_id;
