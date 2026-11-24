-- Stop workers using the old webhook_jobs table and apply 002_webhook_jobs.sql first.
-- Keep each vendor pointed at the archive its existing references use.
INSERT INTO captain_webhook_jobs
    (vendor, delivery_id, input_kind, body, revision, attempts, due_at, leased, last_error)
SELECT
    j.vendor,
    j.delivery_id,
    CASE WHEN j.archive_delivery_id IS NOT NULL OR coalesce(to_jsonb(j)->>'external_archive', 'false') = 'true'
        THEN 'ARCHIVE' ELSE 'PROJECTED' END,
    CASE WHEN coalesce(to_jsonb(j)->>'external_archive', 'false') = 'true'
        THEN NULL ELSE j.projected_body END,
    md5(j.vendor || ':' || j.delivery_id),
    j.attempts,
    CASE
        WHEN j.state IN ('completed', 'failed') THEN NULL
        WHEN j.state = 'running' THEN (extract(epoch FROM j.lease_until) * 1000)::bigint
        ELSE (extract(epoch FROM j.available_at) * 1000)::bigint
    END,
    FALSE,
    left(j.last_error, 255)
FROM webhook_jobs j
ON CONFLICT (vendor, delivery_id) DO NOTHING;
