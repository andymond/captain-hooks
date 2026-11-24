CREATE TABLE IF NOT EXISTS webhook_jobs (
    vendor varchar(63) NOT NULL,
    delivery_id varchar(128) NOT NULL,
    projected_body bytea,
    archive_delivery_id varchar(128),
    state varchar(10) NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'running', 'completed', 'failed')),
    attempts integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    lease_until timestamptz,
    lease_token uuid,
    last_error text,
    PRIMARY KEY (vendor, delivery_id),
    FOREIGN KEY (vendor, archive_delivery_id)
        REFERENCES webhook_deliveries (vendor, delivery_id) DEFERRABLE INITIALLY DEFERRED,
    CHECK (archive_delivery_id IS NULL OR archive_delivery_id = delivery_id),
    CHECK (state = 'completed' OR num_nonnulls(projected_body, archive_delivery_id) = 1)
);

CREATE INDEX IF NOT EXISTS webhook_jobs_pending
    ON webhook_jobs (vendor, available_at) WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS webhook_jobs_running
    ON webhook_jobs (vendor, lease_until) WHERE state = 'running';
