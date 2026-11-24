CREATE TABLE IF NOT EXISTS captain_webhook_jobs (
    vendor varchar(63) NOT NULL,
    delivery_id varchar(128) NOT NULL,
    input_kind varchar(16) NOT NULL,
    body bytea,
    revision varchar(36) NOT NULL,
    attempts integer NOT NULL,
    due_at bigint,
    leased boolean NOT NULL,
    last_error varchar(255),
    PRIMARY KEY (vendor, delivery_id)
);

CREATE INDEX IF NOT EXISTS captain_webhook_jobs_due
    ON captain_webhook_jobs (vendor, input_kind, due_at);
