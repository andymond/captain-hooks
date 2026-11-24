CREATE TABLE IF NOT EXISTS webhook_deliveries (
    vendor varchar(63) NOT NULL,
    delivery_id varchar(128) NOT NULL,
    body bytea NOT NULL,
    received_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (vendor, delivery_id)
);
