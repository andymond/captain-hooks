-- External references occupy the job's byte slot with the UTF-8 delivery ID.
ALTER TABLE webhook_jobs ADD COLUMN IF NOT EXISTS external_archive boolean NOT NULL DEFAULT FALSE;
