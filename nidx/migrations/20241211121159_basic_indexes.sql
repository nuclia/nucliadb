-- Indexes for FKs
CREATE INDEX ON indexes(shard_id);
CREATE INDEX ON segments(index_id);
CREATE INDEX ON segments(merge_job_id);

-- For deciding which indexes to sync
CREATE INDEX ON indexes(updated_at);

-- For taking jobs from the queue
CREATE INDEX ON merge_jobs(priority);

-- For purge jobs
CREATE INDEX ON indexes(deleted_at);
CREATE INDEX ON segments(delete_at);

ANALYZE;
