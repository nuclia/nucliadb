CREATE TABLE shards (
    id UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    kbid UUID NOT NULL
);

CREATE TYPE index_kind AS ENUM ('text', 'paragraph', 'vector', 'relation');

CREATE TABLE indexes (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shard_id UUID NOT NULL REFERENCES shards(id),
    kind index_kind NOT NULL,
    name TEXT,
    configuration JSON,
    UNIQUE (shard_id, kind, name)
);

CREATE TABLE merge_jobs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    index_id BIGINT NOT NULL REFERENCES indexes(id),
    retries SMALLINT NOT NULL DEFAULT 0,
    seq BIGINT NOT NULL,
    enqueued_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    running_at TIMESTAMP
);

CREATE TABLE segments (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    index_id BIGINT NOT NULL REFERENCES indexes(id),
    seq BIGINT NOT NULL,
    records BIGINT,
    size_bytes BIGINT,
    merge_job_id BIGINT REFERENCES merge_jobs(id) ON DELETE SET NULL,
    delete_at TIMESTAMP DEFAULT NOW() + INTERVAL '5 minutes'
);

CREATE TABLE deletions (
    index_id BIGINT NOT NULL REFERENCES indexes(id),
    seq BIGINT NOT NULL,
    keys TEXT[] NOT NULL,
    PRIMARY KEY (index_id, seq)
);
