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

CREATE TABLE segments (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    index_id BIGINT NOT NULL REFERENCES indexes(id),
    ready BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP
);
