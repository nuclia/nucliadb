CREATE TABLE index_requests (
    seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    received_at TIMESTAMP DEFAULT NOW()
);
