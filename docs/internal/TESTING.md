# Testing

In order to test different scenarios we are splitting tests on this areas:

- nucliadb/nucliadb/tests: [Workflow](../../.github/workflows/nucliadb.yml)

  - Storage: local
  - MainDB: local
  - Nodes: PyO3 binding
  - TXN: Local
  - Cache: Memory

- nucliadb/nucliadb/ingest: [Workflow](../../.github/workflows/nucliadb_ingest.yml)

  - Storage: GCS
  - MainDB: Redis/Tikv
  - Nodes: Mock node
  - TXN: Jetstream
  - Cache: Nats

- nucliadb/nucliadb/search: [Workflow](../../.github/workflows/nucliadb_search.yml)

  - Storage: GCS
  - MainDB: Redis
  - Nodes: 2 Nodes chitchat real cluster
  - TXN: Jetstream
  - Cache: Nats

- nucliadb/nucliadb/one: [Workflow](../../.github/workflows/nucliadb_one.yml)

  - Storage: GCS
  - MainDB: Redis
  - Nodes: 2 Nodes chitchat real cluster
  - TXN: Jetstream
  - Cache: Nats

- nucliadb/nucliadb/train: [Workflow](../../.github/workflows/nucliadb_train.yml)

  - Storage: GCS and local
  - MainDB: Redis and local
  - Nodes: Mock node and PyO3 binding
  - TXN: Jetstream and Local
  - Cache: Nats and Memory

- nucliadb/nucliadb/writer: [Workflow](../../.github/workflows/nucliadb_writer.yml)

  - Storage: GCS
  - MainDB: Redis
  - Nodes: Mock node
  - TXN: Jetstream
  - Cache: Nats

- nucliadb/nucliadb/reader: [Workflow](../../.github/workflows/nucliadb_reader.yml)
  - Storage: GCS
  - MainDB: Redis
  - Nodes: Mock node
  - TXN: Jetstream
  - Cache: Nats
