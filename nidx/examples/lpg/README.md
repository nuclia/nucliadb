# LPG storage and hybrid-search spike

An experimental labeled property graph (LPG) layer on nidx, with typed
properties, text/vector search, graph expansion, and shortest paths.

The **Rust `NidxLPG` gRPC service** owns storage, indexing, and graph primitives.
The Python `Graph` facade, JSONL loader, and Cypher example all use that
service and its record format. This remains a feasibility spike, not a
production-ready graph database.

## Table of contents

- [Capabilities](#capabilities)
- [Quick start](#quick-start)
- [Generate and load sample data](#generate-and-load-sample-data)
- [Try the Cypher example](#try-the-cypher-example)
- [Python facade and demo](#python-facade-and-demo)
- [Important limitations](#important-limitations)
- [Detailed reference](#detailed-reference)

## Capabilities

The Rust service exposes:

| RPC | What it does |
| --- | --- |
| `LPGOperation` | Ordered mutations: upsert nodes/edges, delete records, and patch text or embeddings. |
| `BulkLoad` | Load self-contained batches of nodes and edges. |
| `GetRecords` / `FindRecords` | Read records by key or filter by labels, properties, endpoints, and edge type. |
| `Expand` | Batched, filtered one-hop traversal with incoming, outgoing, or both directions. |
| `ShortestPath` | Unweighted bidirectional BFS, with node/edge filters and explicit work limits. |
| `SearchChunks` | Text, vector, or reciprocal-rank-fused hybrid search over node/edge text properties. |

Nodes have labels; edges have their own IDs, one type, and source/target nodes.
Parallel edges are supported. Text properties contain ordered chunks with
optional caller-supplied embeddings.

## Quick start

Run commands from the repository root. You need Docker, Cargo, the nidx Rust
build prerequisites, and the workspace Python dependencies.

```sh
uv sync --group nidx
.venv/bin/python -m pytest nidx/examples/lpg/tests/test_cypher_integration.py -q
.venv/bin/python -m pytest nidx/examples/lpg/tests/test_storage.py -q
```

This is the easiest way to try the real service: the tests start disposable
PostgreSQL and real API/indexer/searcher services, populate isolated graphs,
and clean up afterward. They cover the seven Cypher examples and the
`Expand`/`ShortestPath` RPCs, plus the Graph facade's storage and search
behavior. No existing deployment is required.

For offline loader/generator tests:

```sh
.venv/bin/python -m pytest \
  nidx/examples/lpg/tests/test_load_data.py \
  nidx/examples/lpg/tests/test_generate_data.py -q
```

## Generate and load sample data

The [generator](generate_data.py) produces an inventory graph with products,
warehouses, clients, and their relationships:

```sh
.venv/bin/python -m nidx.examples.lpg.generate_data \
  --output /tmp/lpg-synthetic.jsonl
```

Generation is offline and does not overwrite existing files. Add
`--vector-dimension 384` to include random unit vectors; these test vector
mechanics, **not semantic relevance**.

To load the file, first start a configured Rust NidxLPG deployment
([service setup](REFERENCE.md#nidxlpg-record-lifecycle-rust)), then run:

```sh
.venv/bin/python -m nidx.examples.lpg.load_data \
  --address 127.0.0.1:10000 --input /tmp/lpg-synthetic.jsonl
```

Use the **API** address, not the indexer/searcher address. By default, the
loader creates a fresh tenant and dedicated shard, prints their IDs, and
verifies each batch by reading it back. Keep the printed `graph_id` for queries
or rerunning with `--graph-id`. Failed loads can leave partially applied data.

See [dataset options](REFERENCE.md#synthetic-jsonl-dataset) and
[loading details](REFERENCE.md#loading-jsonl-into-rust-nidxlpg) for embeddings,
batch sizing, timeouts, progress, and retry semantics.

## Try the Cypher example

The [Python interpreter](cypher.py) recognizes the seven shapes in
[queries.md](queries.md): property lookup, neighborhoods, bounded traversal,
joins, relationship filtering, grouping/ranking, and shortest paths.
It is a restricted read-only subset, not a general Cypher engine.

Against a populated Rust service:

```sh
.venv/bin/python -m nidx.examples.lpg.cypher \
  --address 127.0.0.1:10000 --graph-id "$GRAPH_ID" \
  --query 'MATCH (p:Product {category: $category}) RETURN p' \
  --parameters '{"category":"electronics"}'
```

The interpreter still performs graph orchestration in Python using
`FindRecords`/`GetRecords`; it is **not yet wired to `Expand` or `ShortestPath`**.
See [supported syntax and execution limits](REFERENCE.md#cypher-feasibility-example).

## Python facade and demo

`Graph` provides Python methods for record operations, typed filtering,
text/vector search, one-hop expansion, shortest paths, and cascade deletion
through the Rust service. `graph.expand(...)` returns start/edge/neighbor
bindings; `graph.shortest_path(...)` returns an ordered path or `None`.
See [Python graph reads](REFERENCE.md#expansion-and-shortest-paths) for examples.
Run the demo against a configured, trusted API endpoint:

```sh
.venv/bin/python -m nidx.examples.lpg.demo --address 127.0.0.1:10000
```

The demo creates a dedicated graph, prints its tenant and shard IDs, and
verifies replacement, reconnecting, and cascading deletion. Graph data lives
in the service, not in a local Python directory. See the
[Python API examples](REFERENCE.md#python-api) for direct use.

## Important limitations

- **Trusted/internal use only:** the Rust API adds no authentication or authorization.
- **Coordinate writers externally:** use one Rust API writer instance per graph.
- **No snapshots or graph transactions:** concurrent writes can change query
  results, deletion is staged, and timeouts do not imply rollback.
- **Client-managed retries:** mutations have no persisted journal or automatic
  replay. Retry failed/uncertain operations; retry node deletion to resume a cascade.
- **Bounded execution:** candidate, traversal, and response limits fail explicitly
  instead of silently returning incomplete results.
- **Limited Python facade:** `Graph` supports scalar, native `datetime`, and
  string-list properties, but not stored ranges. Filters support scalar equality,
  string-list membership, and numeric/date bounds;
  there is no general multi-hop traversal API.

## Detailed reference

Full examples, exact limits, configuration, and caveats are in
[REFERENCE.md](REFERENCE.md):

- [Rust record lifecycle and deployment](REFERENCE.md#nidxlpg-record-lifecycle-rust)
- [Expand](REFERENCE.md#batched-one-hop-expansion) and [shortest paths](REFERENCE.md#shortest-paths)
- [Cypher syntax and client-side execution](REFERENCE.md#cypher-feasibility-example)
- [JSONL loading](REFERENCE.md#loading-jsonl-into-rust-nidxlpg) and [stateless bulk semantics](REFERENCE.md#stateless-bulk-loading)
- [Public datasets and benchmark alternatives](REFERENCE.md#public-graph-datasets-and-benchmark-alternatives)
- [Python API](REFERENCE.md#python-api), [text/vector search](REFERENCE.md#text-and-vector-search), and [incremental embeddings](REFERENCE.md#adding-chunks-and-embeddings-later)
- [Storage mapping](REFERENCE.md#mapping-to-nidx), [test coverage](REFERENCE.md#verified-by-integration-tests), and [production gaps](REFERENCE.md#python-facade-and-production-gaps)
