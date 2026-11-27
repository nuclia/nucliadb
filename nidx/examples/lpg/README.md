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
| `BulkLoad` | Load nodes and edges with endpoint-label references, without rewriting referenced nodes. |
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
checks the indexer acknowledgement for each batch without waiting for search
visibility or reading records back. Keep the printed `graph_id` for queries
or rerunning with `--graph-id`. Failed loads can leave partially applied data.
For resumable loading, add `--checkpoint /path/to/load.sqlite` and keep that
file; restart with the same arguments plus `--resume`. An uncertain in-flight
batch requires confirming its previous RPC has finished before adding
`--retry-uncertain`. See the reference for details and existing-load recovery.
Nodes are submitted once, before edges; later edge batches carry only endpoint
IDs and labels. Bulk callers must preserve existing node labels and edge types:
unlike ordinary mutations, this trusted path does not read or validate stored types.

See [dataset options](REFERENCE.md#synthetic-jsonl-dataset) and
[loading details](REFERENCE.md#loading-jsonl-into-rust-nidxlpg) for embeddings,
batch sizing, timeouts, progress, and retry semantics.

### Benchmark sampled-chunk self-retrieval

```sh
.venv/bin/python -m nidx.examples.lpg.benchmark \
  --graph-id "$GRAPH_ID" --tenant "$TENANT_ID" \
  --case python/search-
```

Each search case randomly chooses a sampled node and one of its property chunks.
Text search uses the complete chunk; vector search reuses its stored embedding;
hybrid search uses both from the same chunk. All restrict results to that
property, request the top 10, and check that the source node/property/chunk is
returned, without filtering to the source node's ID. The
`python/search-vector-exact` case additionally sets minimum cosine to `0.99`;
the other cases retain their default score thresholds.

Fetching chunks and embeddings happens outside timings. Every warmup and
measurement within a case uses a different node, sampled without replacement.
Cases share samples for comparison, and `--seed` makes sampling reproducible.
The output reports the source chunk's returned rank; missing self-matches count
as failures, not successful empty results. Approximate search, common text, and
identical-vector ties can affect retrieval. Text-only runs do not require
embeddings; vector and hybrid runs do, and `--vector-dimension` must match the graph.

Select case groups with `--case python/` or `--case cypher`, or combine
names and prefixes (for example `--case python/ cypher/lookup`). Exact names
take precedence over prefix matching, and overlapping selections run each
case only once.

Search returns up to `top_k` hits, independent of `max_records` or the total
number of matches. Hybrid ranking uses bounded candidate windows internally;
filling a window is not an error. Rebuild the searcher and API, deploying the
searcher first, and regenerate Python bindings after upgrading.

For large property-filtered enumerations, use `graph.find_page(...,
page_size=100, page_token=token)` and continue with `page.next_page_token`.
The existing `graph.find(...)` still returns a sorted list bounded by
`max_records`. See the [API reference](REFERENCE.md#python-api) for pagination
and search details.

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

The interpreter finds start nodes with `FindRecords`, delegates neighborhoods,
bounded traversals and joins to batched `Expand` calls, and delegates shortest
paths to `ShortestPath`. Aggregation and edge-property filtering still run in
Python over `FindRecords`.
See [supported syntax and execution limits](REFERENCE.md#cypher-feasibility-example).

## Python facade and demo

`Graph` provides Python methods for record operations, typed filtering,
text/vector search, one-hop expansion, shortest paths, and explicit deletion
through the Rust service. `graph.expand(...)` returns start/edge/neighbor
bindings; `graph.shortest_path(...)` returns an ordered path or `None`.
See [Python graph reads](REFERENCE.md#expansion-and-shortest-paths) for examples.
To run nidx locally, start PostgreSQL on `127.0.0.1:55432` first, then run:

```sh
./nidx/examples/lpg/run_local.sh
```

The script runs the API, indexer and searcher in the foreground. It defaults to
the `postgres` database and stores segments under `~/nidx-lpg`; override these
with `METADATA__DATABASE_URL` and `NIDX_LPG_DATA_DIR` if needed. Keep both the
database and segment directory to preserve graphs across restarts. The services
listen on all interfaces without authentication, so use only a trusted host.

In another terminal, run the demo:

```sh
.venv/bin/python -m nidx.examples.lpg.demo --address 127.0.0.1:10000
```

The demo creates an illustrative Progress organization graph and prints its
tenant and shard IDs, property discovery, text/vector/hybrid search, expansion,
shortest paths, seven Cypher query shapes, and explicit node deletion.
Reporting links and other details are illustrative; the toy vectors demonstrate
search mechanics, not semantic relevance. Graph data lives in the service, not
in a local Python directory. See the
[Python API examples](REFERENCE.md#python-api) for direct use.

## Important limitations

- **Trusted/internal use only:** the Rust API adds no authentication or authorization.
- **Coordinate writers externally:** use one Rust API writer instance per graph.
- **No snapshots or graph transactions:** concurrent writes can change query
  results, ordered batches are not atomic, and timeouts do not imply rollback.
- **Client-managed retries:** mutations have no persisted journal or automatic
  replay. Retry failed/uncertain operations before dependent writes.
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
