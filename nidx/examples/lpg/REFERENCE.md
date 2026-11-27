# LPG detailed reference

[Back to the overview and quick start](README.md).

This page covers API semantics, configuration, loading, storage internals,
examples, and limitations for the Rust `NidxLPG` service and its Python
`Graph` facade. Python delegates persistence and indexing to the service;
there is no separate Python storage implementation.

## Table of contents

- [NidxLPG record lifecycle (Rust)](#nidxlpg-record-lifecycle-rust)
  - [Batched one-hop expansion](#batched-one-hop-expansion)
  - [Shortest paths](#shortest-paths)
  - [Cypher feasibility example](#cypher-feasibility-example)
- [Run](#run)
- [Synthetic JSONL dataset](#synthetic-jsonl-dataset)
  - [Loading JSONL into Rust NidxLPG](#loading-jsonl-into-rust-nidxlpg)
  - [Stateless bulk loading](#stateless-bulk-loading)
  - [Public graph datasets and benchmark alternatives](#public-graph-datasets-and-benchmark-alternatives)
- [Python API](#python-api)
  - [Expansion and shortest paths](#expansion-and-shortest-paths)
  - [Text and vector search](#text-and-vector-search)
  - [Adding chunks and embeddings later](#adding-chunks-and-embeddings-later)
- [Mapping to nidx](#mapping-to-nidx)
- [Verified by integration tests](#verified-by-integration-tests)
- [Python facade and production gaps](#python-facade-and-production-gaps)

## NidxLPG record lifecycle (Rust)

The API listener also registers `NidxLPG` from `nidx/nidx_protos/nidx.proto`.
Configure `LPG__INDEXER_ADDRESS` and `LPG__SEARCHER_ADDRESS` as absolute gRPC
URIs, for example `http://127.0.0.1:10002` and `http://127.0.0.1:10001`
for a local deployment. With neither setting, LPG calls fail with
`FAILED_PRECONDITION`; partial configuration is invalid at startup. Existing
NidxApi calls are unchanged. The configured
indexer must run in gRPC mode (without `INDEXER__NATS_SERVER`): the current
NATS-mode indexer does not serve gRPC. Do not mix NATS-indexed and gRPC-indexed
workloads in the same deployment: they use different sequence sources. The API,
indexer and searcher must use
the same metadata and segment storage; the API does **not** upload to or need
access to the indexer's input object store. Treat both listeners as trusted
internal services; this API does not add authentication or authorization.

Create a dedicated shard using `NidxApi.NewShard`, and use the returned ID as
the opaque `graph_id`. Do not mix LPG records with other clients' resources in
that shard. Graph creation/deletion and graph-to-shard mapping are not yet
part of NidxLPG.

* `LPGOperation` is the normal mutation RPC. Its ordered batch supports full node
  and edge upserts, record/node deletion, and text-property patches. A batch
  contains 1–1000 operations and stops at the first failure: prior operations
  remain applied, the failed item reports its status and message, and later
  items are marked skipped. This is not an all-or-nothing transaction.
  Upserts fully replace a node or edge by its `(kind, id)` key. Nodes require
  one or more unique labels; edges have exactly one nonempty `edge_type` and
  cannot have node labels. Edges also require nonempty source and target.
  Endpoints must
  identify existing nodes in the same graph. This matches
  Neo4j's multi-label nodes and single-type relationships, except that this
  prototype requires every node to have a label (Neo4j permits unlabeled
  nodes). Node labels and edge type are immutable after creation; changing
  either requires deleting and recreating that record. Properties and edge
  endpoints remain mutable. `revision` is an opaque client value stored on
  upsert, not an optimistic concurrency token. Text properties contain ordered chunks and
  optionally one embedding per chunk. Vectorset selection uses the record's
  `vectorset`, or the shard's sole paragraph vectorset if it is empty.
  Each edge is also projected as one relation-index triple under `a/metadata`.
  Its `relation_label` is the edge type, and its source/target are resource
  nodes. Facets identify the edge owner and each endpoint's types:
  `/lpg/edge/{record_id}`, `/lpg/type/source/{type}`, and
  `/lpg/type/target/{type}`. The `{type}` segment is the type's UTF-8 bytes
  encoded as unpadded URL-safe base64. Edge type also has the facet
  `/lpg/type/edge/{type}`. `GraphSearch` can combine these `FacetFilter`s with
  a path query to filter traversals by source type, edge type, or target type.
  Immutable node types keep the endpoint-type facets stable; changing edge
  endpoints reprojects the relation.
* `GetRecords` returns found records in request order, omitting missing keys
  and preserving duplicate requested keys. Properties support string, int64,
  finite double, bool, string list, null, datetime string and stored ranges.
  Datetime values must be RFC 3339 instants (naive values are UTC); stored
  date-range properties remain opaque strings.
  The API delegates to the internal `NidxSearcher.LPGGetRecords` endpoint,
  which routes to a shard replica and hydrates records directly from its cached
  text index. Deploy the updated searcher before the API; regenerate Python
  bindings with `make protos` when calling this internal endpoint from Python.
  Chunk search still executes in the API for now.
* `FindRecords` finds nodes or edges by record-local scalar/date equality,
  string-list membership, numeric/date range filters, labels, and edge source, target, type or incident
  node. Parallel edges remain separate; a self-loop matches incident-node
  filtering once. It returns pages using an opaque search cursor;
  concurrent writes or searcher refreshes can change results between pages.
  Nulls and stored range properties are not filterable. Records written before
  this projection was added need a full upsert before they appear in finds.
  The API delegates to `NidxSearcher.LPGFindRecords`. The searcher queries the
  JSON index directly, collects a bounded page of resource IDs, and hydrates
  only that page from the text index, without a text-field prefilter or
  paragraph search. Collection retains only a bounded page per segment,
  but each page still visits all documents matching the JSON query; broad
  filters can remain expensive. Pages are ordered by resource UUID, with query-bound
  continuation tokens that survive segment merges and replica routing.
  Changing the graph or filters invalidates a token; page size may change.
  Pages may contain fewer records than requested to stay within 3 MiB of
  record data. A single oversized record fails explicitly.
  Restart in-flight pagination after upgrading: old paragraph-score tokens
  are not compatible. Pagination is still not a snapshot; concurrent writes
  can change results. Deploy the updated searcher before the API.
* `Expand` performs a batched, filtered one-hop expansion. See below for its
  directions, result bindings and explicit work/result limits.
* `ShortestPath` uses bounded bidirectional BFS on `Expand` to find one
  unweighted shortest path, with edge and node pre-filters.
* `SearchChunks` supports text, vector and reciprocal-rank-fused hybrid search,
  optional kind/property and record-local property filters, and `top_k`.
  The shard-local `LPGSearchChunks` endpoint ranks bounded candidate windows
  and hydrates only final hits. More matching chunks do not cause an error.
  A response over 3 MiB or a final candidate changing during hydration fails
  explicitly. Deploy the updated searcher before the API.

Within `LPGOperation`, edge deletion is idempotent and deletes only that edge;
node deletion rejects nodes with incoming, outgoing, or self-loop edges.
Clients must delete all incident edges before deleting the node. Ordered
operations in the same batch can delete edges before their node. The text-patch
operation adds, replaces or clears chunks and embeddings. Replacing chunks
without embeddings clears their old vectors; embedding-only patches require
existing chunks. Unchanged text properties retain vectors. This is a
read-modify-write, not a conditional update.

Mutations are serialized per graph **within one API instance**. Each ordinary
`LPGOperation` mutation is submitted directly to the indexer, then waits up to 30 seconds for its
envelope and JSON/paragraph projection to become visible (or absent for a
deletion). There is no persisted mutation journal or automatic replay, including
after a process restart. A timeout or downstream error is an uncertain outcome,
not a rollback; clients must retry the operation. An unrelated later mutation
does not retry earlier work. Resolve uncertain writes before submitting dependent
changes: a late completion can otherwise overwrite newer data. Operation batches
stop at the first failure and skip remaining operations, without rolling back
earlier ones.
This visibility check does not verify vector projections, and independent
reads and multi-page queries are not snapshots.
`BulkLoad` instead returns after indexer acknowledgement without waiting for
search visibility; see [stateless bulk loading](#stateless-bulk-loading).

These safeguards do **not** coordinate API replicas or direct external writes.
Use one API writer instance per graph and do not mix other writers in the
shard; multiple instances can race endpoint checks and deletion.
Existing Indexer request expiry and late completion can also break
ordering after prolonged failures. There are no conditional writes,
multi-record transactions, or cross-replica isolation. Errors from downstream
services are returned to clients. Records remain encoded in a stored Resource
text field, rather than a separate canonical record store.
Normal gRPC message size limits apply. The service itself does not provide Cypher.

### Batched one-hop expansion

`NidxLPG.Expand` accepts 1-1000 starting `node_ids`, a required direction
(`LPG_OUTGOING`, `LPG_INCOMING`, or `LPG_BOTH`), optional `edge_types` (OR),
`edge_filters` (AND), and optional `neighbor_label` and `neighbor_filters`
(AND). An empty type list matches all relationship types. Duplicate starting
IDs/types do not multiply results. Scalar/range predicates are typed, as with
`FindRecords`; integer and float predicates are not interchangeable. Null or
missing properties do not match. Text predicates also match members of stored
string lists. This is a typed LPG API, not the Cypher numeric comparison layer.

```python
response = client.Expand(
    nidx_pb2.ExpandRequest(
        graph_id=graph_id,
        node_ids=["p1", "p2"],
        direction=nidx_pb2.LPG_OUTGOING,
        edge_types=["WORKS_FOR"],
        edge_filters=[
            nidx_pb2.LpgPropertyFilter(property="remote", boolean=True),
            nidx_pb2.LpgPropertyFilter(
                property="hours",
                integer_range=nidx_pb2.LpgIntegerRange(lower=20),
            ),
        ],
        neighbor_label="Company",
        neighbor_filters=[nidx_pb2.LpgPropertyFilter(property="country", text="ES")],
        candidate_limit=1000,
        max_results=1000,
    ),
    timeout=30,
)
for match in response.matches:
    edge = response.edges[match.edge_index]
    neighbor = response.neighbors[match.neighbor_index]
    print(match.node_id, edge.key.id, neighbor.key.id)
```

Each match preserves the starting node, individual edge, and neighboring node.
Records are deduplicated into the `edges` and `neighbors` response tables, not
repeated for every binding. Parallel edges remain distinct. With `LPG_BOTH`,
an edge connecting two requested starts produces a binding from each endpoint;
a self-loop produces only one. Edge source/target retain their stored
orientation even for incoming matches. No output ordering is guaranteed.

Both limits default to 1000 and may be raised to 10000. `candidate_limit`
counts distinct edges matching endpoints, types and edge filters **before**
neighbor filtering; `max_results` counts the final bindings, not distinct
edges. `examined_edges` reports the number of candidate edges. Exceeding either
limit or the 3 MiB encoded response cap fails the entire call with
`RESOURCE_EXHAUSTED`; there is no truncation or continuation token.

The API delegates to `NidxSearcher.LPGExpand`, including batched and filtered
expansions. The searcher queries the cached relation index for adjacency and
edge types. If edge-property filters are present, it first obtains a bounded
set of matching incident edge resource IDs from JSON and intersects them with
the relation query. Neighbor labels and properties are evaluated in JSON
restricted to the candidate node IDs, not a graph-wide node scan.
Records are hydrated locally with `ExtractedTexts` in batches, sharing reads
for repeated IDs. No paragraph search or text-field prefilter is used.

Parallel edges retain their resource identities. Exact starting IDs are
verified against relation fast fields, so the generic graph searcher's
case/Unicode normalization cannot conflate LPG node IDs. The existing
`a/metadata` relation projection and `/lpg/edge/{rid}` facets are required;
records from before that projection was introduced need a full upsert.
No new adjacency store or index schema migration is required.

Missing starting nodes produce `NOT_FOUND` and missing neighbors `UNAVAILABLE`.
Like other LPG reads, expansion is not a snapshot. Mismatches between an edge's
JSON, relation, and text projections can fail with `UNAVAILABLE` during index
refreshes. Deploy the updated searcher before the API. The public Python
expansion API and request/response shapes are unchanged; regenerate bindings
with `make protos` only when calling the new internal RPC directly from Python.

The Cypher interpreter uses `Expand` for neighborhood, bounded-traversal and
join shapes, and the generated gRPC clients expose it directly.
Self-contained Python integration tests cover the RPC alongside the Cypher
queries:

```sh
.venv/bin/python -m pytest nidx/examples/lpg/tests/test_cypher_integration.py -q
```

After changing the protocol locally, `make protos` regenerates the Python
bindings. Cargo generates the Rust bindings automatically.

### Shortest paths

`NidxLPG.ShortestPath` returns one unweighted shortest path between two node
IDs. The API delegates to `NidxSearcher.LPGShortestPath`, which runs
bidirectional BFS against cached relation and JSON indexes, expanding the
smaller frontier one complete level at a time (in batches of 128 nodes).
Traversal retains only node IDs and parent edge resource UUIDs. It does not
call `Expand` or hydrate frontier records: text chunks and embeddings on
off-path records cannot trigger an expansion response byte-limit error.
Only the selected final path is hydrated through local `ExtractedTexts`.
The reverse search inverts outgoing/incoming direction; `LPG_BOTH` stays
undirected. Visited sets prevent cycles and self-loops from extending paths.
Parallel edge identities and original edge orientations are preserved; ties
between equally short valid paths are arbitrary.

```python
response = client.ShortestPath(
    nidx_pb2.ShortestPathRequest(
        graph_id=graph_id,
        source="p1",
        target="p3",
        direction=nidx_pb2.LPG_BOTH,
        edge_types=["KNOWS", "REPORTS_TO"],
        node_label="Person",
        node_filters=[nidx_pb2.LpgPropertyFilter(property="active", boolean=True)],
        node_filter_scope=nidx_pb2.LPG_ALL_NODES,
        max_hops=6,
        max_visited_nodes=1000,
        max_examined_edges=10000,
    ),
    timeout=30,
)
if response.found:
    print([node.key.id for node in response.nodes])
    print([edge.key.id for edge in response.edges])
```

`edge_types` are ORed, `edge_filters` are ANDed, and `node_label` plus
`node_filters` are ANDed, using the same typed predicates as `Expand`.
Filters restrict the search **before** choosing the shortest path: if the
unfiltered shortest route is invalid, a longer valid route can be returned.
The default `LPG_ALL_NODES` scope requires source, target and intermediates to
match. `LPG_INTERMEDIATE_NODES` exempts both endpoints, useful for differently
labelled endpoints. Node existence and filters are checked in JSON restricted
to candidate resource IDs before adding nodes to the frontier, so exempt endpoints remain reachable
from either search direction. Filters are local to each node/edge; whole-path
conditions and weighted or all-shortest-path searches are not supported.

The response contains ordered `nodes` and `edges` in source-to-target traversal
order. A successful path has one more node than edges. Equal endpoints return
a zero-edge path if allowed by the node filter scope. Missing endpoints yield
`NOT_FOUND`. If no valid path exists
within `max_hops`, `found=false` with empty record arrays is returned.

Zero-valued limits select defaults:

| Limit | Default | Maximum |
| --- | --- | --- |
| `max_hops` | 6 | 64 |
| `max_visited_nodes` | 1000 | 10000 |
| `max_examined_edges` | 10000 | 100000 |

Visited nodes count search entries across **both** directions, including each
root; a node reached from both sides counts twice. Examined edges accumulate
matching adjacency candidates, including repeat examinations and candidates later
rejected by node filters. Each adjacency batch retains the existing 10000-edge
candidate cap, but does not construct an `ExpandResponse` or apply its binding
and response-byte caps. Retained IDs and parent links are capped at 16 MiB of
identifier data, and the final hydrated response at 3 MiB. Exceeding work,
state or response limits fails with `RESOURCE_EXHAUSTED`, never a partial path
or a false "no path"; a complete BFS level must fit even if a meeting was
already encountered in an earlier batch. Like `Expand`, this is not a snapshot:
concurrent mutation can invalidate shortest-path guarantees or produce read
errors. The selected path is rechecked against hydrated records for identity,
orientation, types, and filters; a mismatch fails with `UNAVAILABLE`.
Use a stable graph when evaluating paths. Deploy the updated searcher before
the API. Public request/response shapes and Python calls are unchanged.

The Python Cypher interpreter calls this RPC for `shortestPath` queries.

### Cypher feasibility example

`cypher.py` implements the seven query shapes in `queries.md` as a restricted,
read-only Cypher subset against the **Rust NidxLPG gRPC service**. It is not a
general Cypher parser. Identifiers, labels, relationship types, property names,
parameter names, hop bounds, and literal LIMIT values may vary; keyword case
and whitespace between clauses are flexible. Other patterns, projections,
predicates and clauses are rejected rather than approximated. Identifiers are
case-sensitive ASCII names without quoting. Parameters are JSON scalar values;
`null` equality or comparisons do not match. Traversals require a positive
bounded hop range and distinct pattern variables. The shortest-path example
rejects identical start/end nodes, and grouping currently accepts only string
or null properties. See `queries.md` for the supported syntax.

Node property maps accept multiple parameterized equality predicates, combined
with AND. The join and relationship-filter shapes also accept `WHERE`
predicates joined by `AND`, using `=` for scalars or `>=` for numbers, on any
node or relationship variable bound by that shape. For example:

```cypher
MATCH (p:Person {country: $country, age: $age, active: $active, rating: $rating})
RETURN p
```

with `{"country": "ES", "age": 34, "active": true, "rating": 4.5}`, or:

```cypher
MATCH (a:Entity)-[r:RELATED_TO]->(b:Entity)
WHERE r.confidence >= $minimum AND r.origin = $origin
  AND r.evidence_count >= $evidence_count AND r.verified = $verified
  AND a.active = $active AND b.country = $country
RETURN a, r, b
```

String and boolean equalities are sent to LPG as conjunctive property filters
for the candidate node/edge. Numeric comparisons and hydrated endpoint
predicates are checked in Python. Integers and floats compare numerically,
while booleans are distinct from numbers. Missing/null properties do not match.
`OR`, arbitrary expressions and other comparison operators remain unsupported.

```sh
.venv/bin/python -m nidx.examples.lpg.cypher \
  --address 127.0.0.1:10000 --graph-id "$GRAPH_ID" \
  --query 'MATCH (p:Person {email: $email}) RETURN p' \
  --parameters '{"email":"alice@example.com"}'
.venv/bin/python -m pytest nidx/examples/lpg/tests/test_cypher.py -q
```

In Python, construct `Cypher(nidx_pb2_grpc.NidxLPGStub(channel), graph_id)` and
call `execute(query, parameters)`. The result is a list of rows keyed by Cypher
return expressions/aliases. Nodes and relationships are `LpgRecord` protobufs;
paths contain ordered `nodes` and `relationships`. CLI output uses protobuf
JSON (including protobuf's string encoding for int64). Parallel relationships
are distinct, and each relationship can occur at most once in a matched path.
`shortestPath` returns one shortest path for each matched endpoint pair; ties
and rows without `ORDER BY` have no guaranteed order.

Start nodes and edge-filter/aggregation candidates use `FindRecords` property,
label and type filters, with pagination followed. Neighborhoods, joins and
bounded path expansion call batched server-side `Expand` (a level at a time for
paths), and `shortestPath` calls `ShortestPath` per matched endpoint pair with
the query's hop bound. Server work limits (`RESOURCE_EXHAUSTED`) surface as
`QueryLimitExceeded`. Residual predicates, grouping and ranking run in Python. Numeric equality and edge
comparisons scan candidates locally because the indexed numeric predicates are
type-specific while Cypher compares integer and float values. The defaults cap
each execution at 10,000 fetched records, 10,000 RPCs, 1,000 output rows and
6 hops; constructor arguments can lower or raise them. A query LIMIT may not
exceed the result cap. Exceeding any cap raises `QueryLimitExceeded`, never a
silently incomplete result. A LIMIT may stop expansion early; the work cap
still applies to whole fetched pages. The RPC timeout applies to each call,
not the entire query.

This is a feasibility prototype, not an efficient graph query engine. Useful
missing server primitives include multi-hop expansion with path/relationship
uniqueness and result/work limits, server-side joins,
aggregation/grouping and ordering, and numeric predicates that compare int and
float consistently. A single server-side query/snapshot spanning the reads
would also avoid inconsistent results under concurrent writes. The current
client issues potentially many RPCs and neither its pages nor its multiple
lookups share a snapshot. Missing endpoints, RPC errors, and safety-limit
failures are surfaced. `test_cypher.py` uses a loopback mock gRPC service.

The Python integration test exercises all seven queries against a real NidxLPG
API, indexer and searcher. It starts its own disposable PostgreSQL container,
builds the `lpg_test_server` Rust example with Cargo, and launches real services
on dynamically allocated loopback ports:

```sh
.venv/bin/python -m pytest nidx/examples/lpg/tests/test_cypher_integration.py -q
```

The test creates a shard with a fresh tenant ID, populates a small graph,
executes the queries, and deletes that shard afterward. Fixtures shut down the
services and remove the container, including on failure. Docker, Cargo and the
nidx Rust build prerequisites are required; no external service configuration
or `LPG_TEST_API_ADDRESS` is needed.

`storage.py` provides the `Graph` Python facade for these RPCs.
`test_storage.py` exercises it against the real Rust service using the same
self-starting PostgreSQL/API/indexer/searcher fixture as the Cypher tests.
There is no embedded storage fallback or legacy-backed mock.

Rust validation from `nidx/`:

```sh
SQLX_OFFLINE=true cargo test -p nidx --no-default-features --lib api::lpg::tests
SQLX_OFFLINE=true cargo check -p nidx --no-default-features --test test_integration
# Set DATABASE_URL to a dedicated disposable PostgreSQL instance first.
SQLX_OFFLINE=true cargo test -p nidx --no-default-features --test test_integration integration::lpg:: -- --test-threads=1
```

The lifecycle integration suite previously passed against a disposable loopback
PostgreSQL 16 container; the deletion and retry tests require a
separate run. Never point them at a production or shared database.

## Run

From the repository root, use the workspace Python environment with
`nidx-protos`, `nucliadb-protos`, and `grpcio` installed.
The repository's `uv sync --group nidx` sets up the workspace. The Python
facade itself does not import the embedded binding. Docker, Cargo, the nidx
Rust build prerequisites, and workspace test dependencies are needed for tests.

The integration suites share a disposable, loopback-only PostgreSQL container
and real Rust API/indexer/searcher for each test session. Tests create isolated
graphs; fixtures stop the services and remove the container afterward:

```sh
.venv/bin/python -m pytest \
  nidx/examples/lpg/tests/test_storage.py \
  nidx/examples/lpg/tests/test_cypher_integration.py -q
.venv/bin/ruff check nidx/examples/lpg
.venv/bin/mypy --explicit-package-bases --follow-untyped-imports nidx/examples/lpg
```

For the standalone demo, first start a configured Rust NidxLPG deployment
as described in [service setup](#nidxlpg-record-lifecycle-rust), then use its
trusted API address:

```sh
.venv/bin/python -m nidx.examples.lpg.demo --address 127.0.0.1:10000
```

The demo creates a new tenant and shard with an illustrative Progress
organization, then shows property discovery, text/vector/hybrid search,
expansion, shortest paths, seven Cypher query shapes, and explicit node deletion.
Reporting links and other details are illustrative, and its toy vectors
demonstrate search mechanics rather than semantic relevance. It prints the tenant UUID and shard ID;
keep these to reconnect. Persistence and service lifecycle belong to the Rust
deployment, not the Python client. The connection is plaintext and has no
credentials; use only trusted internal endpoints or a secure tunnel.

## Synthetic JSONL dataset

Generate data offline using Faker (already in the workspace dev dependencies).
This does not contact a database, create a graph, or ingest anything. The
loader converts its output directly into Rust LPG records.

The example's `synthetic.jsonl` contains 384-dimensional random unit vectors,
one per text chunk. The generator itself still defaults to no embeddings;
pass `--vector-dimension 384` to reproduce the vector-enabled dataset.

```sh
.venv/bin/python -m nidx.examples.lpg.generate_data \
  --output nidx/examples/lpg/synthetic.jsonl
# Optional random unit vectors, one per text chunk:
.venv/bin/python -m nidx.examples.lpg.generate_data \
  --output /tmp/lpg-synthetic-vectors.jsonl --vector-dimension 384
```

Defaults: **1,000 products, 20 warehouses, 200 clients**, seed 42.
The generator streams all 1,220 nodes before 4,184 edges:
`Product -STOCKED_AT-> Warehouse`, `Client -BUYS-> Product`, and
`Warehouse -SUPPLIES-> Client`. Each relationship has its own ID and properties.
The final node of each label is reserved as isolated. Two extra `STOCKED_AT`
edges guarantee a parallel pair between `P00000001` and `W00000001`.
Random endpoint sampling is with replacement, so additional parallel edges
can occur. This is a functional synthetic workload, not a calibrated benchmark.

Use `--products`, `--warehouses`, `--clients`, `--stocked-per-product` (3),
`--buys-per-client` (5), `--supplies-per-warehouse` (10),
`--isolated-per-label` (1), and `--seed` to change the workload. Node counts
include isolated nodes. Zero relationship counts disable that relationship
family, including the extra parallel pair for `STOCKED_AT`. Counts must be
nonnegative and active edges require non-isolated endpoints. Existing output
files are never overwritten; the parent directory must already exist.
If generation fails, the output may be partial: only use files from a
successful run. IDs are stable within a dataset and not tenant-scoped; the
future ingester supplies the graph/tenant.

For a larger workload with 500,000 nodes and 1,999,985 edges:

```sh
.venv/bin/python -m nidx.examples.lpg.generate_data \
  --output nidx/examples/lpg/synthetic-500k-2m.jsonl \
  --products 350000 --warehouses 50000 --clients 100000 \
  --supplies-per-warehouse 9 \
  --max-node-properties 100 --max-edge-properties 50 \
  --vector-dimension 384
```

The optional `--max-node-properties` and `--max-edge-properties` caps include
text properties. Each record gets a random property count between its existing
count and the cap, adding strings, integers, floats, booleans, string lists,
and ISO date strings. Zero (the default) keeps the original properties.
Nonzero caps must be at least 10 for nodes and 5 for edges.
Additional property randomness does not change graph topology, text, or vectors.
Every existing text chunk receives its own embedding when vectors are enabled.
Expect tens of GB of JSONL at this scale. Both generation and loading use
bounded memory. Loading needs temporary disk space for a protobuf snapshot and
its SQLite indexes, in addition to the JSONL file; set `TMPDIR` to choose a
scratch filesystem with sufficient free space.

Each line is one node or edge JSON object. `properties` contains scalar values
and string lists. `text_chunks` maps property names to ordered text chunks.
Nodes carry `labels`; edges carry `source`, `edge_type`, and `target`.
These field names follow `Graph.upsert_node` / `upsert_edge` arguments;
the facade supports both scalars and string lists. The loader below maps directly
to the Rust RPC types.
This is **not** protobuf JSON or a Rust RPC payload.
There are no header or summary rows; the CLI prints counts to stdout separately.

Example shape (abbreviated):

```json
{"kind":"node","id":"P00000001","labels":["Product"],"properties":{"category":"electronics","price":149.5,"available":true},"text_chunks":{"description":["Office laptop for computing and connectivity."]}}
{"kind":"edge","id":"E0000000001","source":"P00000001","edge_type":"STOCKED_AT","target":"W00000001","properties":{"quantity":25},"text_chunks":{"note":["Regional warehouse shipment."]}}
```

Products have category-based descriptions and features, warehouses have
descriptions and handling instructions, clients have profiles, and edges have
shipment notes. Faker supplies fictional company names, cities, dates, and
filler sentences. Dates use fixed bounds rather than today's date.
The same seed, configuration, Python/Faker versions yield repeatable output.

`--vector-dimension` defaults to 0 (no `embeddings` key). When enabled,
`embeddings` maps each text property to one random normalized vector per chunk.
Vector randomness is independent of graph/text generation, so toggling vectors
preserves the rest of the dataset. Match the dimension to the target vectorset
when ingesting. Random vectors test storage, filtering and vector-search
mechanics, **not semantic relevance**. Real embeddings remain useful on
coherent synthetic category descriptions: a later embedding step can replace
the random vectors, using the same model for documents and queries. Random
Faker filler is not a substitute for a relevance-labelled evaluation corpus.

Focused offline validation (no Docker/PostgreSQL):

```sh
.venv/bin/python -m pytest nidx/examples/lpg/tests/test_generate_data.py -q
.venv/bin/ruff check nidx/examples/lpg/generate_data.py nidx/examples/lpg/tests/test_generate_data.py
.venv/bin/mypy --explicit-package-bases --follow-untyped-imports nidx/examples/lpg/generate_data.py
```

### Loading JSONL into Rust NidxLPG

`load_data.py` connects directly to the API gRPC listener using `nidx-protos`,
`nucliadb-protos` and `grpcio` from the workspace environment. It does not run
migrations. The Rust LPG service must already be
running with the indexer/searcher configuration described above.

From the repository root:

```sh
# Generate a tenant UUID, create a dedicated shard and load the example's synthetic.jsonl:
.venv/bin/python -m nidx.examples.lpg.load_data \
  --address 127.0.0.1:10000

# Optionally choose the tenant UUID explicitly:
.venv/bin/python -m nidx.examples.lpg.load_data \
  --address 127.0.0.1:10000 --tenant <tenant-uuid>

# Alternatively, load into an existing dedicated LPG shard:
.venv/bin/python -m nidx.examples.lpg.load_data \
  --address 127.0.0.1:10000 --graph-id <shard-uuid> \
  --input nidx/examples/lpg/synthetic.jsonl
```

Replace the address with your **API** listener, not the indexer or searcher.
The channel is plaintext and has no credentials; use only a trusted internal
endpoint (or an appropriate secure tunnel). `--tenant` and `--graph-id` are
mutually exclusive. Omitting both generates a fresh tenant UUID automatically
and creates a dedicated shard. `--tenant` also creates a new shard every run; after a
failure, use the printed `graph_id` with `--graph-id` to retry the same shard.
Never mix other resources or concurrent writers into the destination graph.
Upserts replace existing records with the same `(kind, id)`, including their
text and embeddings; loading is not additive merging or graph deletion.

The loader streams the file into a temporary, disk-backed SQLite store before
any remote writes, keeping only individual records and bounded SQLite caches
in memory. This immutable protobuf snapshot preserves record revisions and
supports indexed endpoint lookups without retaining all nodes in RAM.
It validates the whole file,
rejecting malformed rows, duplicate record keys, unsupported property types,
missing endpoints and inconsistent embedding dimensions. Endpoints must
appear as nodes in the same file, but may occur after their edges. Blank lines
and unknown fields are errors.
The loader submits every dataset record once, with all nodes before edges.
Edge requests carry deduplicated endpoint IDs and labels rather than repeating
node properties, text and embeddings. Endpoint labels have their own compact
SQLite column, so resolving a reference does not decode the full node.
Request-size preflight consumes and discards one batch at a
time; ingestion then regenerates batches lazily from the snapshot, always
processing nodes before edges. Only the current batches, not the complete
request list, are retained in RAM. Unique progress tracking is also disk-backed.
Without a checkpoint, the temporary store is removed on success or an exception.
With `--checkpoint`, the snapshot and progress are retained in that SQLite
file for resumption. Allow enough disk space for the protobuf snapshot and
SQLite indexes; `TMPDIR` selects the scratch directory for non-checkpointed
loads. Validation and request-size preflight must finish
before the first write, so large files have an initial preparation delay.
Progress starts immediately and emits a flushed JSON message on stderr
approximately every 5 seconds, including while SQLite or an RPC is busy.
Each phase also reports its start and completion (or failure): `reading`,
`validating_endpoints`, `building_indexes`, `preflight`, `creating_shard` or
`checking_shard`, any optional `startup_wait`, and `loading`.
Reading progress includes byte-based `percent`, `bytes_read`, `total_bytes`,
node/edge/record counts, elapsed time and records per second. Preflight reports
`batches_checked` and `checked_records`; endpoint references are not records.
String-list
properties (including product tags) use `LpgProperty.strings`; date strings
remain plain strings rather than being guessed as datetimes.

Optional embeddings are preserved as float32 vectors. A newly created shard
gets a cosine vectorset with the dimension inferred from every embedding in
the JSONL file, with no dimension flag needed on the loader. The example's
384-dimensional vectors therefore create a 384-dimensional index when loaded
into a new tenant. A file without embeddings
gets an empty 3-dimensional cosine vectorset because nidx requires at least one
vector index per shard. This does not generate or attach embeddings.
`--vectorset` defaults to `lpg-property-v1`. If you later add embeddings with
a different dimension, create a matching vectorset rather than using this
empty default index.
For an existing graph it must already exist and its dimension must match the
file; the service checks dimension compatibility when writing. The loader does
not modify existing vector-index configuration.

`--batch-size` defaults to 10 (allowed 1–1000), counting records to write.
Endpoint-label references do not count against this limit, so an edge can be
loaded with batch size 1.
Requests are additionally split at 3 MiB to stay below the normal 4 MiB gRPC
limit; a record plus its required endpoint references must fit together or preflight
fails. The server's projected-resource size limit still applies.
`--timeout` sets seconds per RPC, defaulting to `30 * batch-size + 60`.
Indexing acknowledgements remain sequential, but there is no search-visibility
wait after the writes.
This is a correctness-oriented spike loader, not a bulk-ingestion benchmark.

The loader pauses for 5 seconds after creating a new shard by default.
`--shard-wait <seconds>` changes or disables this startup delay; existing graphs do
not pause. The tenant and graph IDs print before any optional delay.
Bulk loading does not read existing records or require searcher readiness.
Callers guarantee that existing node labels and edge types do not change.

Each batch checks every operation status without reading records back or
waiting for search visibility. Progress and
the graph ID go to stderr; when creating a shard, the tenant UUID is also
printed before record writes. A final JSON object with graph ID, node count,
edge count and (for new shards) tenant UUID goes to stdout only after successful
indexer acknowledgement. Keep the tenant and graph IDs for later queries and experiments.
Periodic loading updates report `acknowledged`, `total` (nodes plus edges), overall
`percent`, `elapsed_seconds`, and average `records_per_second` since ingestion
started. `acknowledged` and the rate count unique dataset records.
`submitted_records` and `submitted_records_per_second` report actual acknowledged
writes; these now match the unique counts because endpoints are references.
The rates include write RPCs, but exclude
file validation, shard creation and the startup pause. Counts advance only after
a complete batch is acknowledged. Heartbeats continue while an RPC is in flight;
`in_flight_records` shows the pending batch size without counting it as ingested.
The final loading update always reports completion or failure, even when less
than 5 seconds have elapsed. Fast batches are coalesced rather than logged
individually. Stdout remains reserved for the final successful load summary.
The `acknowledged` field replaces the former `verified` field: completion now
means indexing was acknowledged, not that records are searchable.

#### Resuming a load

For a new load, create a checkpoint on a filesystem with enough free space:

```sh
.venv/bin/python -m nidx.examples.lpg.load_data \
  --address 127.0.0.1:10000 --input /tmp/lpg-synthetic.jsonl \
  --batch-size 100 --checkpoint /path/to/lpg-load.sqlite
```

Keep the checkpoint file and the original JSONL. It contains the validated
protobuf snapshot, node labels, tenant/shard identity, API address, and the
last fully acknowledged record count. The loader commits an in-flight marker
before each RPC and advances the count only after all results are acknowledged.
Only one process can use a checkpoint at a time. Do not edit the file or
write to the graph concurrently.

To resume, use the same input, address and checkpoint; `--batch-size` can change:

```sh
.venv/bin/python -m nidx.examples.lpg.load_data \
  --address 127.0.0.1:10000 --input /tmp/lpg-synthetic.jsonl \
  --batch-size 500 --checkpoint /path/to/lpg-load.sqlite --resume
```

The checkpoint supplies the original tenant and graph IDs. It reuses the
snapshot and skips acknowledged records in node-then-edge order, so it does not
reparse the JSONL or rewrite acknowledged batches. It checks the input's
resolved path, file identity, size and modification/change timestamps, and
rejects changed or replaced files. The initial validation computes a SHA-256
digest; the stored snapshot and digest are reused on resume, not rehashed.
Use a trusted checkpoint and do not modify the source file during a load.

If interrupted with a batch in flight, its outcome is uncertain: it may have
been partly or fully indexed after the client stopped. Wait until that prior
request is definitely finished and resolve any competing writers, then add
`--retry-uncertain`. The entire uncertain batch is re-upserted, so this is
at-least-once, not exactly-once. Without that explicit confirmation, resume
stops before writing. A crash during shard creation can leave the shard ID
unknown; find the created shard externally and pass `--graph-id` on resume.
The loader verifies it belongs to the checkpoint's tenant before writing.
If preparation was interrupted before the graph or tenant was recorded, pass
the original `--tenant` or `--graph-id` again.

To adopt a load started *without* a checkpoint, first stop the old loader and
ensure its last in-flight RPC has finished. Supply a **trusted lower bound**
from its last `acknowledged` log entry (not its reading `bytes_read`) and the
existing graph and tenant IDs:

```sh
.venv/bin/python -m nidx.examples.lpg.load_data \
  --address 127.0.0.1:10000 --input /tmp/lpg-synthetic.jsonl \
  --graph-id <existing-graph-id> --expected-tenant <existing-tenant-id> \
  --checkpoint /path/to/lpg-load.sqlite --resume-after <acknowledged-count> \
  --retry-uncertain --batch-size 500
```

This manually supplied count is **not independently verified** against the
shard. An overestimate skips data; use 0 if unsure. The first run creates a
snapshot and validates the whole file, and later resumes use `--resume`
instead of `--resume-after`. Subsequent uncertain batches again require
`--retry-uncertain`.

A raw JSONL byte offset is not a safe ingestion checkpoint: file reading is
a separate validation phase, and records are ingested with nodes before edges
even if the file is interleaved. The reading offset may be at EOF before any
record is indexed. A seek alone also loses endpoint labels, and cannot tell
whether an interrupted batch completed. The snapshot retains this information
and lets SQLite skip to the acknowledged position in ingestion order.

On validation, transport or write-result failure, the command
exits nonzero without automatic retries. Batches are not transactions:
previously applied records remain, and an RPC timeout leaves the current
batch's outcome uncertain. No shard is deleted on failure. Search queries
immediately after a successful load may still return incomplete results.

### Stateless bulk loading

The loader exclusively uses `BulkLoad(graph_id, records, endpoint_labels)`. There is no
`--bulk-load` switch, session ID, Begin/End RPC or normal-ingestion mode in
the loader. The server's normal `LPGOperation` API remains available to other
clients with endpoint validation, visibility waits, and client-managed retries.

Each edge endpoint must either be a node in `records` or have an entry in
`endpoint_labels`, mapping its ID to `LpgStringList.values` containing its labels.
References are only used to project relation facets; they are not node upserts.
Callers must index referenced nodes first and supply their correct immutable labels.
The server validates reference IDs and unique, nonempty labels, and rejects
references that disagree with nodes in the same request. It does not read
stored records to validate endpoint existence or immutable types.
All structure, property, text/vector and reference checks complete before any
new indexing. A request accepts 1–1000 records and up to 2000 endpoint references, rejects
duplicate `(kind, id)` keys, and writes
nodes before edges regardless of input order. Results follow input order.

Only explicitly submitted records are reindexed and fully replaced, including
their properties, text and embeddings. Existing self-contained requests remain
supported, but repeated node records in such requests are still full upserts.
Use only a dedicated graph without concurrent/external writers; callers must
not change node labels or edge types through this trusted path.
There are no persisted bulk sessions, declarations, or pending-recovery
records in PostgreSQL. PostgreSQL is still used for ordinary nidx index
metadata and vector configuration.

The server projects all records before writing, then groups their resources into
`NidxIndexer.IndexResources` requests bounded by the 4 MiB encoded protobuf limit.
Each group builds shared text, paragraph, JSON, relation and vector segments,
rather than separate segments per record. Vector segment tags remain isolated.
Original resource IDs and per-resource replacement/deletion keys are preserved.
Each group uses one indexing sequence and publishes all its segments and
deletions together in one metadata transaction. No LPG reference data is persisted.

Groups are submitted sequentially; the server returns once the indexer has
acknowledged them all. It does not poll record, JSON, relation or vector search
visibility. A build/upload failure publishes none of that group's indexes, but
earlier groups remain applied. An RPC failure after publication can leave the
outcome uncertain. There is no request-wide rollback or automatic recovery, and
a later bulk call does not replay previous work.
The loader checks each batch's operation statuses and stops on any failure;
it does not read records back. Search visibility remains asynchronous.
Without a checkpoint, rerunning with `--graph-id` re-upserts all data.

Rebuild/restart nidx and regenerate Python bindings for the updated contract:

```sh
uv sync --reinstall-package nidx_protos --inexact
```

LPG requires no additional PostgreSQL tables or migrations. This spike assumes
a fresh database; databases using earlier LPG migrations are not supported.
This protocol does not add cross-replica writer isolation.

Focused validation uses a disposable loopback gRPC test server implementing
the actual generated service contract; it does not start Rust nidx/PostgreSQL
or establish live Rust interoperability:

```sh
.venv/bin/python -m pytest nidx/examples/lpg/tests/test_load_data.py nidx/examples/lpg/tests/test_generate_data.py -q
.venv/bin/ruff check nidx/examples/lpg/load_data.py nidx/examples/lpg/tests/test_load_data.py
.venv/bin/mypy --explicit-package-bases --follow-untyped-imports nidx/examples/lpg/load_data.py
```

### Public graph datasets and benchmark alternatives

| Dataset | Why use it | Format / caveat |
| --- | --- | --- |
| [LDBC SNB](https://ldbcouncil.org/benchmarks/snb/datasets/) | Best next step for a standard property-graph workload: social entities and relationships with published interactive/BI workloads. Start with Interactive v1 SF0.1. | Downloadable compressed CSV variants, plus a generator; keep the chosen version/format aligned with its driver. Synthetic, not captured social data. |
| [LDBC FinBench](https://ldbcouncil.org/benchmarks/finbench/) | Financial graph benchmark for neighbourhood reads and ongoing inserts/deletes; good for temporal edges and transaction-like graph workloads. | [Public datasets](https://ldbcouncil.org/benchmarks/finbench/datasets/) and [Spark generator](https://github.com/ldbc/ldbc_finbench_datagen) (generator: Apache-2.0). Domain differs from inventory. |
| [LDBC Graphalytics](https://ldbcouncil.org/benchmarks/graphalytics/) | Standard graph-analysis workloads with reference outputs, useful for traversal/topology stress. | [Dataset catalog](https://ldbcouncil.org/benchmarks/graphalytics/datasets/) has multiple sizes and text/Parquet downloads; not a rich property/text search benchmark. |
| [SNAP Amazon0302](https://snap.stanford.edu/data/amazon0302.html) | Real product co-purchasing topology: 262,111 nodes and 1,234,877 directed edges, with a research citation. | [Compressed edge list](https://snap.stanford.edu/data/amazon0302.txt.gz); no warehouse/customer records or rich product text. Properties would need separate enrichment. |
| [TPC-H](https://www.tpc.org/tpch/) | Established business decision-support benchmark; products, suppliers, customers, orders and line items are close to the enterprise domain. | Relational generator/specification, not a native LPG dataset. A graph mapping changes the benchmark; do not report adapted runs as official TPC-H results. |

For this spike, use Faker for predictable functional cases, then SNB for a
standard LPG workload or Amazon for real topology. Dataset conversion and
benchmark drivers are separate work; having benchmark data alone does not make
a run benchmark-compliant. Check each artifact's licence/terms and preserve
required citations before downloading or redistributing data (do not assume
the generator's licence covers every dataset). No external dataset is
downloaded automatically.

## Python API

```python
from uuid import uuid4

from nidx.examples.lpg.storage import Graph, NidxClient, PropertyFilter

nidx = NidxClient("127.0.0.1:10000")
try:
    graph = Graph.create(nidx, str(uuid4()))
    graph.upsert_node("P1", ["Product"], {"price": 150, "available": True})
    graph.upsert_node("W1", ["Warehouse"], {"country": "Spain"})
    graph.upsert_edge("E1", "P1", "STOCKED_AT", "W1", {"quantity": 10})

    product = graph.get("node", "P1")
    products = graph.find("node", PropertyFilter("price", "range", lower=100))
    edges = graph.topology(PropertyFilter("quantity", "range", lower=5))

    reopened = Graph(nidx, graph.tenant, graph.shard_id)
    reopened.delete_edge("E1")
    reopened.delete_node("P1")
finally:
    nidx.close()
```

Properties accept strings, booleans, int64 integers, finite floats, `None`,
native Python `datetime` objects, and `list[str]`:

```python
from datetime import datetime, timezone

graph.upsert_node("event", ["Event"], {
    "starts_at": datetime(2026, 12, 1, 10, tzinfo=timezone.utc),
    "tags": ["conference", "python"],
})
```

Datetimes are stored as ISO 8601 strings in the dedicated gRPC datetime field
and read back as `datetime` objects. Naive values remain naive; aware values
preserve their UTC offset and microseconds, not their named timezone or `fold`.
Malformed datetime strings written by raw clients raise `ValueError` on read.
String lists preserve order, duplicates, and empty values, including empty lists.
Stored ranges remain unsupported. `PropertyFilter` supports scalar/date equality,
string-list membership (a string equality predicate), and inclusive numeric/date
bounds. Datetime comparisons use UTC instants; naive datetimes are interpreted
as UTC, never the machine's local timezone. The indexed date range is approximately
1677-09-21 through 2262-04-11 (signed int64 nanoseconds); dates outside it are
rejected by the server. Existing graph records must be re-upserted to index dates.

```python
graph.find("node", PropertyFilter(
    "starts_at", "range",
    lower=datetime(2026, 12, 1, tzinfo=timezone.utc),
    upper=datetime(2026, 12, 31, tzinfo=timezone.utc),
))
graph.find("node", PropertyFilter("tags", "eq", value="python"))
```

`find` returns all matching records sorted by external ID, raising if their
total exceeds `max_records`. For larger enumerations, use `find_page`, which
does not apply that total-record cap:

```python
token = ""
while True:
    page = graph.find_page(
        "node", PropertyFilter("tags", "eq", value="python"),
        page_size=100, page_token=token,
    )
    for record in page.records:
        print(record.id)
    token = page.next_page_token
    if not token:
        break
```

`FindPage` exposes `records` and `next_page_token`. Page sizes are 1..1000;
record byte limits can produce shorter pages. Keep the kind and filters
unchanged when reusing a token. Pages follow resource-UUID order, not external
ID order, and are not a snapshot of concurrent writes.

Date filters work in `find`, `topology`, chunk `search`, `expand`, and
`shortest_path`, including their edge and neighbor/node filters. Date-range
filter bounds may be open on either side and must use datetimes, not mixed
numeric/date values. These predicates query stored datetime values; they do
not add support for stored interval properties.

### Expansion and shortest paths

On an open `Graph`, `expand` wraps one server-side `Expand` call and resolves
its record-table indexes into Python objects:

```python
expansion = graph.expand(
    ["P1"],
    direction="outgoing",
    edge_types=["STOCKED_AT"],
    edge_filters=[PropertyFilter("quantity", "range", lower=5)],
    neighbor_label="Warehouse",
    neighbor_filters=[PropertyFilter("country", "eq", value="Spain")],
    candidate_limit=1000,
    max_results=1000,
)
for match in expansion.matches:
    print(match.node_id, match.edge.id, match.neighbor.id)
print(expansion.examined_edges)
```

`Expansion.matches` contains `ExpansionMatch(node_id, edge, neighbor)` values.
Both records use the same `Record` type as `get` and `find`, including tenant,
properties, text chunks, and original edge orientation. Parallel edges and
self-loops retain the server's binding semantics; result order is unspecified.

`shortest_path` wraps the server's bidirectional BFS, without implementing
another search in Python:

```python
path = graph.shortest_path(
    "P1",
    "W1",
    direction="outgoing",
    edge_types=["STOCKED_AT"],
    edge_filters=[PropertyFilter("quantity", "range", lower=5)],
    max_hops=6,
    max_visited_nodes=1000,
    max_examined_edges=10000,
)
if path is not None:
    print([node.id for node in path.nodes])
    print([edge.id for edge in path.edges])
    print(path.examined_edges)
```

The result is a `Path` with ordered `nodes`, `edges`, and `examined_edges`,
or `None` when no qualifying path exists within the hop bound. Equal endpoints
can return a zero-edge path. Ties are arbitrary.

Both methods require an explicit `direction`: `"outgoing"`, `"incoming"`,
or `"both"`. Types are ORed; `PropertyFilter` predicates are ANDed.
`shortest_path` additionally accepts `node_label`, `node_filters`, and
`node_filter_scope="all"` (the default) or `"intermediate"` to exempt
source and target. Filters apply during search, not after selecting a path.

The methods expose the RPC limits directly, independently of `Graph.max_records`.
Defaults and maximums are described in [Expand](#batched-one-hop-expansion) and
[shortest paths](#shortest-paths); zero-valued limits select the server defaults.
Missing nodes, work-limit failures, and other RPC errors propagate as
`grpc.RpcError`, rather than becoming empty results or `None`. Reads still
have no shared snapshot. There is no general multi-hop traversal method.

### Text and vector search

Text properties are stored as ordered chunks. Supply one embedding per chunk
when vector search is desired:

```python
product = graph.upsert_node(
    "P2",
    ["Product"],
    {"category": "fruit"},
    text_chunks={
        "description": ["red apple from Spain", "quiet green orchard"],
        "features": ["crisp sweet fruit"],
    },
    embeddings={
        "description": [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]],
        "features": [[0.0, 0.0, 1.0]],
    },
)

edge = graph.upsert_edge(
    "E2",
    "P2",
    "STOCKED_AT",
    "W1",
    {"quantity": 1},
    text_chunks={
        "note": ["inspected at warehouse"],
        "inspection": ["sealed package"],
    },
    embeddings={
        "note": [[0.0, 0.0, 1.0]],
        "inspection": [[1.0, 0.0, 0.0]],
    },
)

feature_hits = graph.search(
    text="crisp sweet",
    vector=[0.0, 0.0, 1.0],
    kind="node",
    text_property="features",
    filters=[PropertyFilter("category", "eq", value="fruit")],
    top_k=10,
)

all_property_hits = graph.search(vector=[0.0, 0.0, 1.0])
inspection_hits = graph.search(
    vector=[1.0, 0.0, 0.0], kind="edge", text_property="inspection"
)
```

`Graph.search` accepts text, a vector, or both. Text uses nidx paragraph search;
vectors use the configured cosine vectorset. When both are supplied, results
from both ranked lists are combined per chunk using reciprocal-rank fusion
(RRF), `sum(1 / (rrf_k + rank))`, with `rrf_k=60` by default. Results identify
the owning record, text property, chunk index and text, plus each modality's
rank. Text and vector search can also be run separately. A record can have
multiple embedded text properties, as can an edge; the `embeddings` mapping
uses the same property keys as `text_chunks`. Set `text_property` to limit
search to one property, or omit it to search across all indexed text properties.
Every hit's `property` field identifies which property matched, including when
searching across both node and edge properties.

Set `min_score_semantic` (cosine similarity) and `min_score_keyword` (BM25)
to filter each modality's candidates before RRF:

```python
hits = graph.search(
    text="industrial parts",
    vector=embedding,
    min_score_semantic=0.7,
    min_score_keyword=2.0,
)
```

Both must be finite numbers and default to `0.0`, preserving previous behavior.
They are not thresholds on the final RRF score. A chunk passing either modality's
threshold can still appear in a hybrid result. A threshold has no effect when its
modality is not requested. Regenerate Python bindings with `make protos`,
then deploy the updated searcher before the API.

### Adding chunks and embeddings later

Chunks and embeddings can be attached in separate steps after creating a node
or edge. This fits an ingestion flow where text extraction or embedding
generation runs asynchronously:

```python
graph.upsert_node("P3", ["Product"], {"category": "fruit"})

graph.update_text_properties(
    "node",
    "P3",
    text_chunks={"description": ["fresh pear from Italy"]},
)

graph.update_text_properties(
    "node",
    "P3",
    embeddings={"description": [[0.0, 0.0, 1.0]]},
)
```

The method also accepts both arguments together and works for edges. It patches
only the named text properties; the rest of the record and other properties'
vectors remain unchanged. Embeddings-only updates must target existing text
chunks and provide one vector per chunk. Replacing a property's chunks without
new embeddings clears that property's old vectors, avoiding stale matches.

`text_chunks` maps a property to a nonempty ordered list of nonempty strings.
If `embeddings` includes that property, its vector list must have exactly one
finite, nonzero vector per chunk, all with the configured dimension. The
default vectorset is `lpg-property-v1`, with dimension 3 for this synthetic
demo; configure the vectorset and its dimension on `Graph.create`, and use the
same values when reopening `Graph`. The caller supplies embeddings; this layer
does not select or validate a model. Use one vectorset only for compatible
embedding models and dimensions.

`kind`, typed JSON `filters`, and `text_property` constrain candidates. Property
filters apply to the owning node/edge record; they do not filter on endpoint
properties. Search returns chunk-level results, and `top_k` limits the final
ranked list. `top_k` accepts 1..10000, independently of `max_records`.
Single-modality search retrieves `top_k` candidates. Hybrid search fuses up
to `min(10000, max(100, 10 * top_k))` candidates per modality, then selects the
best `top_k`. Fusion is over those windows, not every matching chunk globally;
filling a window is normal, not a completeness error. Equal fused scores are
ordered by internal chunk identity. Only final results are hydrated, and the
encoded response is limited to 3 MiB.

The old `SearchChunksRequest.candidate_limit` field is retired (its protobuf
number is reserved); old clients' values are ignored by the new server.
`max_records` continues to bound convenience enumeration, not ranked search.
Ordinary text queries use OR-combined keyword terms, even when passed a full
source chunk. Quoted queries request tokenized phrase matching, not byte-exact
chunk equality; identical text can occur on many nodes.
Replacements remove prior text, paragraph, JSON, relation, and configured
vectorset projections before writing the new record.

- `Graph.create` creates one shard with the tenant UUID as its `kbid`.
  `Graph(nidx, tenant, shard_id)` reopens it and verifies shard ownership.
  Keep the tenant/shard mapping outside the facade; this spike does not
  implement a tenant registry or authentication service.
- The Rust service maps node/edge keys to graph-scoped resource IDs.
  Edges require existing endpoints in the same graph.
- Properties are a schemaless map of strings, booleans, signed int64, finite
  floats, and null. Nested objects/arrays and temporal values are not supported
  yet. Null is preserved in the record but omitted from the property index;
  null equality is rejected rather than approximated.
- Equality filters and inclusive numeric ranges are pushed into nidx. Multiple
  filters are ANDed. Range bounds must have the same numeric type; no Cypher
  numeric coercion or missing/null semantics are claimed.
- Upserts replace the complete record, including its properties and text.
  Labels and edge type are immutable; edge endpoints may change. Repeating an
  edge ID replaces that edge; distinct edge IDs preserve parallel edges.
- `topology` enumerates edge records through `FindRecords`, preserving their
  identity. Its property filters apply to edges, **not endpoint properties**.
  It does not invoke `GraphSearch`; `find` also uses `FindRecords` pagination.
- RPCs use a 60-second timeout. The facade waits up to 15 seconds between
  checks for shard/read-back visibility, with each in-flight RPC still subject
  to its own timeout. The server's mutation visibility rules apply; vectors
  are not included in its visibility fence. A timeout is an uncertain outcome,
  not a rollback.
- Candidate enumeration is deliberately bounded (1,000 records by default).
  Overflow raises instead of returning a truncated graph.

## Mapping to nidx

Each node/edge is one resource:

| Projection | Representation |
| --- | --- |
| Stored record | Base64-encoded full `LpgRecord` protobuf in text field `a/lpg_record_v1`, without text tokenization |
| Property predicates | JSON projection in `a/lpg_record_v1`, with UTF-8/hex encoded property keys |
| Record candidates | One metadata-only envelope paragraph, allowing JSON prefiltering to return resource IDs |
| Text properties | Encoded text fields and one paragraph per supplied property chunk |
| Property embeddings | One paragraph vector per chunk in the configured vectorset |
| Search results | Text/vector ranked chunk IDs hydrated to owning records and chunk text |
| Edge topology | One triple in relation field `a/metadata`, with external node IDs and type/owner facets |
| Edge identity | Distinct edge records and `/lpg/edge/<resource-id>` owner facets preserve parallel edges |
| Visibility probe | Server-generated JSON fence, stored protobuf read-back, and relation projection checks; no vector visibility guarantee |

`TextInformation.skip_tokenization` is a per-field opt-out implemented by the
text and paragraph indexes. It defaults to false, preserving full-text indexing
for RAG and existing clients. When true, stored field text remains retrievable,
and field/paragraph IDs, offsets, labels and vectors remain indexed, but text
tokens are omitted. LPG sets it only on the envelope, not on human-readable
text properties. This needs no index schema change or shard migration; existing
segments retain their tokens until records are reindexed.
The complete envelope retains chunks and embeddings for record retrieval,
search hydration and read-modify-write patches.

Using `a/metadata` is intentional: the current relation prefilter adapter maps
resource-level JSON matches to this field, not to all relation fields.
The owner facet checks topology directly during deletion, so disappearance of a
text prefilter cannot mask a stale relation.

The Rust API prepares each `Resource` and sends it directly to the trusted
`NidxIndexer.IndexResource` RPC for ordinary mutations, or the additive
`NidxIndexer.IndexResources` RPC for bulk loading. The existing single-resource
indexing path is unchanged. Python does not construct index projections, write
ingestion files, or manage indexer object storage. Replacement invalidates the
resource's older projections while preserving neighbors in shared segments.

nidx requires a vector index at shard creation. This spike configures a
cosine-similarity vectorset and indexes supplied paragraph embeddings; it does
not generate embeddings or manage model compatibility.

## Verified by integration tests

The Python facade suite runs against the real Rust LPG service:

- Typed property round trips, exact string predicates, inclusive ranges,
  conjunctions, arbitrary property keys, and isolated nodes.
- Parallel-edge identity and edge-property filtering through `FindRecords`.
- Full replacement: old properties, endpoints, and text/vector content disappear;
  node labels and relationship types cannot be changed in place.
- Identical external node/edge IDs in different tenants stay separate.
- Edge deletion and rejection of node deletion until incoming, outgoing,
  parallel, and self-loop edges are removed.
- Reopening the facade and reconnecting a new gRPC client to the same graph.
- Explicit candidate-limit failures and a successful standalone demo subprocess.
- Text-only, vector-only, and RRF hybrid chunk search with multiple embedded
  properties on nodes and edges; property-scoped and all-property searches
  report the matched property. Also covers typed-property and tenant filters,
  and replacement removing stale chunks and vectors.
- Adding chunks and embeddings independently to existing nodes and edges;
  partial updates preserve unrelated properties and vectors.

The Cypher/graph-RPC suite additionally covers all seven supported query shapes,
filtered one-hop expansion, and shortest paths compared with a reference BFS.
Server-side deletion and explicit client-retry checks belong to the Rust test suite.

## Python facade and production gaps

`Graph` delegates mutations and node deletion to Rust. Before node
deletion it enumerates incident edges with `FindRecords`, enforcing its
candidate cap and giving a local error if edges remain. The server checks
again before performing the deletion.

Use one API writer instance per graph and coordinate external writers.
There is no cross-replica writer lock, optimistic concurrency control,
transaction isolation, or consistent snapshot across query calls.

The facade exposes scalar, datetime and string-list properties, record operations,
chunk search, `Expand`, and `ShortestPath`. Stored ranges remain unsupported.
Raw RPCs additionally expose stored ranges and label/endpoint record filters.
The separate Cypher interpreter remains a restricted feasibility example.

Production work still includes efficient joins/aggregation, reliable
concurrent mutation/recovery, snapshots, authentication, routing, quotas,
observability, and a tenant registry. Multi-shard graphs, graph transactions,
and performance claims remain outside this spike. See the
[proposal](../../../lpg-proposal.md).
