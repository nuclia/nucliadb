# LPG storage and hybrid-search spike

A Python experiment using **real embedded nidx**, PostgreSQL metadata, and
file-backed object storage. There is no Python record database or persistent
in-memory graph. The storage and property-level hybrid-search milestones use
existing nidx APIs. It remains separate from the new Rust service described
below; the two record formats are not interoperable.

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
  identify existing, non-deleting nodes in the same graph. This matches
  Neo4j's multi-label nodes and single-type relationships, except that this
  prototype requires every node to have a label (Neo4j permits unlabeled
  nodes). Node labels and edge type are immutable after creation; changing
  either requires deleting and recreating that record. Properties and edge
  endpoints remain mutable. `revision` is an opaque client value stored on
  upsert, not an optimistic concurrency token;
  clients cannot set `deleting`. Text properties contain ordered chunks and
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
  Datetimes and date ranges are opaque strings in this slice.
* `FindRecords` finds nodes or edges by record-local scalar equality and
  numeric range filters, labels, and edge source, target, type or incident
  node. Parallel edges remain separate; a self-loop matches incident-node
  filtering once. It returns pages using an opaque search cursor;
  concurrent writes or searcher refreshes can change results between pages.
  Nulls, dates and stored ranges are not filterable. Records written before
  this projection was added need a full upsert before they appear in finds.
* `Expand` performs a batched, filtered one-hop expansion. See below for its
  directions, result bindings and explicit work/result limits.
* `SearchChunks` supports text, vector and reciprocal-rank-fused hybrid search,
  optional kind/property and record-local property filters, and `top_k`.
  Candidate limits fail explicitly rather than silently returning a partial
  result; hydration can fail if a candidate changes during the query.

Within `LPGOperation`, edge deletion is idempotent and deletes only that edge;
node deletion uses non-detach semantics and rejects nodes with incident edges.
The node-delete operation accepts `detach`: it marks the node as deleting,
removes all incident edges (including parallel and self-loop edges), then
deletes the node. Retrying a marked deletion resumes it even if `detach` is
false. Reads can see the marker and a partially deleted graph. The text-patch
operation adds, replaces or clears chunks and embeddings. Replacing chunks
without embeddings clears their old vectors; embedding-only patches require
existing chunks. Unchanged text properties retain vectors. This is a
read-modify-write, not a conditional update.

Mutations are serialized per graph **within one API instance**. Each mutation
persists a pending request, indexes it, and waits up to 30 seconds for its
envelope and JSON/paragraph projection to become visible (or absent for a
deletion). The next mutation replays any pending request before proceeding,
including after a process restart. A timeout or downstream error is an
uncertain outcome, not a rollback; retry the operation. Pending work may block
further mutations until the indexer and searcher recover. A permanently
rejected pending request also blocks the graph; there is no repair API yet.
This visibility check does not verify vector projections, and independent
reads and multi-page queries are not snapshots.

These safeguards do **not** coordinate API replicas or direct external writes.
Use one API writer instance per graph and do not mix other writers in the
shard; multiple instances can race endpoint checks, deletion and pending
recovery. Existing Indexer request expiry and late completion can also break
ordering after prolonged failures. There are no conditional writes,
multi-record transactions, or cross-replica isolation. Errors from downstream
services are returned to clients. Records remain encoded in a stored Resource
text field, not canonical storage or the Python prototype's JSON envelope.
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

Missing starting nodes produce `NOT_FOUND`, deleting starting nodes
`FAILED_PRECONDITION`, and missing/deleting neighbors `UNAVAILABLE`. Like other
LPG reads, expansion is not a snapshot. It uses one indexed batch edge search
over the existing JSON endpoint/type projections and batch-fetches neighboring
records, sharing hydration for repeated IDs. It does not load the full graph
or require a new adjacency store.

The Cypher interpreter still uses its existing lookup-based execution;
server-side `Expand` is available directly through the generated gRPC clients.
Self-contained Python integration tests cover the RPC alongside the Cypher
queries:

```sh
.venv/bin/python -m pytest nidx/examples/lpg/tests/test_cypher_integration.py -q
```

After changing the protocol locally, `make protos` regenerates the Python
bindings. Cargo generates the Rust bindings automatically.

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

Lookups use `FindRecords` property/label and endpoint filters, `GetRecords`
hydrates endpoints, and pagination is followed. Joins, bounded path expansion,
shortest paths, grouping and ranking run in Python. Numeric equality and edge
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
uniqueness and result/work limits, server-side joins and shortest paths,
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

The test creates a shard with a fresh tenant ID, bulk-loads a small graph,
executes the queries, and deletes that shard afterward. Fixtures shut down the
services and remove the container, including on failure. Docker, Cargo and the
nidx Rust build prerequisites are required; no external service configuration
or `LPG_TEST_API_ADDRESS` is needed.

`storage.py` includes an experimental `GraphV2` Python facade for these RPCs.
The embedded harness does not configure the NidxLPG indexer/searcher clients,
so `GraphV2` is not exercised against a live NidxLPG server here. Its separate
test suite injects a mock NidxLPG client that delegates persistence and search
to the existing Python prototype; it checks facade behavior and RPC mapping,
not Rust service interoperability.

Rust validation from `nidx/`:

```sh
SQLX_OFFLINE=true cargo test -p nidx --no-default-features --lib api::lpg::tests
SQLX_OFFLINE=true cargo check -p nidx --no-default-features --test test_integration
# Set DATABASE_URL to a dedicated disposable PostgreSQL instance first.
SQLX_OFFLINE=true cargo test -p nidx --no-default-features --test test_integration integration::lpg:: -- --test-threads=1
```

The lifecycle integration suite previously passed against a disposable loopback
PostgreSQL 16 container; the newer deletion and recovery tests require a
separate run. Never point them at a production or shared database.

## Run

From the repository root, use the workspace Python environment with
`nidx-binding`, `nidx-protos`, `nucliadb-protos`, and `grpcio` installed.
The repository's `uv sync --group nidx` sets up the workspace, including the
Rust binding. Docker and the workspace test dependencies are needed for tests.

The integration tests start a uniquely named, loopback-only PostgreSQL container,
verify a host SQL connection, and remove that container afterwards:

```sh
.venv/bin/python -m pytest nidx/examples/lpg/tests/test_storage.py -q
.venv/bin/python -m pytest nidx/examples/lpg/tests/test_storage_v2.py -q
.venv/bin/ruff check nidx/examples/lpg
.venv/bin/mypy --explicit-package-bases --follow-untyped-imports nidx/examples/lpg
```

For the standalone demo, start a **dedicated disposable** PostgreSQL instance in
another terminal. Do not point the prototype at a production/shared database:
the binding runs nidx migrations.

```sh
docker run --rm --name lpg-prototype-postgres \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -p 127.0.0.1:55432:5432 postgres:16-alpine
```

After PostgreSQL is accepting host connections:

```sh
export LPG_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/postgres
.venv/bin/python -m nidx.examples.lpg.demo --directory /tmp/nidx-lpg-demo
```

The demo creates a new tenant, adds products/warehouses/clients and parallel
edges, prints property-filtered records, replaces an edge, reconnects the Python
layer, and verifies a cascade. It prints the tenant UUID and shard ID.
The directory and database together hold persistent state; keep both to reopen
it. Stop the demo PostgreSQL with `docker stop lpg-prototype-postgres` when done.
The example's trust authentication is for this loopback-only disposable setup.

## Synthetic JSONL dataset

Generate data offline using Faker (already in the workspace dev dependencies).
This does not import the binding, contact a database, create a graph, or ingest
anything. The later converter/ingester can target `GraphV2`.

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

Each line is one node or edge JSON object. `properties` contains scalar values
and string lists. `text_chunks` maps property names to ordered text chunks.
Nodes carry `labels`; edges carry `source`, `edge_type`, and `target`.
These field names follow `GraphV2.upsert_node` / `upsert_edge` arguments;
the current facade supports only scalar properties, not the generated string
lists. The loader below maps directly to the Rust RPC types instead.
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
`nucliadb-protos` and `grpcio` from the workspace environment. It does not use
the embedded binding or run migrations. The Rust LPG service must already be
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

The loader validates the whole file in memory before any remote writes,
rejecting malformed rows, duplicate record keys, unsupported property types,
missing endpoints and inconsistent embedding dimensions. Endpoints must
appear as nodes in the same file. Blank lines and unknown fields are errors.
The loader constructs self-contained bulk requests, including complete
endpoint node records alongside edges. Repeated nodes are full upserts, not
ID-only references. String-list
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

`--batch-size` defaults to 10 (allowed 1–1000), counting all records in a request,
including repeated endpoint nodes. Endpoints are deduplicated within each
request. A non-self-loop edge requires room for at least three records (two
nodes and the edge); too-small batches fail before remote writes.
Requests are additionally split at 3 MiB to stay below the normal 4 MiB gRPC
limit; a record plus its required endpoints must fit together or preflight
fails. The server's projected-resource size limit still applies.
`--timeout` sets seconds per RPC, defaulting to `30 * batch-size + 60`, because
indexing acknowledgements remain sequential, followed by one shared 30-second
visibility budget for the entire batch.
This is a correctness-oriented spike loader, not a bulk-ingestion benchmark.

After creating a new shard, the loader pauses for 5 seconds before writing
records. Use `--shard-wait <seconds>` to change this delay, or
`--shard-wait 0` to disable it. Existing graphs do not pause. The tenant and
graph IDs print before the pause. This fixed delay allows time for searcher
refresh but is not a readiness guarantee; normal RPC errors still apply.

Each batch checks every operation status and reads all written records back
through `GetRecords` to verify their complete protobuf contents. Progress and
the graph ID go to stderr; when creating a shard, the tenant UUID is also
printed before record writes. A final JSON object with graph ID, node count,
edge count and (for new shards) tenant UUID goes to stdout only after successful
verification. Keep the tenant and graph IDs for later queries and experiments.
Each verified batch reports `verified`, `total` (nodes plus edges), overall
`percent`, `elapsed_seconds`, and average `records_per_second` since ingestion
started. `verified` and the rate count unique dataset records, so repeated
endpoints do not inflate progress beyond 100%. `submitted_records` and
`submitted_records_per_second` also report actual verified writes including
repeated endpoints. The rates include write RPCs and read-back verification, but exclude
file validation, shard creation and the startup pause. Updates occur after
each verified batch, not while an RPC is in flight.

On validation, transport, write-result or verification failure, the command
exits nonzero without automatic retries. Batches are not transactions:
previously applied records remain, and an RPC timeout leaves the current
batch's outcome uncertain. No shard is deleted on failure. Read-back validates
the stored record, not vector-index visibility or semantic ranking.

### Stateless bulk loading

The loader exclusively uses `BulkLoad(graph_id, records)`. There is no
`--bulk-load` switch, session ID, Begin/End RPC or normal-ingestion mode in
the loader. The server's normal `LPGOperation` API remains available to other
clients with its existing validation and pending-recovery behavior.

Each bulk request is an independent, self-contained graph. Every edge must
reference nodes included in that same payload, even when they were written
by an earlier batch. All structure, property, text/vector and endpoint checks
complete before any new indexing. A request accepts 1–1000 records, rejects
duplicate `(kind, id)` keys and the server-owned `deleting` flag, and writes
nodes before edges regardless of input order. Results follow input order.
The server does not check stored endpoint existence or deletion markers.

Repeated nodes are reindexed and fully replaced, including their properties,
text and embeddings. Use only a dedicated graph without concurrent/external
writers: this trusted path can overwrite newer node data or deletion markers.
There are no persisted bulk sessions, declarations, or pending-recovery
records in PostgreSQL. PostgreSQL is still used for ordinary nidx index
metadata and vector configuration.

The server submits every resource sequentially, then waits for all records
and their JSON fences to become visible within one shared 30-second budget.
Indexing/visibility failures return an RPC error with a partial/uncertain
outcome. Writes already submitted remain; there is no rollback or automatic
recovery. A later bulk call does not replay previous work. A graph with a
pending normal mutation is rejected to prevent bypassing its recovery order.
The loader verifies each batch by reading it back, and stops on any failure.
Manually rerunning with `--graph-id` re-upserts data; it is not session resume.

Rebuild/restart nidx and regenerate Python bindings for the updated contract:

```sh
uv sync --reinstall-package nidx_protos --inexact
```

The migration removes obsolete session/declaration tables, not graph resources.
It refuses to run while legacy pending bulk batches exist; finish/recover them
using the previous server before upgrading. Normal pending mutations remain
supported. This protocol does not add cross-replica writer isolation.

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
from pathlib import Path
from uuid import uuid4

from nidx.examples.lpg.storage import EmbeddedNidx, Graph, PropertyFilter

nidx = EmbeddedNidx(database_url, Path("/tmp/nidx-lpg-demo"))
try:
    graph = Graph.create(nidx, str(uuid4()))
    graph.upsert_node("P1", ["Product"], {"price": 150, "available": True})
    graph.upsert_node("W1", ["Warehouse"], {"country": "Spain"})
    graph.upsert_edge("E1", "P1", "STOCKED_AT", "W1", {"quantity": 10})

    product = graph.get("node", "P1")
    products = graph.find("node", PropertyFilter("price", "range", lower=100))
    edges = graph.topology(PropertyFilter("quantity", "range", lower=5))

    reopened = Graph(nidx, graph.tenant, graph.shard_id)
    reopened.delete_node("P1", detach=True)
finally:
    nidx.close()
```

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
ranked list. Each modality's candidate retrieval is capped by `max_records`
(1,000 by default) and raises if nidx reports more candidates rather than
silently truncating.
Replacements remove prior text, paragraph, JSON, relation, and configured
vectorset projections before writing the new record.

- `Graph.create` creates one shard with the tenant UUID as its `kbid`.
  `Graph(nidx, tenant, shard_id)` reopens it and verifies shard ownership.
  Keep the tenant/shard mapping outside the `Graph` object; this spike does not
  implement a tenant registry or authentication service.
- Node and edge external IDs occupy distinct, tenant-scoped UUID namespaces.
  Edges require existing, non-deleting endpoints in the same tenant.
- Properties are a schemaless map of strings, booleans, signed int64, finite
  floats, and null. Nested objects/arrays and temporal values are not supported
  yet. Null is preserved in the record but omitted from the property index;
  null equality is rejected rather than approximated.
- Equality filters and inclusive numeric ranges are pushed into nidx. Multiple
  filters are ANDed. Range bounds must have the same numeric type; no Cypher
  numeric coercion or missing/null semantics are claimed.
- Upserts replace the entire property map and node label list, or the complete
  edge including endpoints/type. Repeating an edge ID replaces that edge;
  distinct edge IDs preserve parallel edges. Labels are stored, not yet exposed
  as label predicates.
- `topology` retrieves actual relation paths and hydrates their owning edge
  resources. Its property filters apply to edges, **not endpoint properties**.
  `find` retrieves records through JSON-filtered paragraph search.
- Calls wait for bounded, observed search visibility (a 15-second polling
  budget, plus in-flight checks with a 5-second deadline per RPC). They do not equate indexer acknowledgement with
  visibility. A timeout is an uncertain outcome: the mutation may be committed.
  Reopen/read/retry rather than assuming rollback.
- Candidate enumeration is deliberately bounded (1,000 records by default).
  Overflow raises instead of returning a truncated graph or incomplete cascade.

## Mapping to nidx

Each node/edge is one resource:

| Projection | Representation |
| --- | --- |
| Canonical record | Versioned JSON envelope in text field `a/lpg`, retrieved using `ExtractedTexts` |
| Property predicates | JSON field `a/lpg`, with UTF-8/hex encoded property keys to avoid JSON path ambiguity |
| Record candidates | One envelope paragraph, allowing JSON prefiltering to return resource IDs |
| Text properties | Encoded text fields and one paragraph per supplied property chunk |
| Property embeddings | One paragraph vector per chunk in the configured vectorset |
| Search results | Text/vector ranked chunk IDs hydrated to owning records and chunk text |
| Edge topology | One triple in relation field `a/metadata`; endpoints contain stable node resource UUIDs |
| Edge identity | Path `resource_field_id` maps back to the edge resource, including parallel edges |
| Visibility probe | Revision in the JSON index plus envelope retrieval, expected vector IDs, and per-edge owner facet `/lpg/edge/<uuid>` |

Using `a/metadata` is intentional: the current relation prefilter adapter maps
resource-level JSON matches to this field, not to all relation fields.
The owner facet checks topology directly during deletion, so disappearance of a
text prefilter cannot mask a stale relation.

Creation/replacement writes a serialized `Resource` to a unique file in the
configured indexer store and passes its key in an `IndexMessage`. Embedded
indexing is synchronous, so that payload file is removed after the call.
Replacement supplies deletion keys for every projection; edge relation deletion
keys use the fixed relation field, while text/paragraph/JSON/vector deletion keys
use the resource UUID. Deletion submits a `DELETION` message.

nidx requires a vector index at shard creation. This spike configures a
cosine-similarity vectorset and indexes supplied paragraph embeddings; it does
not generate embeddings or manage model compatibility.

## Verified by integration tests

- Typed property round trips, exact string predicates, inclusive ranges,
  conjunctions, arbitrary property keys, and isolated nodes.
- Parallel-edge identity through native relation paths, including edge-property
  JSON prefiltering.
- Full replacement: old properties and old endpoint/type projections disappear.
- Identical external node/edge IDs in different tenants stay separate.
- Edge deletion, rejection of non-detach node deletion, and cascades covering
  incoming, outgoing, parallel, and self-loop edges.
- Resuming an interrupted cascade from a persisted deletion marker.
- Reopening both the Python graph layer and the entire embedded nidx binding
  against the same PostgreSQL database and storage directory.
- Explicit candidate-limit failures and a successful standalone demo subprocess.
- Text-only, vector-only, and RRF hybrid chunk search with multiple embedded
  properties on nodes and edges; property-scoped and all-property searches
  report the matched property. Also covers typed-property and tenant filters,
  and replacement removing stale chunks and vectors.
- Adding chunks and embeddings independently to existing nodes and edges;
  partial updates preserve unrelated properties and vectors.

## Python workarounds and production gaps

**Serialize all writes for a tenant externally, including across Graph objects.**
There is no lock service, optimistic concurrency control, transaction isolation,
or safe concurrent graph writer in this spike.

Cascade discovery scans the bounded set of edge records in Python. Before
deleting edges, it persists `deleting=True` on the node. Endpoint validation then
rejects new edges to that node, and a fresh Python instance can resume deletion
by repeating `delete_node(..., detach=True)`. Recovery is explicit, not automatic;
deletion is not atomic. Reads can see the marker and a partially deleted graph.

For production, evaluate native record get/batch-get, complete paginated
adjacency enumeration, efficient joins, consistent index visibility tokens,
and reliable concurrent mutation/recovery semantics. The service additionally
needs authentication, routing, quotas, observability, and a tenant registry.
The binding's startup can panic on invalid configuration/unavailable PostgreSQL;
it is a local harness, not a production service lifecycle.

Cypher parsing/execution, endpoint-property joins, multi-shard tenants,
performance claims, and graph transactions remain outside this spike. See the
[proposal](../../../lpg-proposal.md).
