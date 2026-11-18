# LPG service on nidx: proposed experiment

Status: milestone 1 implemented as an isolated
[Python storage spike](nidx/examples/lpg/README.md). Later milestones remain proposals.

## Goal and agreed boundaries

Test whether nidx can be the persistent foundation for a cloud-native,
multi-tenant LPG service, not just its secondary search index.

- Use synthetic products, warehouses, and clients, with `STOCKED_AT`, `BUYS`,
  and `SUPPLIES` relationships. Include isolated nodes, parallel edges, and
  properties on both nodes and edges.
- Start with simple queries and iterate toward multi-hop queries and Cypher
  mutations. Do not claim full Cypher compatibility.
- Keep persistent graph records in nidx; no separate canonical database.
- Accept eventual search visibility. Demonstrate cascading node/edge deletion.
- Expect regular additions/deletions and relatively infrequent property updates.
- Graph sizes, query latency targets, and traversal depths remain unknown:
  the initial experiment proves functionality, not production scalability.

## What can be reused, and what is missing

| Area | Reusable today | Additional LPG work |
| --- | --- | --- |
| Cloud infrastructure | Object-store segments, mmap search, compaction, metadata, separately deployable components | Measure graph workloads, small-record ingestion overhead, cold starts, and tenant cost |
| Tenancy | `kbid` metadata and shard-scoped operations | Authentication, authorization, tenant-to-shard routing, quotas; `kbid` alone is not an authorization boundary |
| Properties | Schemaless JSON indexing with typed equality/range and Boolean filters; Python spike hydrates and filters records | Native direct record retrieval, nested values, missing/null semantics, mixed numeric types |
| Text | Stored field text, `ExtractedTexts`, and paragraph full-text search; Python spike maps text properties to fields/chunks and hydrates results | Native property/chunk API and measured production query behavior |
| Embeddings | Paragraph vectors and vectorsets; the Python spike assigns stable chunk IDs, stores supplied embeddings, and supports text-only/vector-only/RRF hybrid search | Model generation/compatibility, production lifecycle, measured ranking/latency, and multi-shard search |
| Topology | Directed/undirected single-hop triple search | Independent node/edge identity, endpoint-property joins, multi-hop execution, complete adjacency enumeration |
| Online mutations | Resource ingestion, deletion keys, background index synchronization | Graph-level validation, cascading deletion, retry ordering, visibility tracking, concurrency semantics |
| Cypher | No Cypher execution API in the inspected nidx interfaces | Parser, validation, logical plan, execution, result semantics, and explicit unsupported-feature errors |

Evidence: [APIs](nidx/nidx_protos/nidx.proto),
[resource model](nidx/nidx_protos/noderesources.proto),
[query model](nidx/nidx_protos/nodereader.proto),
[JSON index](nidx/nidx_json/src/resource_indexer.rs),
[prefiltering](nidx/src/searcher/query_planner/prefilter.rs), and
[relation search](nidx/nidx_relation/src/reader.rs).

## Proposed storage mapping

Use **one resource per node and one resource per edge**, so parallel edges,
isolated nodes, and independently mutable properties have explicit identities.

- Assign deterministic, tenant-scoped UUIDs from `(kind, external_id)`. Use the
  canonical 32-hex resource representation expected by existing deletion paths.
  Endpoint references use stable node IDs, never mutable display names.
- Store a complete versioned JSON envelope in a dedicated text field: kind,
  external ID, labels/type, properties, and edge endpoints. Retrieve it through
  `ExtractedTexts`. This is a prototype record-store adaptation, not a new
  key-value API. Avoid duplicating large vectors in this envelope by default.
- Also populate `Resource.json_fields` for typed property filtering. This index
  is schemaless, but currently returns resource IDs internally, not raw records.
- Give each searchable text property an encoded field ID. Add paragraphs only
  where chunk-level text/vector search is needed, not for every scalar property.
- Attach supplied property/chunk embeddings through paragraph vectors. Preserve
  existing resource/field/offset ID grammar; namespace within those components,
  rather than inventing arbitrary paragraph IDs. Use vectorsets by compatible
  model/dimension, not one vectorset per property.
- Store one topology triple on each edge resource. Recover edge identity from
  its owning resource/field, since the current relation value is not a unique
  edge ID. Validate this round trip, including parallel edges, before building
  query execution on it. The milestone 1 spike verifies this with triples under
  `a/metadata` and a per-edge owner facet for direct visibility checks.
- Begin with one shard per tenant. Multi-shard tenants are a later experiment;
  `kbid` does not automatically implement distributed graph joins.

Schemaless means no user-declared fixed property schema. It does not remove the
need for a supported value model or configured embedding dimensions.

## Python service boundary and query execution

Expose tenant-bound `upsert_node`, `upsert_edge`, `delete_edge`,
`delete_node(detach=True)`, `get_node`, `get_edge`, and
`query(cypher, parameters)` operations. Upserts initially replace the complete
property map; partial updates can be added explicitly later.

Ingestion serializes a `Resource` to the indexer's configured object storage,
then submits an `IndexMessage` referencing it. Deletion submits a deletion
message. Replacements must supply the correct deletion keys for old text,
paragraph, JSON, relation, and vector entries; ingestion is not implicitly a
graph upsert.

The Python query layer parses an explicitly documented Cypher subset, pushes
supported predicates/searches into nidx, hydrates records from stored text, and
performs remaining endpoint checks and joins in Python. Never silently truncate
an exact `MATCH` to a search `top_k`: exhaust candidates or fail with an explicit
prototype resource-limit error.

Important distinction: `GraphSearchRequest` has no JSON filter. The regular
`SearchRequest` can combine JSON prefiltering with graph search, but that filters
the resource owning the triple, not properties of independently stored endpoint
nodes. Endpoint-property predicates therefore require separate lookups/joins.
Validate the relation field mapping against resource-level prefilter behavior.
The storage spike verifies edge-property JSON prefiltering with triples under
`a/metadata`; it does not yet implement endpoint-property joins.

Prototype-only Python work includes candidate enumeration, multi-hop joins,
record hydration, endpoint validation, and incident-edge discovery/cascades.
Any in-memory lookup must be rebuildable from nidx, not the source of truth.
Production needs native efficient equivalents for record lookup, complete
adjacency scans, and graph joins; Cypher parsing/planning can remain in the
service if that boundary proves useful.

## Milestones and acceptance checks

1. **Storage/API spike:** real nidx round trips for nodes/edges, typed properties,
   isolated nodes, parallel edges, replacements, and deletion. Reopen the Python
   layer and recover records without relying on process-local state.
   Implemented and tested, including a full embedded nidx binding restart.
2. **Read queries:** parameterized single-node and one-hop
   `MATCH / WHERE / RETURN`, with node and edge property filters; then a two-hop
   join. Evaluate parser reuse before implementing a grammar. Reject unsupported
   syntax explicitly.
3. **Hybrid search:** implemented in the Python spike for supplied embeddings on
   multiple text properties of nodes and edges. It supports text-only/vector-only
   search and per-chunk reciprocal-rank fusion, typed record filters,
   property-scoped or all-property retrieval (each hit identifies its property),
   tenant isolation, and attaching chunks and embeddings in separate updates.
   Embedding generation/model selection is caller-owned; this is not a Cypher
   procedure or production model lifecycle.
4. **Mutations:** demonstrate Python API upserts/deletes and cascading deletion;
   then incrementally add Cypher `CREATE`, `SET`, `DELETE`/`DETACH DELETE`, and
   finally `MERGE` with clearly bounded uniqueness semantics.

Use a tiny deterministic dataset and assert exact rows, including a query
filtering source, edge, and destination properties. Test identical external IDs
in two tenants, removed properties/vectors after replacement, parallel edges,
and absence of incident edges after a cascade becomes visible. Distinguish
indexer acknowledgement from search visibility; use a bounded, explicit
visibility check, not an arbitrary sleep.

Cascades initially require serialized writes per tenant and an explicit
in-progress state: discover all incident edges, delete them, then delete the
node. This is not an atomic transaction. A production design must address
concurrent edge creation, crash recovery, idempotency, and observable partial
deletion before claiming referential integrity.
The initial implementation uses a persisted node deletion marker and requires
externally serialized writes; it supports explicitly resuming a cascade, not
automatic recovery or concurrent writers.

## Milestones 1 and 3 validation

The [implementation and run instructions](nidx/examples/lpg/README.md) include a
standalone enterprise-data demo. Validation used real embedded nidx and dedicated,
temporary PostgreSQL containers:

- `.venv/bin/python -m pytest nidx/examples/lpg/tests/test_storage.py -vv --tb=long`:
  **21 passed**, including hybrid search, a demo subprocess, and
  persistent-storage restart.
- `.venv/bin/ruff check nidx/examples/lpg`: **passed**.
- `.venv/bin/mypy --explicit-package-bases --follow-untyped-imports nidx/examples/lpg`:
  **passed**.

No native nidx code was changed. The spike intentionally bounds candidate counts
and raises on overflow instead of silently truncating results.

## Decision after the experiment

Proceed if stored-text hydration, index-backed filtering, identity round trips,
and complete candidate enumeration work reliably. Record which operations need
native nidx APIs and benchmark their costs before expanding Cypher coverage.
If nidx-only persistence requires excessive scans or cannot provide adequate
mutation recovery, revisit a dedicated record/adjacency index within nidx or a
separate canonical store rather than hiding the limitation in Python.
