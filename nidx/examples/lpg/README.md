# LPG storage and hybrid-search spike

A Python experiment using **real embedded nidx**, PostgreSQL metadata, and
file-backed object storage. There is no Python record database or persistent
in-memory graph. The storage and property-level hybrid-search milestones use
existing nidx APIs; no Rust changes are required.

## Run

From the repository root, use the workspace Python environment with
`nidx-binding`, `nidx-protos`, `nucliadb-protos`, and `grpcio` installed.
The repository's `uv sync --group nidx` sets up the workspace, including the
Rust binding. Docker and the workspace test dependencies are needed for tests.

The integration tests start a uniquely named, loopback-only PostgreSQL container,
verify a host SQL connection, and remove that container afterwards:

```sh
.venv/bin/python -m pytest nidx/examples/lpg/tests/test_storage.py -q
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
