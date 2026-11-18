# LPG storage spike (milestone 1)

A Python experiment using **real embedded nidx**, PostgreSQL metadata, and
file-backed object storage. There is no Python record database or persistent
in-memory graph. No Rust changes are required.

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
| Edge topology | One triple in relation field `a/metadata`; endpoints contain stable node resource UUIDs |
| Edge identity | Path `resource_field_id` maps back to the edge resource, including parallel edges |
| Visibility probe | Revision in the JSON index plus envelope retrieval and per-edge owner facet `/lpg/edge/<uuid>` |

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

nidx currently requires at least one vector index at shard creation. This spike
creates an unused 3-dimensional vectorset; it does not ingest embeddings.

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

Cypher parsing/execution, multi-hop joins, property-level chunks/embeddings,
multi-shard tenants, performance claims, and graph transactions are deliberately
outside milestone 1. See the [proposal](../../../lpg-proposal.md).
