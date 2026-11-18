from __future__ import annotations

import json
import math
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Literal, TypeAlias
from uuid import UUID, uuid4, uuid5

import grpc
from nidx_binding import NidxBinding
from nidx_protos import nidx_pb2, nidx_pb2_grpc
from nidx_protos import nodereader_pb2 as reader
from nidx_protos import noderesources_pb2 as resources
from nidx_protos import nodewriter_pb2 as writer

from nucliadb_protos import utils_pb2

Scalar: TypeAlias = str | int | float | bool | None
Kind: TypeAlias = Literal["node", "edge"]
ENVELOPE_FIELD = "a/lpg"
RELATION_FIELD = "a/metadata"
RPC_TIMEOUT = 5


def _nonempty(value: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError("IDs, labels, and relationship types must be nonempty strings")


def _scalar(value: Scalar) -> None:
    if value is None or type(value) in (str, bool):
        return
    if type(value) is int and -(2**63) <= value < 2**63:
        return
    if type(value) is float and math.isfinite(value):
        return
    raise ValueError("Properties must be strings, booleans, int64, finite floats, or null")


def _property_key(key: str) -> str:
    if not isinstance(key, str):
        raise ValueError("Property keys must be strings")
    # Keep arbitrary user keys out of Tantivy's dot-separated JSON path syntax.
    return "p_" + key.encode("utf-8").hex()


@dataclass(frozen=True)
class Record:
    tenant: str
    kind: Kind
    id: str
    properties: dict[str, Scalar]
    labels: list[str] = field(default_factory=list)
    source: str | None = None
    target: str | None = None
    edge_type: str | None = None
    deleting: bool = False
    revision: str = field(default_factory=lambda: uuid4().hex)
    version: int = 1

    def __post_init__(self) -> None:
        UUID(self.tenant)
        _nonempty(self.id)
        if self.version != 1 or self.kind not in ("node", "edge"):
            raise ValueError("Unsupported LPG record format")
        if not isinstance(self.properties, dict) or not isinstance(self.labels, list):
            raise ValueError("Invalid LPG properties or labels")
        for key, value in self.properties.items():
            _property_key(key)
            _scalar(value)
        for label in self.labels:
            _nonempty(label)
        if self.kind == "edge":
            if self.source is None or self.target is None or self.edge_type is None:
                raise ValueError("Edges require source, target, and edge_type")
            for value in (self.source, self.target, self.edge_type):
                _nonempty(value)
        elif any(value is not None for value in (self.source, self.target, self.edge_type)):
            raise ValueError("Nodes cannot have edge endpoints or type")
        if type(self.deleting) is not bool or not isinstance(self.revision, str):
            raise ValueError("Invalid deletion state or revision")


@dataclass(frozen=True)
class PropertyFilter:
    key: str
    op: Literal["eq", "range"]
    value: Scalar = None
    lower: int | float | None = None
    upper: int | float | None = None

    def expression(self) -> reader.JsonFilterExpression:
        predicate = reader.JsonFieldPathFilter(
            field_id=ENVELOPE_FIELD, json_path=f"properties.{_property_key(self.key)}"
        )
        if self.op == "eq":
            _scalar(self.value)
            if type(self.value) is bool:
                predicate.boolean = self.value
            elif type(self.value) is int:
                predicate.int = self.value
            elif type(self.value) is float:
                predicate.float = self.value
            elif isinstance(self.value, str):
                predicate.text = self.value
            else:
                raise ValueError("Null equality is not supported; null is stored but not indexed")
        elif self.op == "range":
            bounds = [bound for bound in (self.lower, self.upper) if bound is not None]
            if not bounds or any(type(bound) not in (int, float) for bound in bounds):
                raise ValueError("A range requires at least one numeric bound")
            for bound in bounds:
                _scalar(bound)
            if len({type(bound) for bound in bounds}) != 1:
                raise ValueError("Use the same numeric type for both bounds")
            if self.lower is not None and self.upper is not None and self.lower > self.upper:
                raise ValueError("Range lower bound exceeds upper bound")
            if type(bounds[0]) is int:
                assert self.lower is None or type(self.lower) is int
                assert self.upper is None or type(self.upper) is int
                predicate.int_range.CopyFrom(
                    reader.JsonFieldPathFilter.IntegerRangePredicate(lower=self.lower, upper=self.upper)
                )
            else:
                predicate.float_range.CopyFrom(
                    reader.JsonFieldPathFilter.FloatRangePredicate(lower=self.lower, upper=self.upper)
                )
        else:
            raise ValueError(f"Unsupported property operator: {self.op}")
        return reader.JsonFilterExpression(path=predicate)


class EmbeddedNidx:
    """Local harness. Use a dedicated PostgreSQL database: nidx runs migrations."""

    def __init__(self, database_url: str, directory: Path) -> None:
        self.inbox = directory.resolve() / "inbox"
        segments = directory.resolve() / "segments"
        self.inbox.mkdir(parents=True, exist_ok=True)
        segments.mkdir(parents=True, exist_ok=True)
        self.binding = NidxBinding(
            {
                "METADATA__DATABASE_URL": database_url,
                "INDEXER__OBJECT_STORE": "file",
                "INDEXER__FILE_PATH": str(self.inbox),
                "STORAGE__OBJECT_STORE": "file",
                "STORAGE__FILE_PATH": str(segments),
                "SEARCHER__METADATA_REFRESH_INTERVAL": "0.1",
            }
        )
        self._api_channel = grpc.insecure_channel(f"localhost:{self.binding.api_port}")
        self._search_channel = grpc.insecure_channel(f"localhost:{self.binding.searcher_port}")
        grpc.channel_ready_future(self._api_channel).result(timeout=RPC_TIMEOUT)
        grpc.channel_ready_future(self._search_channel).result(timeout=RPC_TIMEOUT)
        self.api = nidx_pb2_grpc.NidxApiStub(self._api_channel)
        self.searcher = nidx_pb2_grpc.NidxSearcherStub(self._search_channel)

    def close(self) -> None:
        self._api_channel.close()
        self._search_channel.close()
        del self.binding

    def index(self, message: writer.IndexMessage, resource: resources.Resource | None = None) -> None:
        if resource is None:
            self.binding.index(message.SerializeToString())
            return
        key = f"{uuid4().hex}.pb"
        path = self.inbox / key
        path.write_bytes(resource.SerializeToString())
        message.storage_key = key
        try:
            self.binding.index(message.SerializeToString())
        finally:
            # Embedded indexing consumes the payload synchronously, including on failure.
            path.unlink()


class Graph:
    """Single-writer, one-shard-per-tenant storage spike, with bounded visibility waits."""

    def __init__(
        self, nidx: EmbeddedNidx, tenant: str, shard_id: str, *, max_records: int = 1000
    ) -> None:
        self.nidx = nidx
        self.tenant = str(UUID(tenant))
        self.shard_id = str(UUID(shard_id))
        if not 1 <= max_records <= 100_000:
            raise ValueError("max_records must be between 1 and 100000")
        self.max_records = max_records
        shard = nidx.api.GetShard(
            reader.GetShardRequest(shard_id=resources.ShardId(id=self.shard_id)),
            timeout=RPC_TIMEOUT,
        )
        if UUID(shard.metadata.kbid) != UUID(self.tenant):
            raise ValueError("Shard does not belong to this tenant")
        self._wait(self._shard_visible, "shard indexes")

    @classmethod
    def create(cls, nidx: EmbeddedNidx, tenant: str) -> Graph:
        tenant = str(UUID(tenant))
        created = nidx.api.NewShard(
            writer.NewShardRequest(
                kbid=tenant,
                # nidx currently requires at least one vector index, even without embeddings.
                vectorsets_configs={"unused": writer.VectorIndexConfig(vector_dimension=3)},
            ),
            timeout=RPC_TIMEOUT,
        )
        return cls(nidx, tenant, created.id)

    def _shard_visible(self) -> bool:
        try:
            self._get_resource(self.resource_id("node", "__readiness__"))
            response = self.nidx.searcher.Search(
                reader.SearchRequest(
                    shard_ids=[self.shard_id],
                    paragraph=True,
                    result_per_page=1,
                    json_filter=reader.JsonFilterExpression(
                        path=reader.JsonFieldPathFilter(
                            field_id=ENVELOPE_FIELD, json_path="kind", text="node"
                        )
                    ),
                ),
                timeout=RPC_TIMEOUT,
            )
        except grpc.RpcError as error:
            if error.code() != grpc.StatusCode.NOT_FOUND:
                raise
            # A newly created shard is absent from the searcher's cache until its next sync.
            return False
        return list(response.shard_ids) == [self.shard_id]

    def resource_id(self, kind: Kind, external_id: str) -> str:
        if kind not in ("node", "edge"):
            raise ValueError(f"Unknown record kind: {kind}")
        _nonempty(external_id)
        return uuid5(UUID(self.tenant), json.dumps([kind, external_id])).hex

    def get(self, kind: Kind, external_id: str) -> Record | None:
        return self._get_resource(self.resource_id(kind, external_id))

    def _get_resource(self, rid: str) -> Record | None:
        response = self.nidx.searcher.ExtractedTexts(
            nidx_pb2.ExtractedTextsRequest(
                shard_id=self.shard_id,
                field_ids=[
                    nidx_pb2.ExtractedTextsRequest.FieldId(rid=rid, field_type="a", field_name="lpg")
                ],
            ),
            timeout=RPC_TIMEOUT,
        )
        raw = response.fields.get(f"{rid}/{ENVELOPE_FIELD}")
        if raw is None:
            return None
        record = Record(**json.loads(raw))
        if record.tenant != self.tenant or self.resource_id(record.kind, record.id) != rid:
            raise ValueError("Stored record identity does not match tenant/resource")
        return record

    def find(self, kind: Kind, *filters: PropertyFilter) -> list[Record]:
        return self._find(self._filter(kind, filters))

    @staticmethod
    def _filter(kind: Kind, filters: Sequence[PropertyFilter]) -> reader.JsonFilterExpression:
        if kind not in ("node", "edge"):
            raise ValueError(f"Unknown record kind: {kind}")
        expressions = [
            reader.JsonFilterExpression(
                path=reader.JsonFieldPathFilter(field_id=ENVELOPE_FIELD, json_path="kind", text=kind)
            ),
            *(predicate.expression() for predicate in filters),
        ]
        return reader.JsonFilterExpression(
            bool_and=reader.JsonFilterExpression.List(operands=expressions)
        )

    def _find(self, expression: reader.JsonFilterExpression) -> list[Record]:
        response = self.nidx.searcher.Search(
            reader.SearchRequest(
                shard_ids=[self.shard_id],
                paragraph=True,
                result_per_page=self.max_records + 1,
                json_filter=expression,
            ),
            timeout=RPC_TIMEOUT,
        )
        self._complete(response.shard_ids)
        if response.paragraph.next_page or len(response.paragraph.results) > self.max_records:
            raise RuntimeError("Prototype candidate limit exceeded; result would be incomplete")
        records = []
        seen = set()
        for hit in response.paragraph.results:
            if hit.uuid in seen:
                raise RuntimeError("Duplicate record revision visible; retry after synchronization")
            seen.add(hit.uuid)
            record = self._get_resource(hit.uuid)
            if record is None:
                raise RuntimeError("Candidate disappeared during hydration; retry the query")
            records.append(record)
        return sorted(records, key=lambda record: record.id)

    def upsert_node(self, id: str, labels: list[str], properties: dict[str, Scalar]) -> Record:
        current = self.get("node", id)
        if current is not None and current.deleting:
            raise ValueError("Finish the pending node deletion before replacing it")
        record = Record(self.tenant, "node", id, properties, labels=labels)
        self._put(record)
        return record

    def upsert_edge(
        self, id: str, source: str, edge_type: str, target: str, properties: dict[str, Scalar]
    ) -> Record:
        record = Record(
            self.tenant,
            "edge",
            id,
            properties,
            source=source,
            target=target,
            edge_type=edge_type,
        )
        for endpoint in (source, target):
            node = self.get("node", endpoint)
            if node is None or node.deleting:
                raise ValueError(f"Endpoint {endpoint!r} is missing or being deleted")
        self._put(record)
        return record

    def _put(self, record: Record) -> None:
        rid = self.resource_id(record.kind, record.id)
        envelope = json.dumps(asdict(record), allow_nan=False, sort_keys=True)
        indexed = {
            "kind": record.kind,
            "revision": record.revision,
            "properties": {
                _property_key(key): value
                for key, value in record.properties.items()
                if value is not None
            },
        }
        resource = resources.Resource(
            resource=resources.ResourceID(uuid=rid, shard_id=self.shard_id),
            shard_id=self.shard_id,
            texts={ENVELOPE_FIELD: resources.TextInformation(text=envelope)},
            json_fields={ENVELOPE_FIELD: resources.JsonFieldValue(value=json.dumps(indexed))},
            texts_to_delete=[rid],
            paragraphs_to_delete=[rid],
            json_fields_to_delete=[rid],
            relation_fields_to_delete=[RELATION_FIELD],
            vectors_to_delete_in_all_vectorsets=[rid],
        )
        resource.metadata.created.GetCurrentTime()
        resource.metadata.modified.CopyFrom(resource.metadata.created)
        resource.paragraphs[ENVELOPE_FIELD].paragraphs[
            f"{rid}/{ENVELOPE_FIELD}/0-{len(envelope)}"
        ].CopyFrom(resources.IndexParagraph(start=0, end=len(envelope), field=ENVELOPE_FIELD))
        if record.kind == "edge":
            assert record.source is not None and record.target is not None
            assert record.edge_type is not None
            resource.field_relations[RELATION_FIELD].relations.add(
                facets=[f"/lpg/edge/{rid}"],
                relation=utils_pb2.Relation(
                    source=utils_pb2.RelationNode(
                        value=self.resource_id("node", record.source),
                        ntype=utils_pb2.RelationNode.RESOURCE,
                    ),
                    to=utils_pb2.RelationNode(
                        value=self.resource_id("node", record.target),
                        ntype=utils_pb2.RelationNode.RESOURCE,
                    ),
                    relation=utils_pb2.Relation.OTHER,
                    relation_label=record.edge_type,
                ),
            )
        self.nidx.index(self._message(rid, writer.CREATION), resource)
        self._wait(lambda: self._visible(record), f"record {record.kind}/{record.id}")

    def _visible(self, record: Record) -> bool:
        if self.get(record.kind, record.id) != record:
            return False
        matches = self._find(
            reader.JsonFilterExpression(
                path=reader.JsonFieldPathFilter(
                    field_id=ENVELOPE_FIELD, json_path="revision", text=record.revision
                )
            )
        )
        if matches != [record]:
            return False
        rid = self.resource_id(record.kind, record.id)
        topology = self._topology(rid=rid)
        if record.kind == "node":
            return not topology.graph
        if len(topology.graph) != 1:
            return False
        assert record.source is not None and record.target is not None
        path = topology.graph[0]
        return (
            path.resource_field_id == f"{rid}/{RELATION_FIELD}"
            and topology.nodes[path.source].value == self.resource_id("node", record.source)
            and topology.nodes[path.destination].value == self.resource_id("node", record.target)
            and topology.relations[path.relation].label == record.edge_type
        )

    def topology(self, *filters: PropertyFilter) -> list[Record]:
        """Return edge records via JSON-prefiltered topology, preserving parallel edges."""
        response = self.nidx.searcher.Search(
            reader.SearchRequest(
                shard_ids=[self.shard_id],
                result_per_page=self.max_records + 1,
                json_filter=self._filter("edge", filters),
                graph_search=reader.SearchRequest.GraphSearch(
                    query=reader.GraphQuery(
                        path=reader.GraphQuery.PathQuery(path=reader.GraphQuery.Path())
                    )
                ),
            ),
            timeout=RPC_TIMEOUT,
        )
        self._complete(response.shard_ids)
        if len(response.graph.graph) > self.max_records:
            raise RuntimeError("Prototype topology limit exceeded; result would be incomplete")
        records = []
        for path in response.graph.graph:
            rid, field_id = path.resource_field_id.split("/", 1)
            record = self._get_resource(rid)
            if record is None or record.kind != "edge" or field_id != RELATION_FIELD:
                raise RuntimeError("Invalid topology ownership")
            records.append(record)
        if len({record.id for record in records}) != len(records):
            raise RuntimeError("Duplicate topology revision visible")
        return sorted(records, key=lambda record: record.id)

    def _topology(self, *, rid: str) -> reader.GraphSearchResponse:
        response = self.nidx.searcher.GraphSearch(
            reader.GraphSearchRequest(
                shard_ids=[self.shard_id],
                top_k=2,
                # Query ownership directly: a text prefilter could hide a stale triple
                # after its owning text field has already disappeared.
                query=reader.GraphQuery(
                    path=reader.GraphQuery.PathQuery(
                        facet=reader.GraphQuery.FacetFilter(facet=f"/lpg/edge/{rid}")
                    )
                ),
            ),
            timeout=RPC_TIMEOUT,
        )
        self._complete(response.shard_ids)
        return response

    def delete_edge(self, id: str) -> None:
        self._delete("edge", id)

    def delete_node(self, id: str, *, detach: bool = False) -> None:
        record = self.get("node", id)
        if record is None:
            return
        edges = [edge for edge in self.find("edge") if id in (edge.source, edge.target)]
        if edges and not detach:
            raise ValueError("Node has incident edges; use detach=True")
        # A durable marker lets a new Python Graph instance resume a partial cascade.
        self._put(
            Record(self.tenant, "node", id, record.properties, labels=record.labels, deleting=True)
        )
        for edge in edges:
            self.delete_edge(edge.id)
        self._delete("node", id)

    def _delete(self, kind: Kind, id: str) -> None:
        rid = self.resource_id(kind, id)
        self.nidx.index(self._message(rid, writer.DELETION))

        def absent() -> bool:
            if self.get(kind, id) is not None:
                return False
            candidates = self.nidx.searcher.Search(
                reader.SearchRequest(
                    shard_ids=[self.shard_id],
                    paragraph=True,
                    result_per_page=1,
                    field_filter=reader.FilterExpression(
                        resource=reader.FilterExpression.ResourceFilter(resource_id=rid)
                    ),
                ),
                timeout=RPC_TIMEOUT,
            )
            self._complete(candidates.shard_ids)
            return not candidates.paragraph.results and not self._topology(rid=rid).graph

        self._wait(absent, f"deletion {kind}/{id}")

    def _message(self, rid: str, kind: writer.TypeMessage.ValueType) -> writer.IndexMessage:
        return writer.IndexMessage(shard=self.shard_id, resource=rid, kbid=self.tenant, typemessage=kind)

    def _complete(self, shard_ids: Sequence[str]) -> None:
        if list(shard_ids) != [self.shard_id]:
            raise RuntimeError("nidx returned an incomplete shard response")

    @staticmethod
    def _wait(check: Callable[[], bool], description: str, timeout: float = 15) -> None:
        deadline = time.monotonic() + timeout
        while True:
            if check():
                return
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for nidx visibility: {description}")
            time.sleep(0.05)
