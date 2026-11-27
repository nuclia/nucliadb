# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Literal, Protocol, TypeAlias
from uuid import UUID, uuid4

import grpc
from nidx_protos import nidx_pb2, nidx_pb2_grpc
from nidx_protos import nodereader_pb2 as reader
from nidx_protos import noderesources_pb2 as resources
from nidx_protos import nodewriter_pb2 as writer

from nucliadb_protos import utils_pb2

Scalar: TypeAlias = str | int | float | bool | None
PropertyValue: TypeAlias = Scalar | datetime | list[str]
Kind: TypeAlias = Literal["node", "edge"]
Direction: TypeAlias = Literal["outgoing", "incoming", "both"]
NodeFilterScope: TypeAlias = Literal["all", "intermediate"]
RPC_TIMEOUT = 60
DEFAULT_VECTORSET = "lpg-property-v1"
DEFAULT_VECTOR_DIMENSION = 3
MAX_SEARCH_RESULTS = 10_000


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


def _property_value(value: PropertyValue) -> None:
    if isinstance(value, datetime):
        return
    if isinstance(value, list):
        if any(not isinstance(item, str) for item in value):
            raise ValueError("String-list properties must contain only strings")
        return
    _scalar(value)


def _validate_property_name(key: str) -> None:
    if not isinstance(key, str):
        raise ValueError("Property keys must be strings")


@dataclass(frozen=True)
class Record:
    tenant: str
    kind: Kind
    id: str
    properties: dict[str, PropertyValue]
    labels: list[str] = field(default_factory=list)
    source: str | None = None
    target: str | None = None
    edge_type: str | None = None
    revision: str = field(default_factory=lambda: uuid4().hex)
    text_chunks: dict[str, list[str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        UUID(self.tenant)
        _nonempty(self.id)
        if self.kind not in ("node", "edge"):
            raise ValueError("Unsupported LPG record kind")
        if not isinstance(self.properties, dict) or not isinstance(self.labels, list):
            raise ValueError("Invalid LPG properties or labels")
        for key, value in self.properties.items():
            _validate_property_name(key)
            _property_value(value)
        for label in self.labels:
            _nonempty(label)
        if self.kind == "node":
            if not self.labels:
                raise ValueError("Nodes require at least one label/type")
            if len(set(self.labels)) != len(self.labels):
                raise ValueError("Node labels/types must be unique")
        if not isinstance(self.text_chunks, dict):
            raise ValueError("Text chunks must be a property-to-chunks mapping")
        for key, chunks in self.text_chunks.items():
            _validate_property_name(key)
            if not isinstance(chunks, list) or not chunks:
                raise ValueError("Text properties must contain a nonempty list of chunks")
            if any(not isinstance(chunk, str) or not chunk.strip() for chunk in chunks):
                raise ValueError("Text chunks must be nonempty strings")
        if self.kind == "edge":
            if self.labels:
                raise ValueError("Edges use edge_type and cannot have node labels")
            if self.source is None or self.target is None or self.edge_type is None:
                raise ValueError("Edges require source, target, and edge_type")
            for value in (self.source, self.target, self.edge_type):
                _nonempty(value)
        elif any(value is not None for value in (self.source, self.target, self.edge_type)):
            raise ValueError("Nodes cannot have edge endpoints or type")
        if not isinstance(self.revision, str):
            raise ValueError("Invalid revision")


@dataclass(frozen=True)
class FindPage:
    records: list[Record]
    next_page_token: str


@dataclass(frozen=True)
class PropertyFilter:
    key: str
    op: Literal["eq", "range"]
    value: Scalar | datetime = None
    lower: int | float | datetime | None = None
    upper: int | float | datetime | None = None

    def to_proto(self) -> nidx_pb2.LpgPropertyFilter:
        _validate_property_name(self.key)
        predicate = nidx_pb2.LpgPropertyFilter(property=self.key)
        if self.op == "eq":
            if isinstance(self.value, datetime):
                predicate.datetime = self.value.isoformat()
                return predicate
            _scalar(self.value)
            if type(self.value) is bool:
                predicate.boolean = self.value
            elif type(self.value) is int:
                predicate.integer = self.value
            elif type(self.value) is float:
                predicate.float = self.value
            elif isinstance(self.value, str):
                predicate.text = self.value
            else:
                raise ValueError("Null equality is not supported; null is stored but not indexed")
        elif self.op == "range":
            bounds = [bound for bound in (self.lower, self.upper) if bound is not None]
            if any(isinstance(bound, datetime) for bound in bounds):
                if not all(isinstance(bound, datetime) for bound in bounds):
                    raise ValueError("Use datetime values for both date bounds")
                assert self.lower is None or isinstance(self.lower, datetime)
                assert self.upper is None or isinstance(self.upper, datetime)
                lower = (
                    self.lower.replace(tzinfo=timezone.utc)
                    if self.lower is not None and self.lower.utcoffset() is None
                    else self.lower
                )
                upper = (
                    self.upper.replace(tzinfo=timezone.utc)
                    if self.upper is not None and self.upper.utcoffset() is None
                    else self.upper
                )
                if lower is not None and upper is not None and lower > upper:
                    raise ValueError("Range lower bound exceeds upper bound")
                predicate.date_range.CopyFrom(
                    nidx_pb2.LpgDateRange(
                        lower=self.lower.isoformat() if self.lower is not None else None,
                        upper=self.upper.isoformat() if self.upper is not None else None,
                    )
                )
                return predicate
            if not bounds or any(type(bound) not in (int, float) for bound in bounds):
                raise ValueError("A range requires at least one numeric bound")
            for bound in bounds:
                assert not isinstance(bound, datetime)
                _scalar(bound)
            assert not isinstance(self.lower, datetime)
            assert not isinstance(self.upper, datetime)
            if len({type(bound) for bound in bounds}) != 1:
                raise ValueError("Use the same numeric type for both bounds")
            if self.lower is not None and self.upper is not None and self.lower > self.upper:
                raise ValueError("Range lower bound exceeds upper bound")
            if type(bounds[0]) is int:
                assert self.lower is None or type(self.lower) is int
                assert self.upper is None or type(self.upper) is int
                predicate.integer_range.CopyFrom(
                    nidx_pb2.LpgIntegerRange(lower=self.lower, upper=self.upper)
                )
            else:
                predicate.float_range.CopyFrom(
                    nidx_pb2.LpgFloatRange(lower=self.lower, upper=self.upper)
                )
        else:
            raise ValueError(f"Unsupported property operator: {self.op}")
        return predicate


@dataclass(frozen=True)
class SearchHit:
    record: Record
    property: str
    chunk_index: int
    text: str
    score: float
    text_rank: int | None
    vector_rank: int | None


@dataclass(frozen=True)
class ExpansionMatch:
    node_id: str
    edge: Record
    neighbor: Record


@dataclass(frozen=True)
class Expansion:
    matches: list[ExpansionMatch]
    examined_edges: int


@dataclass(frozen=True)
class Path:
    nodes: list[Record]
    edges: list[Record]
    examined_edges: int


def _lpg_direction(direction: Direction) -> nidx_pb2.LpgDirection.ValueType:
    if direction == "outgoing":
        return nidx_pb2.LPG_OUTGOING
    if direction == "incoming":
        return nidx_pb2.LPG_INCOMING
    if direction == "both":
        return nidx_pb2.LPG_BOTH
    raise ValueError(f"Unknown graph direction: {direction!r}")


def _string_sequence(values: Sequence[str]) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise ValueError("Expected a sequence of strings, not a single string")
    items = list(values)
    for value in items:
        _nonempty(value)
    return items


def _validate_embeddings(
    text_chunks: dict[str, list[str]],
    embeddings: Mapping[str, Sequence[Sequence[float]]] | None,
    dimension: int,
) -> dict[str, list[list[float]]]:
    if embeddings is None:
        return {}
    if not isinstance(embeddings, Mapping):
        raise ValueError("Embeddings must map text-property keys to chunk vectors")
    vectors_by_property: dict[str, list[list[float]]] = {}
    for key, vectors in embeddings.items():
        if key not in text_chunks:
            raise ValueError(f"Embeddings supplied for non-text property {key!r}")
        if not isinstance(vectors, Sequence) or isinstance(vectors, (str, bytes)):
            raise ValueError("Each embedding property must contain a sequence of vectors")
        if len(vectors) != len(text_chunks[key]):
            raise ValueError(f"Property {key!r} requires one embedding per text chunk")
        normalized_vectors = []
        for vector in vectors:
            if not isinstance(vector, Sequence) or isinstance(vector, (str, bytes)):
                raise ValueError("Each embedding must be a sequence of numbers")
            if len(vector) != dimension:
                raise ValueError(f"Embeddings must have exactly {dimension} dimensions")
            if any(type(value) not in (int, float) or not math.isfinite(value) for value in vector):
                raise ValueError("Embedding values must be finite numbers")
            result = [float(value) for value in vector]
            if not any(value != 0 for value in result):
                raise ValueError("Zero vectors are not supported with cosine similarity")
            normalized_vectors.append(result)
        vectors_by_property[key] = normalized_vectors
    return vectors_by_property


class NidxLpgClient(Protocol):
    def LPGOperation(
        self, request: nidx_pb2.LpgOperationRequest, timeout: float | None = None
    ) -> nidx_pb2.LpgOperationResponse: ...

    def GetRecords(
        self, request: nidx_pb2.GetRecordsRequest, timeout: float | None = None
    ) -> nidx_pb2.GetRecordsResponse: ...

    def FindRecords(
        self, request: nidx_pb2.FindRecordsRequest, timeout: float | None = None
    ) -> nidx_pb2.FindRecordsResponse: ...

    def Expand(
        self, request: nidx_pb2.ExpandRequest, timeout: float | None = None
    ) -> nidx_pb2.ExpandResponse: ...

    def ShortestPath(
        self, request: nidx_pb2.ShortestPathRequest, timeout: float | None = None
    ) -> nidx_pb2.ShortestPathResponse: ...

    def SearchChunks(
        self, request: nidx_pb2.SearchChunksRequest, timeout: float | None = None
    ) -> nidx_pb2.SearchChunksResponse: ...


class NidxClient:
    """Connection to a trusted Nidx API listener with LPG enabled."""

    def __init__(self, address: str) -> None:
        _nonempty(address)
        self._channel = grpc.insecure_channel(address)
        self.api = nidx_pb2_grpc.NidxApiStub(self._channel)
        self.lpg_client: NidxLpgClient = nidx_pb2_grpc.NidxLPGStub(self._channel)

    def close(self) -> None:
        self._channel.close()


def _lpg_kind(kind: Kind) -> nidx_pb2.LpgRecordKind.ValueType:
    if kind == "node":
        return nidx_pb2.LPG_NODE
    if kind == "edge":
        return nidx_pb2.LPG_EDGE
    raise ValueError(f"Unknown record kind: {kind}")


def _kind_name(value: nidx_pb2.LpgRecordKind.ValueType) -> Kind:
    if value == nidx_pb2.LPG_NODE:
        return "node"
    if value == nidx_pb2.LPG_EDGE:
        return "edge"
    raise ValueError(f"Unknown NidxLPG record kind: {value}")


def _to_lpg_property(value: PropertyValue) -> nidx_pb2.LpgProperty:
    _property_value(value)
    result = nidx_pb2.LpgProperty()
    if isinstance(value, datetime):
        result.datetime = value.isoformat()
    elif isinstance(value, list):
        result.strings.CopyFrom(nidx_pb2.LpgStringList(values=value))
    elif value is None:
        result.null_value = True
    elif type(value) is str:
        result.text = value
    elif type(value) is bool:
        result.boolean = value
    elif type(value) is int:
        result.integer = value
    elif type(value) is float:
        result.float = value
    else:
        raise ValueError("Unsupported LPG property value")
    return result


def _from_lpg_property(value: nidx_pb2.LpgProperty) -> PropertyValue:
    kind = value.WhichOneof("value")
    if kind == "datetime":
        try:
            return datetime.fromisoformat(value.datetime)
        except ValueError as error:
            raise ValueError(f"Invalid NidxLPG datetime property: {value.datetime!r}") from error
    if kind == "strings":
        return list(value.strings.values)
    if kind == "text":
        return value.text
    if kind == "integer":
        return value.integer
    if kind == "float":
        return value.float
    if kind == "boolean":
        return value.boolean
    if kind == "null_value":
        if not value.null_value:
            raise ValueError("Invalid NidxLPG null property")
        return None
    raise ValueError(f"Property type {kind!r} is not supported by the Python LPG facade")


def _from_lpg_record(record: nidx_pb2.LpgRecord, tenant: str) -> Record:
    if not record.HasField("key"):
        raise RuntimeError("NidxLPG returned a record without a key")
    kind = _kind_name(record.key.kind)
    return Record(
        tenant=tenant,
        kind=kind,
        id=record.key.id,
        properties={key: _from_lpg_property(value) for key, value in record.properties.items()},
        labels=list(record.labels),
        source=record.source if record.HasField("source") else None,
        target=record.target if record.HasField("target") else None,
        edge_type=record.edge_type if record.HasField("edge_type") else None,
        revision=record.revision,
        text_chunks={name: list(value.chunks) for name, value in record.text_properties.items()},
    )


class Graph:
    """Python facade for an existing tenant-owned NidxLPG shard.

    Use ``create`` to create a shard, or construct a Graph with an existing
    shard ID. ``max_records`` bounds the list-returning enumeration helpers.
    Search uses ``top_k``; ``find_page`` exposes independent pagination.
    """

    def __init__(
        self,
        nidx: NidxClient,
        tenant: str,
        shard_id: str,
        *,
        vectorset: str = DEFAULT_VECTORSET,
        vector_dimension: int = DEFAULT_VECTOR_DIMENSION,
        max_records: int = 1000,
    ) -> None:
        self.nidx = nidx
        self.lpg: NidxLpgClient = nidx.lpg_client
        self.tenant = str(UUID(tenant))
        self.shard_id = str(UUID(shard_id))
        _nonempty(vectorset)
        if not 1 <= vector_dimension <= 4096:
            raise ValueError("vector_dimension must be between 1 and 4096")
        if not 1 <= max_records <= 100_000:
            raise ValueError("max_records must be between 1 and 100000")
        self.vectorset = vectorset
        self.vector_dimension = vector_dimension
        self.max_records = max_records
        shard = nidx.api.GetShard(
            reader.GetShardRequest(shard_id=resources.ShardId(id=self.shard_id)),
            timeout=RPC_TIMEOUT,
        )
        if UUID(shard.metadata.kbid) != UUID(self.tenant):
            raise ValueError("Shard does not belong to this tenant")
        vectorsets = nidx.api.ListVectorSets(resources.ShardId(id=self.shard_id), timeout=RPC_TIMEOUT)
        if self.vectorset not in vectorsets.vectorsets:
            raise ValueError(f"Shard is missing LPG property vectorset {self.vectorset!r}")
        self._wait(self._shard_visible, "shard indexes")

    def _shard_visible(self) -> bool:
        try:
            self._get_proto("node", "__lpg_readiness__")
        except grpc.RpcError as error:
            if error.code() != grpc.StatusCode.NOT_FOUND:
                raise
            return False
        return True

    @classmethod
    def create(
        cls,
        nidx: NidxClient,
        tenant: str,
        *,
        vectorset: str = DEFAULT_VECTORSET,
        vector_dimension: int = DEFAULT_VECTOR_DIMENSION,
        max_records: int = 1000,
    ) -> Graph:
        """Create a graph shard for the tenant with a cosine-similarity vectorset."""
        tenant = str(UUID(tenant))
        _nonempty(vectorset)
        if not 1 <= vector_dimension <= 4096:
            raise ValueError("vector_dimension must be between 1 and 4096")
        created = nidx.api.NewShard(
            writer.NewShardRequest(
                kbid=tenant,
                vectorsets_configs={
                    vectorset: writer.VectorIndexConfig(
                        similarity=utils_pb2.COSINE,
                        vector_dimension=vector_dimension,
                    )
                },
            ),
            timeout=RPC_TIMEOUT,
        )
        return cls(
            nidx,
            tenant,
            created.id,
            vectorset=vectorset,
            vector_dimension=vector_dimension,
            max_records=max_records,
        )

    def _key(self, kind: Kind, id: str) -> nidx_pb2.LpgRecordKey:
        if kind not in ("node", "edge"):
            raise ValueError(f"Unknown record kind: {kind}")
        _nonempty(id)
        return nidx_pb2.LpgRecordKey(kind=_lpg_kind(kind), id=id)

    def _operation(self, operation: nidx_pb2.LpgOperation) -> nidx_pb2.LpgOperationResult:
        response = self.lpg.LPGOperation(
            nidx_pb2.LpgOperationRequest(graph_id=self.shard_id, operations=[operation]),
            timeout=RPC_TIMEOUT,
        )
        if len(response.results) != 1:
            raise RuntimeError("NidxLPG returned an invalid operation result count")
        result = response.results[0]
        if result.status != nidx_pb2.LPG_OPERATION_STATUS_APPLIED:
            raise RuntimeError(f"NidxLPG operation failed ({result.error_code}): {result.error_message}")
        return result

    def _get_proto(self, kind: Kind, id: str) -> nidx_pb2.LpgRecord | None:
        response = self.lpg.GetRecords(
            nidx_pb2.GetRecordsRequest(graph_id=self.shard_id, keys=[self._key(kind, id)]),
            timeout=RPC_TIMEOUT,
        )
        if not response.records:
            return None
        record = response.records[0]
        if record.key.kind != _lpg_kind(kind) or record.key.id != id:
            raise RuntimeError("NidxLPG returned a record for a different key")
        return record

    def get(self, kind: Kind, external_id: str) -> Record | None:
        """Return the node or edge with this ID, or None if it does not exist."""
        proto = self._get_proto(kind, external_id)
        return None if proto is None else _from_lpg_record(proto, self.tenant)

    def _to_proto(
        self,
        record: Record,
        embeddings: Mapping[str, Sequence[Sequence[float]]] | None = None,
    ) -> nidx_pb2.LpgRecord:
        if record.tenant != self.tenant:
            raise ValueError("Record belongs to a different tenant")
        vectors = _validate_embeddings(record.text_chunks, embeddings, self.vector_dimension)
        proto = nidx_pb2.LpgRecord(
            key=self._key(record.kind, record.id),
            labels=record.labels,
            properties={key: _to_lpg_property(value) for key, value in record.properties.items()},
            vectorset=self.vectorset,
            revision=record.revision,
        )
        if record.kind == "edge":
            assert (
                record.source is not None and record.target is not None and record.edge_type is not None
            )
            proto.source = record.source
            proto.target = record.target
            proto.edge_type = record.edge_type
        for name, chunks in record.text_chunks.items():
            text_property = nidx_pb2.LpgTextProperty(chunks=chunks)
            if name in vectors:
                text_property.embeddings.extend(
                    nidx_pb2.LpgVector(values=vector) for vector in vectors[name]
                )
            proto.text_properties[name].CopyFrom(text_property)
        return proto

    def _put(
        self,
        record: Record,
        embeddings: Mapping[str, Sequence[Sequence[float]]] | None = None,
    ) -> None:
        proto = self._to_proto(record, embeddings)
        self._operation(nidx_pb2.LpgOperation(upsert_record=proto))
        self._wait(
            lambda: self.get(record.kind, record.id) == record, f"record {record.kind}/{record.id}"
        )

    def upsert_node(
        self,
        id: str,
        labels: list[str],
        properties: dict[str, PropertyValue],
        *,
        text_chunks: dict[str, list[str]] | None = None,
        embeddings: Mapping[str, Sequence[Sequence[float]]] | None = None,
    ) -> Record:
        """Create or fully replace a node and return it.

        Labels are required and immutable. Optional embeddings must provide
        one vector per text chunk.
        """
        current = self.get("node", id)
        chunks = text_chunks or {}
        record = Record(self.tenant, "node", id, properties, labels=labels, text_chunks=chunks)
        if current is not None and set(current.labels) != set(record.labels):
            raise ValueError("Node labels/types are immutable; delete and recreate the node")
        vectors = _validate_embeddings(chunks, embeddings, self.vector_dimension)
        self._put(record, vectors)
        return record

    def upsert_edge(
        self,
        id: str,
        source: str,
        edge_type: str,
        target: str,
        properties: dict[str, PropertyValue],
        *,
        text_chunks: dict[str, list[str]] | None = None,
        embeddings: Mapping[str, Sequence[Sequence[float]]] | None = None,
    ) -> Record:
        """Create or fully replace a directed edge and return it.

        Both endpoints must exist, and an existing edge's type is immutable.
        Optional embeddings must provide one vector per text chunk.
        """
        chunks = text_chunks or {}
        record = Record(
            self.tenant,
            "edge",
            id,
            properties,
            source=source,
            target=target,
            edge_type=edge_type,
            text_chunks=chunks,
        )
        current = self.get("edge", id)
        if current is not None and current.edge_type != record.edge_type:
            raise ValueError("Edge type is immutable; delete and recreate the edge")
        vectors = _validate_embeddings(chunks, embeddings, self.vector_dimension)
        for endpoint_id in (source, target):
            node = self.get("node", endpoint_id)
            if node is None:
                raise ValueError(f"Endpoint {endpoint_id!r} is missing")
        self._put(record, vectors)
        return record

    def _find_page(
        self,
        kind: Kind,
        filters: Sequence[PropertyFilter],
        *,
        page_size: int,
        page_token: str,
        incident_node: str | None = None,
    ) -> FindPage:
        if kind not in ("node", "edge"):
            raise ValueError(f"Unknown record kind: {kind}")
        if type(page_size) is not int or not 1 <= page_size <= 1000:
            raise ValueError("page_size must be between 1 and 1000")
        if not isinstance(page_token, str):
            raise ValueError("page_token must be a string")
        request = nidx_pb2.FindRecordsRequest(
            graph_id=self.shard_id,
            kind=_lpg_kind(kind),
            filters=[predicate.to_proto() for predicate in filters],
            page_size=page_size,
            page_token=page_token,
        )
        if incident_node is not None:
            request.incident_node = incident_node
        response = self.lpg.FindRecords(request, timeout=RPC_TIMEOUT)
        return FindPage(
            [_from_lpg_record(record, self.tenant) for record in response.records],
            response.next_page_token,
        )

    def find_page(
        self,
        kind: Kind,
        *filters: PropertyFilter,
        page_size: int = 100,
        page_token: str = "",
    ) -> FindPage:
        """Return one resource-ID-ordered page, independent of max_records.

        Pass next_page_token with the same kind/filters to continue. An empty
        next_page_token marks the end. Pages are not a snapshot of concurrent writes.
        """
        return self._find_page(kind, filters, page_size=page_size, page_token=page_token)

    def _find(
        self,
        kind: Kind,
        filters: Sequence[PropertyFilter] = (),
        *,
        incident_node: str | None = None,
    ) -> list[Record]:
        token = ""
        seen_tokens: set[str] = set()
        records: list[Record] = []
        page_size = min(1000, self.max_records + 1)
        while True:
            page = self._find_page(
                kind,
                filters,
                page_size=page_size,
                page_token=token,
                incident_node=incident_node,
            )
            records.extend(page.records)
            if len(records) > self.max_records:
                raise RuntimeError("Prototype candidate limit exceeded; result would be incomplete")
            token = page.next_page_token
            if not token:
                return sorted(records, key=lambda record: record.id)
            if token in seen_tokens:
                raise RuntimeError("NidxLPG repeated a pagination token")
            seen_tokens.add(token)

    def find(self, kind: Kind, *filters: PropertyFilter) -> list[Record]:
        """Return matching nodes or edges sorted by ID, applying all filters.

        Raise RuntimeError if the result exceeds ``max_records``.
        Use find_page for caller-controlled pagination without that total limit.
        """
        if kind not in ("node", "edge"):
            raise ValueError(f"Unknown record kind: {kind}")
        return self._find(kind, filters)

    def topology(self, *filters: PropertyFilter) -> list[Record]:
        """Return edges sorted by ID, applying filters to edge properties."""
        return self._find("edge", filters)

    def expand(
        self,
        node_ids: Sequence[str],
        *,
        direction: Direction,
        edge_types: Sequence[str] = (),
        edge_filters: Sequence[PropertyFilter] = (),
        neighbor_label: str | None = None,
        neighbor_filters: Sequence[PropertyFilter] = (),
        candidate_limit: int = 1000,
        max_results: int = 1000,
    ) -> Expansion:
        """Expand one hop from the given nodes in the requested direction.

        Optional filters restrict edges and neighbors. The result contains
        start-node, edge, and neighbor bindings and the examined edge count.
        """
        response = self.lpg.Expand(
            nidx_pb2.ExpandRequest(
                graph_id=self.shard_id,
                node_ids=_string_sequence(node_ids),
                direction=_lpg_direction(direction),
                edge_types=_string_sequence(edge_types),
                edge_filters=[predicate.to_proto() for predicate in edge_filters],
                neighbor_label=neighbor_label,
                neighbor_filters=[predicate.to_proto() for predicate in neighbor_filters],
                candidate_limit=candidate_limit,
                max_results=max_results,
            ),
            timeout=RPC_TIMEOUT,
        )
        edges = [_from_lpg_record(record, self.tenant) for record in response.edges]
        neighbors = [_from_lpg_record(record, self.tenant) for record in response.neighbors]
        matches = []
        for match in response.matches:
            if match.edge_index >= len(edges) or match.neighbor_index >= len(neighbors):
                raise RuntimeError("NidxLPG returned an invalid expansion record index")
            matches.append(
                ExpansionMatch(
                    node_id=match.node_id,
                    edge=edges[match.edge_index],
                    neighbor=neighbors[match.neighbor_index],
                )
            )
        return Expansion(matches=matches, examined_edges=response.examined_edges)

    def shortest_path(
        self,
        source: str,
        target: str,
        *,
        direction: Direction,
        edge_types: Sequence[str] = (),
        edge_filters: Sequence[PropertyFilter] = (),
        node_label: str | None = None,
        node_filters: Sequence[PropertyFilter] = (),
        node_filter_scope: NodeFilterScope = "all",
        max_hops: int = 6,
        max_visited_nodes: int = 1000,
        max_examined_edges: int = 10000,
    ) -> Path | None:
        """Return one bounded, unweighted shortest path, or None if not found.

        Node filters apply to all nodes by default; ``intermediate`` exempts
        the source and target. Direction and edge filters constrain traversal.
        """
        _nonempty(source)
        _nonempty(target)
        if node_filter_scope == "all":
            scope = nidx_pb2.LPG_ALL_NODES
        elif node_filter_scope == "intermediate":
            scope = nidx_pb2.LPG_INTERMEDIATE_NODES
        else:
            raise ValueError(f"Unknown node filter scope: {node_filter_scope!r}")
        response = self.lpg.ShortestPath(
            nidx_pb2.ShortestPathRequest(
                graph_id=self.shard_id,
                source=source,
                target=target,
                direction=_lpg_direction(direction),
                edge_types=_string_sequence(edge_types),
                edge_filters=[predicate.to_proto() for predicate in edge_filters],
                node_label=node_label,
                node_filters=[predicate.to_proto() for predicate in node_filters],
                node_filter_scope=scope,
                max_hops=max_hops,
                max_visited_nodes=max_visited_nodes,
                max_examined_edges=max_examined_edges,
            ),
            timeout=RPC_TIMEOUT,
        )
        if not response.found:
            return None
        nodes = [_from_lpg_record(record, self.tenant) for record in response.nodes]
        edges = [_from_lpg_record(record, self.tenant) for record in response.edges]
        if len(nodes) != len(edges) + 1 or nodes[0].id != source or nodes[-1].id != target:
            raise RuntimeError("NidxLPG returned an invalid shortest path")
        return Path(nodes=nodes, edges=edges, examined_edges=response.examined_edges)

    def search(
        self,
        *,
        text: str | None = None,
        vector: Sequence[float] | None = None,
        kind: Kind | None = None,
        text_property: str | None = None,
        filters: Sequence[PropertyFilter] = (),
        top_k: int = 10,
        rrf_k: int = 60,
        min_score_semantic: float = 0.0,
        min_score_keyword: float = 0.0,
    ) -> list[SearchHit]:
        """Search text chunks using text, a vector, or both.

        Hybrid search combines text and vector rankings with reciprocal rank
        fusion. Optional kind, property, and record filters restrict results.
        Minimum scores filter semantic and keyword candidates before fusion,
        not the final reciprocal-rank score. Both default to zero.
        Return up to top_k hits, independent of max_records and the total
        matching chunks. Hybrid fusion uses bounded internal ranked windows.
        """
        if (text is None or not text.strip()) and vector is None:
            raise ValueError("Provide nonempty text, an embedding, or both")
        if text is not None and not text.strip():
            raise ValueError("Text query cannot be empty")
        if vector is not None:
            if len(vector) != self.vector_dimension:
                raise ValueError(f"Query vector must have exactly {self.vector_dimension} dimensions")
            if any(type(value) not in (int, float) or not math.isfinite(value) for value in vector):
                raise ValueError("Query vector values must be finite numbers")
            if not any(value != 0 for value in vector):
                raise ValueError("Zero vectors are not supported with cosine similarity")
        if kind is not None and kind not in ("node", "edge"):
            raise ValueError(f"Unknown record kind: {kind}")
        if text_property is not None:
            _validate_property_name(text_property)
        if type(top_k) is not int or not 1 <= top_k <= MAX_SEARCH_RESULTS:
            raise ValueError(f"top_k must be between 1 and {MAX_SEARCH_RESULTS}")
        if rrf_k < 1:
            raise ValueError("rrf_k must be positive")
        for name, value in (
            ("min_score_semantic", min_score_semantic),
            ("min_score_keyword", min_score_keyword),
        ):
            if type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError(f"{name} must be a finite number")

        request = nidx_pb2.SearchChunksRequest(
            graph_id=self.shard_id,
            vector=list(vector) if vector is not None else [],
            filters=[predicate.to_proto() for predicate in filters],
            top_k=top_k,
            vectorset=self.vectorset,
            rrf_k=rrf_k,
            min_score_semantic=min_score_semantic,
            min_score_keyword=min_score_keyword,
        )
        if text_property is not None:
            request.property = text_property
        if text is not None:
            request.text = text
        if kind is not None:
            request.kind = _lpg_kind(kind)
        response = self.lpg.SearchChunks(request, timeout=RPC_TIMEOUT)
        hits = []
        for hit in response.hits:
            if not hit.HasField("record"):
                raise RuntimeError("NidxLPG returned a search hit without a record")
            hits.append(
                SearchHit(
                    record=_from_lpg_record(hit.record, self.tenant),
                    property=hit.property,
                    chunk_index=hit.chunk_index,
                    text=hit.text,
                    score=hit.score,
                    text_rank=hit.text_rank if hit.HasField("text_rank") else None,
                    vector_rank=hit.vector_rank if hit.HasField("vector_rank") else None,
                )
            )
        return hits

    def update_text_properties(
        self,
        kind: Kind,
        id: str,
        *,
        text_chunks: Mapping[str, Sequence[str]] | None = None,
        embeddings: Mapping[str, Sequence[Sequence[float]]] | None = None,
    ) -> Record:
        """Update selected text chunks and/or embeddings on an existing record.

        Replacing chunks clears their previous embeddings unless new ones are
        supplied; other text properties are preserved.
        """
        if not text_chunks and not embeddings:
            raise ValueError("Provide text chunks, embeddings, or both")
        current = self.get(kind, id)
        if current is None:
            raise ValueError(f"Cannot update text properties for missing {kind} {id!r}")
        if text_chunks is not None and not isinstance(text_chunks, Mapping):
            raise ValueError("Text chunks must map property names to chunk sequences")
        updated_chunks = {name: list(chunks) for name, chunks in current.text_chunks.items()}
        for name, chunks in (text_chunks or {}).items():
            if not isinstance(chunks, Sequence) or isinstance(chunks, (str, bytes)):
                raise ValueError("Each text property must contain a sequence of chunks")
            updated_chunks[name] = list(chunks)
        replace(current, text_chunks=updated_chunks)
        vectors = _validate_embeddings(updated_chunks, embeddings, self.vector_dimension)
        patches: dict[str, nidx_pb2.LpgTextPropertyPatch] = {}
        for name, chunks in (text_chunks or {}).items():
            patch = nidx_pb2.LpgTextPropertyPatch()
            patch.chunks.CopyFrom(nidx_pb2.LpgTextChunks(chunks=list(chunks)))
            if name in vectors:
                patch.embeddings.CopyFrom(
                    nidx_pb2.LpgEmbeddings(
                        vectors=[nidx_pb2.LpgVector(values=vector) for vector in vectors[name]]
                    )
                )
            patches[name] = patch
        for name, property_vectors in vectors.items():
            if name not in patches:
                patches[name] = nidx_pb2.LpgTextPropertyPatch(
                    embeddings=nidx_pb2.LpgEmbeddings(
                        vectors=[nidx_pb2.LpgVector(values=vector) for vector in property_vectors]
                    )
                )
        result = self._operation(
            nidx_pb2.LpgOperation(
                patch_text_properties=nidx_pb2.LpgTextPropertyUpdate(
                    key=self._key(kind, id),
                    properties=patches,
                    vectorset=self.vectorset,
                )
            ),
        )
        if not result.HasField("record"):
            raise RuntimeError("NidxLPG returned no record after a text-property patch")
        record = _from_lpg_record(result.record, self.tenant)
        self._wait(lambda: self.get(kind, id) == record, f"text properties for {kind}/{id}")
        return record

    def delete_edge(self, id: str) -> None:
        """Delete an edge by ID; deleting a missing edge is a no-op."""
        self._delete("edge", id)

    def delete_node(self, id: str) -> None:
        """Delete a node by ID after all incoming and outgoing edges are removed.

        Deleting a missing node is a no-op; deletion fails if edges remain.
        """
        record = self.get("node", id)
        if record is None:
            return
        incident_edges = self._find("edge", incident_node=id)
        if incident_edges:
            raise ValueError("Node has incident edges; delete them first")
        self._operation(nidx_pb2.LpgOperation(delete_node=nidx_pb2.LpgNodeDeletion(id=id)))
        self._wait(lambda: self.get("node", id) is None, f"deletion node/{id}")

    def _delete(self, kind: Kind, id: str) -> None:
        self._operation(nidx_pb2.LpgOperation(delete_record=self._key(kind, id)))
        self._wait(lambda: self.get(kind, id) is None, f"deletion {kind}/{id}")

    @staticmethod
    def _wait(check: Callable[[], bool], description: str, timeout: float = 15) -> None:
        deadline = time.monotonic() + timeout
        while True:
            if check():
                return
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for NidxLPG visibility: {description}")
            time.sleep(0.05)
