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

import json
import math
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
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
DEFAULT_VECTORSET = "lpg-property-v1"
DEFAULT_VECTOR_DIMENSION = 3


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
    text_chunks: dict[str, list[str]] = field(default_factory=dict)

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
        if not isinstance(self.text_chunks, dict):
            raise ValueError("Text chunks must be a property-to-chunks mapping")
        for key, chunks in self.text_chunks.items():
            _property_key(key)
            if not isinstance(chunks, list) or not chunks:
                raise ValueError("Text properties must contain a nonempty list of chunks")
            if any(not isinstance(chunk, str) or not chunk.strip() for chunk in chunks):
                raise ValueError("Text chunks must be nonempty strings")
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
class _ChunkRef:
    resource_id: str
    field: str
    start: int
    end: int

    @property
    def id(self) -> str:
        return f"{self.resource_id}/{self.field}/{self.start}-{self.end}"


def _chunk_id(resource_id: str, property_name: str, start: int, end: int) -> str:
    return f"{resource_id}/t/{_property_key(property_name)}/{start}-{end}"


def _parse_chunk_id(value: str) -> _ChunkRef:
    parts = value.split("/")
    if len(parts) != 4 or parts[1] != "t" or not parts[2].startswith("p_"):
        raise ValueError(f"Invalid LPG paragraph/vector ID: {value!r}")
    UUID(parts[0])
    try:
        bytes.fromhex(parts[2][2:])
        span = re.fullmatch(r"(\d+)-(\d+)", parts[3])
        if span is None:
            raise ValueError("Invalid span")
        start, end = (int(part) for part in span.groups())
    except ValueError as error:
        raise ValueError(f"Invalid LPG paragraph/vector ID: {value!r}") from error
    if start < 0 or end <= start:
        raise ValueError(f"Invalid LPG paragraph/vector span: {value!r}")
    return _ChunkRef(parts[0], f"t/{parts[2]}", start, end)


def _chunk_layout(record: Record) -> list[tuple[str, int, int, int]]:
    layout: list[tuple[str, int, int, int]] = []
    for property_name, chunks in record.text_chunks.items():
        offset = 0
        for index, chunk in enumerate(chunks):
            end = offset + len(chunk)
            layout.append((property_name, index, offset, end))
            offset = end + 1
    return layout


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
        self,
        nidx: EmbeddedNidx,
        tenant: str,
        shard_id: str,
        *,
        vectorset: str = DEFAULT_VECTORSET,
        vector_dimension: int = DEFAULT_VECTOR_DIMENSION,
        max_records: int = 1000,
    ) -> None:
        self.nidx = nidx
        self.tenant = str(UUID(tenant))
        self.shard_id = str(UUID(shard_id))
        _nonempty(vectorset)
        if not 1 <= vector_dimension <= 4096:
            raise ValueError("vector_dimension must be between 1 and 4096")
        self.vectorset = vectorset
        self.vector_dimension = vector_dimension
        if not 1 <= max_records <= 100_000:
            raise ValueError("max_records must be between 1 and 100000")
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

    @classmethod
    def create(
        cls,
        nidx: EmbeddedNidx,
        tenant: str,
        *,
        vectorset: str = DEFAULT_VECTORSET,
        vector_dimension: int = DEFAULT_VECTOR_DIMENSION,
    ) -> Graph:
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
        return cls(nidx, tenant, created.id, vectorset=vectorset, vector_dimension=vector_dimension)

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
        expression = self._filter(kind, filters)
        assert expression is not None
        return self._find(expression)

    @staticmethod
    def _filter(
        kind: Kind | None, filters: Sequence[PropertyFilter]
    ) -> reader.JsonFilterExpression | None:
        if kind is not None and kind not in ("node", "edge"):
            raise ValueError(f"Unknown record kind: {kind}")
        expressions = []
        if kind is not None:
            expressions.append(
                reader.JsonFilterExpression(
                    path=reader.JsonFieldPathFilter(field_id=ENVELOPE_FIELD, json_path="kind", text=kind)
                )
            )
        expressions.extend(predicate.expression() for predicate in filters)
        if not expressions:
            return None
        return reader.JsonFilterExpression(
            bool_and=reader.JsonFilterExpression.List(operands=expressions)
        )

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
    ) -> list[SearchHit]:
        """Search text chunks with BM25, cosine vectors, or reciprocal-rank fusion."""
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
        if text_property is not None:
            _property_key(text_property)
        if not 1 <= top_k <= self.max_records:
            raise ValueError(f"top_k must be between 1 and {self.max_records}")
        if rrf_k < 1:
            raise ValueError("rrf_k must be positive")

        request = reader.SearchRequest(
            shard_ids=[self.shard_id],
            body=text or "",
            paragraph=text is not None,
            vector=list(vector) if vector is not None else [],
            vectorset=self.vectorset if vector is not None else "",
            result_per_page=self.max_records + 1,
            with_duplicates=True,
        )
        if text_property is not None:
            request.field_filter.CopyFrom(
                reader.FilterExpression(
                    field=reader.FilterExpression.FieldFilter(
                        field_type="t", field_id=_property_key(text_property)
                    )
                )
            )
        json_filter = self._filter(kind, filters)
        if json_filter is not None:
            request.json_filter.CopyFrom(json_filter)
        response = self.nidx.searcher.Search(request, timeout=RPC_TIMEOUT)
        self._complete(response.shard_ids)

        ranks: dict[str, dict[str, int]] = {}
        if text is not None:
            paragraph_response = response.paragraph
            property_hits = [
                hit for hit in paragraph_response.results if hit.field.strip("/").startswith("t/p_")
            ]
            if paragraph_response.next_page or len(property_hits) > self.max_records:
                raise RuntimeError("Text search candidate limit exceeded; result would be incomplete")
            for rank, hit in enumerate(property_hits, start=1):
                field = hit.field.strip("/")
                ref = _parse_chunk_id(f"{hit.uuid}/{field}/{hit.start}-{hit.end}")
                ranks.setdefault(ref.id, {})["text"] = rank
        if vector is not None:
            for rank, vector_hit in enumerate(response.vector.documents, start=1):
                if vector_hit.doc_id is None:
                    raise RuntimeError("nidx returned a vector result without an ID")
                ref = _parse_chunk_id(vector_hit.doc_id.id)
                ranks.setdefault(ref.id, {})["vector"] = rank
            if len(response.vector.documents) > self.max_records:
                raise RuntimeError("Vector search candidate limit exceeded; result may be incomplete")
        if not ranks:
            return []

        request_texts = nidx_pb2.ExtractedTextsRequest(shard_id=self.shard_id)
        refs = {chunk_id: _parse_chunk_id(chunk_id) for chunk_id in ranks}
        resource_ids = sorted({ref.resource_id for ref in refs.values()})
        for rid in resource_ids:
            request_texts.field_ids.add(rid=rid, field_type="a", field_name="lpg")
        for ref in refs.values():
            field_type, field_name = ref.field.split("/", 1)
            request_texts.paragraph_ids.add(
                rid=ref.resource_id,
                field_type=field_type,
                field_name=field_name,
                paragraph_start=ref.start,
                paragraph_end=ref.end,
            )
        hydrated = self.nidx.searcher.ExtractedTexts(request_texts, timeout=RPC_TIMEOUT)

        records: dict[str, Record] = {}
        for rid in resource_ids:
            raw = hydrated.fields.get(f"{rid}/{ENVELOPE_FIELD}")
            if raw is None:
                raise RuntimeError("Search candidate disappeared during record hydration")
            record = Record(**json.loads(raw))
            if record.tenant != self.tenant or self.resource_id(record.kind, record.id) != rid:
                raise ValueError("Stored record identity does not match tenant/resource")
            if kind is not None and record.kind != kind:
                raise RuntimeError("Search returned a record outside the requested kind")
            records[rid] = record

        property_names: dict[tuple[str, str], str] = {}
        chunk_indices: dict[tuple[str, str, int, int], int] = {}
        for rid, record in records.items():
            for property_name, chunk_index, start, end in _chunk_layout(record):
                token = _property_key(property_name)
                property_names[(rid, token)] = property_name
                chunk_indices[(rid, f"t/{token}", start, end)] = chunk_index

        hits = []
        for chunk_id, modality_ranks in ranks.items():
            ref = refs[chunk_id]
            property_token = ref.field.split("/", 1)[1]
            matched_property = property_names.get((ref.resource_id, property_token))
            matched_chunk_index = chunk_indices.get((ref.resource_id, ref.field, ref.start, ref.end))
            if matched_property is None or matched_chunk_index is None:
                raise RuntimeError("Search result references a stale or unknown text chunk")
            if text_property is not None and matched_property != text_property:
                raise RuntimeError("Search returned a different text property than requested")
            chunk_text = hydrated.paragraphs.get(chunk_id)
            if chunk_text is None:
                raise RuntimeError("nidx could not hydrate the matched text chunk")
            score = sum(1 / (rrf_k + rank) for rank in modality_ranks.values())
            hits.append(
                SearchHit(
                    record=records[ref.resource_id],
                    property=matched_property,
                    chunk_index=matched_chunk_index,
                    text=chunk_text,
                    score=score,
                    text_rank=modality_ranks.get("text"),
                    vector_rank=modality_ranks.get("vector"),
                )
            )
        return sorted(
            hits,
            key=lambda hit: (
                -hit.score,
                hit.record.id,
                hit.property,
                hit.chunk_index,
            ),
        )[:top_k]

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
                continue
            seen.add(hit.uuid)
            record = self._get_resource(hit.uuid)
            if record is None:
                raise RuntimeError("Candidate disappeared during hydration; retry the query")
            records.append(record)
        return sorted(records, key=lambda record: record.id)

    def upsert_node(
        self,
        id: str,
        labels: list[str],
        properties: dict[str, Scalar],
        *,
        text_chunks: dict[str, list[str]] | None = None,
        embeddings: Mapping[str, Sequence[Sequence[float]]] | None = None,
    ) -> Record:
        current = self.get("node", id)
        if current is not None and current.deleting:
            raise ValueError("Finish the pending node deletion before replacing it")
        chunks = text_chunks or {}
        record = Record(self.tenant, "node", id, properties, labels=labels, text_chunks=chunks)
        vectors = _validate_embeddings(chunks, embeddings, self.vector_dimension)
        self._put(record, vectors)
        return record

    def upsert_edge(
        self,
        id: str,
        source: str,
        edge_type: str,
        target: str,
        properties: dict[str, Scalar],
        *,
        text_chunks: dict[str, list[str]] | None = None,
        embeddings: Mapping[str, Sequence[Sequence[float]]] | None = None,
    ) -> Record:
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
        vectors = _validate_embeddings(chunks, embeddings, self.vector_dimension)
        for endpoint in (source, target):
            node = self.get("node", endpoint)
            if node is None or node.deleting:
                raise ValueError(f"Endpoint {endpoint!r} is missing or being deleted")
        self._put(record, vectors)
        return record

    def update_text_properties(
        self,
        kind: Kind,
        id: str,
        *,
        text_chunks: Mapping[str, Sequence[str]] | None = None,
        embeddings: Mapping[str, Sequence[Sequence[float]]] | None = None,
    ) -> Record:
        """Add or replace text chunks and/or embeddings without replacing the LPG record."""
        if not text_chunks and not embeddings:
            raise ValueError("Provide text chunks, embeddings, or both")
        current = self.get(kind, id)
        if current is None:
            raise ValueError(f"Cannot update text properties for missing {kind} {id!r}")
        if current.deleting:
            raise ValueError("Finish the pending deletion before updating text properties")
        if text_chunks is not None and not isinstance(text_chunks, Mapping):
            raise ValueError("Text chunks must map property names to chunk sequences")

        updated_chunks = {
            property_name: list(chunks) for property_name, chunks in current.text_chunks.items()
        }
        for property_name, chunks in (text_chunks or {}).items():
            if not isinstance(chunks, Sequence) or isinstance(chunks, (str, bytes)):
                raise ValueError("Each text property must contain a sequence of chunks")
            updated_chunks[property_name] = list(chunks)

        updated_record = replace(current, text_chunks=updated_chunks, revision=uuid4().hex)
        vectors = _validate_embeddings(updated_chunks, embeddings, self.vector_dimension)
        changed_properties = set(text_chunks or {}) | set(vectors)
        rid = self.resource_id(kind, id)
        vector_delete_prefixes = [
            f"{rid}/t/{_property_key(property_name)}/" for property_name in sorted(changed_properties)
        ]
        self._put(
            updated_record,
            vectors,
            vector_delete_prefixes=vector_delete_prefixes,
        )
        return updated_record

    def _put(
        self,
        record: Record,
        embeddings: dict[str, list[list[float]]] | None = None,
        *,
        delete_vectors: bool = True,
        vector_delete_prefixes: Sequence[str] | None = None,
    ) -> None:
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
        )
        if delete_vectors:
            prefixes = [rid] if vector_delete_prefixes is None else vector_delete_prefixes
            resource.vector_prefixes_to_delete[self.vectorset].items.extend(prefixes)
        resource.metadata.created.GetCurrentTime()
        resource.metadata.modified.CopyFrom(resource.metadata.created)
        resource.paragraphs[ENVELOPE_FIELD].paragraphs[
            f"{rid}/{ENVELOPE_FIELD}/0-{len(envelope)}"
        ].CopyFrom(resources.IndexParagraph(start=0, end=len(envelope), field=ENVELOPE_FIELD))
        embeddings = embeddings or {}
        for property_name, chunks in record.text_chunks.items():
            field_id = f"t/{_property_key(property_name)}"
            resource.texts[field_id].text = "\n".join(chunks)
            offset = 0
            for index, chunk in enumerate(chunks):
                end = offset + len(chunk)
                paragraph_id = _chunk_id(rid, property_name, offset, end)
                paragraph = resources.IndexParagraph(start=offset, end=end, field=field_id, index=index)
                vectors = embeddings.get(property_name)
                if vectors is not None:
                    paragraph.vectorsets_sentences[self.vectorset].sentences[paragraph_id].vector.extend(
                        vectors[index]
                    )
                resource.paragraphs[field_id].paragraphs[paragraph_id].CopyFrom(paragraph)
                offset = end + 1
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
        self._wait(
            lambda: self._visible(record, embeddings),
            f"record {record.kind}/{record.id}",
        )

    def _visible(self, record: Record, embeddings: dict[str, list[list[float]]]) -> bool:
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
            if topology.graph:
                return False
        else:
            if len(topology.graph) != 1:
                return False
            assert record.source is not None and record.target is not None
            path = topology.graph[0]
            if not (
                path.resource_field_id == f"{rid}/{RELATION_FIELD}"
                and topology.nodes[path.source].value == self.resource_id("node", record.source)
                and topology.nodes[path.destination].value == self.resource_id("node", record.target)
                and topology.relations[path.relation].label == record.edge_type
            ):
                return False
        if record.text_chunks:
            request = nidx_pb2.ExtractedTextsRequest(shard_id=self.shard_id)
            expected_chunks: dict[str, str] = {}
            for property_name, chunk_index, start, end in _chunk_layout(record):
                chunk_id = _chunk_id(rid, property_name, start, end)
                expected_chunks[chunk_id] = record.text_chunks[property_name][chunk_index]
                request.paragraph_ids.add(
                    rid=rid,
                    field_type="t",
                    field_name=_property_key(property_name),
                    paragraph_start=start,
                    paragraph_end=end,
                )
            extracted = self.nidx.searcher.ExtractedTexts(request, timeout=RPC_TIMEOUT)
            if any(extracted.paragraphs.get(chunk_id) != chunk for chunk_id, chunk in expected_chunks.items()):
                return False
        for property_name, vectors in embeddings.items():
            chunks = [
                (start, end) for name, _, start, end in _chunk_layout(record) if name == property_name
            ]
            for vector, (start, end) in zip(vectors, chunks, strict=True):
                vector_ids = self._vector_search_ids(
                    vector,
                    text_property=property_name,
                    revision=record.revision,
                )
                if _chunk_id(rid, property_name, start, end) not in vector_ids:
                    return False
        return True

    def _vector_search_ids(
        self,
        vector: Sequence[float],
        *,
        text_property: str | None = None,
        filters: Sequence[PropertyFilter] = (),
        kind: Kind | None = None,
        revision: str | None = None,
    ) -> list[str]:
        request = reader.SearchRequest(
            shard_ids=[self.shard_id],
            vector=list(vector),
            vectorset=self.vectorset,
            result_per_page=self.max_records + 1,
            with_duplicates=True,
        )
        if text_property is not None:
            request.field_filter.CopyFrom(
                reader.FilterExpression(
                    field=reader.FilterExpression.FieldFilter(
                        field_type="t", field_id=_property_key(text_property)
                    )
                )
            )
        json_filter = self._filter(kind, filters)
        if revision is not None:
            revision_filter = reader.JsonFilterExpression(
                path=reader.JsonFieldPathFilter(
                    field_id=ENVELOPE_FIELD, json_path="revision", text=revision
                )
            )
            json_filter = (
                reader.JsonFilterExpression(
                    bool_and=reader.JsonFilterExpression.List(
                        operands=[json_filter, revision_filter]
                    )
                )
                if json_filter is not None
                else revision_filter
            )
        if json_filter is not None:
            request.json_filter.CopyFrom(json_filter)
        response = self.nidx.searcher.Search(request, timeout=RPC_TIMEOUT)
        self._complete(response.shard_ids)
        if len(response.vector.documents) > self.max_records:
            raise RuntimeError("Vector search candidate limit exceeded; result may be incomplete")
        return [
            document.doc_id.id for document in response.vector.documents if document.doc_id is not None
        ]

    def topology(self, *filters: PropertyFilter) -> list[Record]:
        """Return edge records via JSON-prefiltered topology, preserving parallel edges."""
        json_filter = self._filter("edge", filters)
        assert json_filter is not None
        response = self.nidx.searcher.Search(
            reader.SearchRequest(
                shard_ids=[self.shard_id],
                result_per_page=self.max_records + 1,
                json_filter=json_filter,
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
            Record(
                self.tenant,
                "node",
                id,
                record.properties,
                labels=record.labels,
                deleting=True,
                text_chunks=record.text_chunks,
            ),
            delete_vectors=False,
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
