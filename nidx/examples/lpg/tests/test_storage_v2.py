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
import os
import subprocess
import sys
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from uuid import uuid4

import pytest
from nidx_protos import nidx_pb2, noderesources_pb2
from nidx_protos import nodereader_pb2 as reader

from nidx.examples.lpg.storage import (
    DEFAULT_VECTORSET,
    RPC_TIMEOUT,
    EmbeddedNidx,
    Graph,
    Graph,
    Kind,
    PropertyFilter,
    Record,
    Scalar,
)


class MockNidxLpgClient:
    """Mock the new RPC boundary while persisting through the existing embedded spike."""

    def __init__(self, nidx: EmbeddedNidx) -> None:
        self.nidx = nidx
        self.records: dict[tuple[str, int, str], nidx_pb2.LpgRecord] = {}

    def LPGOperation(
        self, request: nidx_pb2.LpgOperationRequest, timeout: float | None = None
    ) -> nidx_pb2.LpgOperationResponse:
        del timeout
        results = []
        failed = False
        for operation in request.operations:
            if failed:
                results.append(nidx_pb2.LpgOperationResult(status=nidx_pb2.LPG_OPERATION_STATUS_SKIPPED))
                continue
            kind = operation.WhichOneof("operation")
            try:
                if kind == "upsert_record":
                    self.UpsertRecord(
                        nidx_pb2.UpsertRecordRequest(
                            graph_id=request.graph_id,
                            record=operation.upsert_record,
                        )
                    )
                    result = nidx_pb2.LpgOperationResult(status=nidx_pb2.LPG_OPERATION_STATUS_APPLIED)
                elif kind == "delete_record":
                    self.DeleteRecord(
                        nidx_pb2.DeleteRecordRequest(
                            graph_id=request.graph_id,
                            key=operation.delete_record,
                        )
                    )
                    result = nidx_pb2.LpgOperationResult(status=nidx_pb2.LPG_OPERATION_STATUS_APPLIED)
                elif kind == "delete_node":
                    self.DeleteNode(
                        nidx_pb2.DeleteNodeRequest(
                            graph_id=request.graph_id,
                            id=operation.delete_node.id,
                            detach=operation.delete_node.detach,
                        )
                    )
                    result = nidx_pb2.LpgOperationResult(status=nidx_pb2.LPG_OPERATION_STATUS_APPLIED)
                elif kind == "patch_text_properties":
                    patched = self.PatchTextProperties(
                        nidx_pb2.PatchTextPropertiesRequest(
                            graph_id=request.graph_id,
                            key=operation.patch_text_properties.key,
                            properties=operation.patch_text_properties.properties,
                            vectorset=operation.patch_text_properties.vectorset,
                        )
                    )
                    result = nidx_pb2.LpgOperationResult(
                        status=nidx_pb2.LPG_OPERATION_STATUS_APPLIED,
                        record=patched.record,
                    )
                else:
                    raise ValueError("Operation type required")
            except (ValueError, RuntimeError) as error:
                failed = True
                result = nidx_pb2.LpgOperationResult(
                    status=nidx_pb2.LPG_OPERATION_STATUS_FAILED,
                    error_code="InvalidArgument" if isinstance(error, ValueError) else "Internal",
                    error_message=str(error),
                )
            results.append(result)
        return nidx_pb2.LpgOperationResponse(results=results)

    def _graph(self, graph_id: str, *, max_records: int = 1000) -> Graph:
        shard = self.nidx.api.GetShard(
            reader.GetShardRequest(shard_id=noderesources_pb2.ShardId(id=graph_id)),
            timeout=RPC_TIMEOUT,
        )
        return Graph(
            self.nidx,
            shard.metadata.kbid,
            graph_id,
            vectorset=DEFAULT_VECTORSET,
            vector_dimension=3,
            max_records=max_records,
        )

    @staticmethod
    def _kind(kind: int) -> Kind:
        if kind == nidx_pb2.LPG_NODE:
            return "node"
        if kind == nidx_pb2.LPG_EDGE:
            return "edge"
        raise ValueError(f"Unknown record kind: {kind}")

    @staticmethod
    def _properties(record: nidx_pb2.LpgRecord) -> dict[str, Scalar]:
        values: dict[str, Scalar] = {}
        for key, value in record.properties.items():
            kind = value.WhichOneof("value")
            if kind == "text":
                values[key] = value.text
            elif kind == "integer":
                values[key] = value.integer
            elif kind == "float":
                values[key] = value.float
            elif kind == "boolean":
                values[key] = value.boolean
            elif kind == "null_value" and value.null_value:
                values[key] = None
            else:
                raise ValueError(f"Unsupported mock property type: {kind}")
        return values

    def _record(self, message: nidx_pb2.LpgRecord, tenant: str) -> Record:
        if not message.HasField("key"):
            raise ValueError("Record key required")
        kind = self._kind(message.key.kind)
        return Record(
            tenant=tenant,
            kind=kind,
            id=message.key.id,
            properties=self._properties(message),
            labels=list(message.labels),
            source=message.source if message.HasField("source") else None,
            target=message.target if message.HasField("target") else None,
            edge_type=message.edge_type if message.HasField("edge_type") else None,
            deleting=message.deleting,
            revision=message.revision,
            text_chunks={name: list(value.chunks) for name, value in message.text_properties.items()},
        )

    @staticmethod
    def _property(value: object) -> nidx_pb2.LpgProperty:
        result = nidx_pb2.LpgProperty()
        if value is None:
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
            raise ValueError("Unsupported mock property value")
        return result

    def _message(
        self,
        record: Record,
        *,
        existing: nidx_pb2.LpgRecord | None = None,
    ) -> nidx_pb2.LpgRecord:
        result = nidx_pb2.LpgRecord(
            key=nidx_pb2.LpgRecordKey(
                kind=nidx_pb2.LPG_NODE if record.kind == "node" else nidx_pb2.LPG_EDGE,
                id=record.id,
            ),
            labels=record.labels,
            properties={key: self._property(value) for key, value in record.properties.items()},
            vectorset=DEFAULT_VECTORSET,
            revision=record.revision,
            deleting=record.deleting,
        )
        if record.source is not None:
            result.source = record.source
            result.target = record.target or ""
            result.edge_type = record.edge_type or ""
        for name, chunks in record.text_chunks.items():
            prop = nidx_pb2.LpgTextProperty(chunks=chunks)
            if existing is not None and name in existing.text_properties:
                previous = existing.text_properties[name]
                if list(previous.chunks) == chunks:
                    prop.embeddings.extend(previous.embeddings)
            result.text_properties[name].CopyFrom(prop)
        return result

    @staticmethod
    def _copy(record: nidx_pb2.LpgRecord) -> nidx_pb2.LpgRecord:
        result = nidx_pb2.LpgRecord()
        result.CopyFrom(record)
        return result

    def _get(
        self,
        graph_id: str,
        kind: Kind,
        record_id: str,
        graph: Graph,
    ) -> nidx_pb2.LpgRecord | None:
        record = graph.get(kind, record_id)
        if record is None:
            return None
        key = (graph_id, nidx_pb2.LPG_NODE if kind == "node" else nidx_pb2.LPG_EDGE, record_id)
        stored = self.records.get(key)
        if stored is None:
            stored = self._message(record)
            self.records[key] = stored
        return self._copy(stored)

    @staticmethod
    def _filters(filters: Sequence[nidx_pb2.LpgPropertyFilter]) -> list[PropertyFilter]:
        predicates = []
        for value in filters:
            kind = value.WhichOneof("predicate")
            if kind in ("text", "integer", "float", "boolean"):
                predicates.append(PropertyFilter(value.property, "eq", value=getattr(value, kind)))
            elif kind in ("integer_range", "float_range"):
                bounds = getattr(value, kind)
                lower = bounds.lower if bounds.HasField("lower") else None
                upper = bounds.upper if bounds.HasField("upper") else None
                predicates.append(PropertyFilter(value.property, "range", lower=lower, upper=upper))
            else:
                raise ValueError(f"Unsupported mock filter: {kind}")
        return predicates

    def UpsertRecord(
        self, request: nidx_pb2.UpsertRecordRequest, timeout: float | None = None
    ) -> noderesources_pb2.EmptyQuery:
        del timeout
        if not request.HasField("record"):
            raise ValueError("Record required")
        graph = self._graph(request.graph_id)
        proto = request.record
        record = self._record(proto, graph.tenant)
        embeddings = {
            name: [list(vector.values) for vector in prop.embeddings]
            for name, prop in proto.text_properties.items()
            if prop.embeddings
        }
        if record.kind == "node":
            current = graph.get("node", record.id)
            if current is not None and current.deleting and not record.deleting:
                raise ValueError("Finish the pending node deletion before replacing it")
        else:
            for endpoint in (record.source, record.target):
                node = graph.get("node", endpoint or "")
                if node is None or node.deleting:
                    raise ValueError(f"Endpoint {endpoint!r} is missing or being deleted")
        graph._put(record, embeddings, delete_vectors=not record.deleting)
        key = (request.graph_id, proto.key.kind, proto.key.id)
        self.records[key] = self._copy(proto)
        return noderesources_pb2.EmptyQuery()

    def GetRecords(
        self, request: nidx_pb2.GetRecordsRequest, timeout: float | None = None
    ) -> nidx_pb2.GetRecordsResponse:
        del timeout
        graph = self._graph(request.graph_id)
        records = []
        for key in request.keys:
            record = self._get(request.graph_id, self._kind(key.kind), key.id, graph)
            if record is not None:
                records.append(record)
        return nidx_pb2.GetRecordsResponse(records=records)

    def DeleteRecord(
        self, request: nidx_pb2.DeleteRecordRequest, timeout: float | None = None
    ) -> noderesources_pb2.EmptyQuery:
        del timeout
        if not request.HasField("key"):
            raise ValueError("Record key required")
        graph = self._graph(request.graph_id)
        kind = self._kind(request.key.kind)
        graph._delete(kind, request.key.id)
        self.records.pop((request.graph_id, request.key.kind, request.key.id), None)
        return noderesources_pb2.EmptyQuery()

    def FindRecords(
        self, request: nidx_pb2.FindRecordsRequest, timeout: float | None = None
    ) -> nidx_pb2.FindRecordsResponse:
        del timeout
        graph = self._graph(request.graph_id)
        kind = self._kind(request.kind)
        records = graph.find(kind, *self._filters(request.filters))
        if request.HasField("label"):
            records = [record for record in records if request.label in record.labels]
        if request.HasField("source"):
            records = [record for record in records if record.source == request.source]
        if request.HasField("target"):
            records = [record for record in records if record.target == request.target]
        if request.HasField("edge_type"):
            records = [record for record in records if record.edge_type == request.edge_type]
        if request.HasField("incident_node"):
            records = [
                record
                for record in records
                if request.incident_node in (record.source, record.target)
            ]
        offset = int(request.page_token) if request.page_token else 0
        end = offset + request.page_size
        page = records[offset:end]
        next_token = str(end) if end < len(records) else ""
        messages = [
            self._get(request.graph_id, record.kind, record.id, graph)
            for record in page
        ]
        return nidx_pb2.FindRecordsResponse(
            records=[record for record in messages if record is not None],
            next_page_token=next_token,
        )

    def PatchTextProperties(
        self, request: nidx_pb2.PatchTextPropertiesRequest, timeout: float | None = None
    ) -> nidx_pb2.PatchTextPropertiesResponse:
        del timeout
        if not request.HasField("key"):
            raise ValueError("Record key required")
        graph = self._graph(request.graph_id)
        kind = self._kind(request.key.kind)
        existing = self._get(request.graph_id, kind, request.key.id, graph)
        if existing is None:
            raise ValueError("Record not found")
        chunks: dict[str, list[str]] = {}
        vectors: dict[str, list[list[float]]] = {}
        for name, patch in request.properties.items():
            if patch.HasField("chunks"):
                chunks[name] = list(patch.chunks.chunks)
            if patch.HasField("embeddings"):
                vectors[name] = [list(vector.values) for vector in patch.embeddings.vectors]
        updated = graph.update_text_properties(
            kind,
            request.key.id,
            text_chunks=chunks or None,
            embeddings=vectors or None,
        )
        changed = self._copy(existing)
        changed.revision = updated.revision
        for name, patch in request.properties.items():
            if patch.HasField("chunks") and not patch.chunks.chunks:
                del changed.text_properties[name]
                continue
            prop = changed.text_properties[name]
            if patch.HasField("chunks"):
                prop.chunks[:] = patch.chunks.chunks
                del prop.embeddings[:]
            if patch.HasField("embeddings"):
                del prop.embeddings[:]
                prop.embeddings.extend(patch.embeddings.vectors)
        self.records[(request.graph_id, request.key.kind, request.key.id)] = changed
        return nidx_pb2.PatchTextPropertiesResponse(record=changed)

    def SearchChunks(
        self, request: nidx_pb2.SearchChunksRequest, timeout: float | None = None
    ) -> nidx_pb2.SearchChunksResponse:
        del timeout
        limit = request.candidate_limit or 1000
        graph = self._graph(request.graph_id, max_records=limit)
        kind = self._kind(request.kind) if request.HasField("kind") else None
        hits = graph.search(
            text=request.text if request.HasField("text") else None,
            vector=list(request.vector) if request.vector else None,
            kind=kind,
            text_property=request.property if request.HasField("property") else None,
            filters=self._filters(request.filters),
            top_k=request.top_k,
            rrf_k=request.rrf_k or 60,
        )
        messages = []
        for hit in hits:
            record = self._get(request.graph_id, hit.record.kind, hit.record.id, graph)
            if record is None:
                raise ValueError("Mock search record disappeared")
            message = nidx_pb2.LpgChunkHit(
                record=record,
                property=hit.property,
                chunk_index=hit.chunk_index,
                text=hit.text,
                score=hit.score,
            )
            if hit.text_rank is not None:
                message.text_rank = hit.text_rank
            if hit.vector_rank is not None:
                message.vector_rank = hit.vector_rank
            messages.append(message)
        return nidx_pb2.SearchChunksResponse(hits=messages)

    def DeleteNode(
        self, request: nidx_pb2.DeleteNodeRequest, timeout: float | None = None
    ) -> noderesources_pb2.EmptyQuery:
        del timeout
        graph = self._graph(request.graph_id)
        graph.delete_node(request.id, detach=request.detach)
        self.records.pop((request.graph_id, nidx_pb2.LPG_NODE, request.id), None)
        if request.detach:
            for key, record in list(self.records.items()):
                if key[0] == request.graph_id and key[1] == nidx_pb2.LPG_EDGE:
                    if request.id in (record.source, record.target):
                        self.records.pop(key, None)
        return noderesources_pb2.EmptyQuery()


@pytest.fixture
def nidx(database_url: str, tmp_path: Path) -> Iterator[EmbeddedNidx]:
    instance = EmbeddedNidx(database_url, tmp_path)
    instance.lpg_client = MockNidxLpgClient(instance)
    try:
        yield instance
    finally:
        instance.close()


def ids(records: list[Record]) -> list[str]:
    return [record.id for record in records]


def test_records_filters_and_reopen(nidx: EmbeddedNidx) -> None:
    graph = Graph.create(nidx, str(uuid4()))
    product = graph.upsert_node(
        "P1",
        ["Product", "Sellable"],
        {
            "name": "Red apple",
            "price": 150,
            "rating": 4.5,
            "available": True,
            "supplier.name/\u5730\u533a": "Espa\u00f1a",
            "unset": None,
        },
    )
    graph.upsert_node("W1", ["Warehouse"], {"country": "Spain"})
    assert graph.get("node", "missing") is None
    assert graph.get("node", "P1") == product
    assert graph.get("edge", "P1") is None
    assert ids(graph.find("node")) == ["P1", "W1"]
    assert ids(graph.find("node", PropertyFilter("price", "eq", value=150))) == ["P1"]
    assert ids(graph.find("node", PropertyFilter("price", "range", lower=100, upper=150))) == ["P1"]
    assert ids(graph.find("node", PropertyFilter("price", "range", lower=151))) == []
    assert ids(graph.find("node", PropertyFilter("rating", "range", upper=4.5))) == ["P1"]
    assert ids(graph.find("node", PropertyFilter("available", "eq", value=True))) == ["P1"]
    assert ids(graph.find("node", PropertyFilter("available", "eq", value=False))) == []
    assert ids(
        graph.find(
            "node",
            PropertyFilter("available", "eq", value=True),
            PropertyFilter("price", "range", lower=100),
        )
    ) == ["P1"]
    assert ids(graph.find("node", PropertyFilter("name", "eq", value="Red apple"))) == ["P1"]
    assert ids(graph.find("node", PropertyFilter("name", "eq", value="Red"))) == []
    assert ids(
        graph.find("node", PropertyFilter("supplier.name/\u5730\u533a", "eq", value="Espa\u00f1a"))
    ) == ["P1"]
    assert graph.topology() == []  # Isolated nodes need no artificial self-edge.

    reopened = Graph(nidx, graph.tenant, graph.shard_id)
    assert reopened.get("node", "P1") == product
    assert ids(reopened.find("node")) == ["P1", "W1"]
    assert not list(nidx.inbox.iterdir())


def test_parallel_edges_typed_graph_prefilter_and_replacement(nidx: EmbeddedNidx) -> None:
    graph = Graph.create(nidx, str(uuid4()))
    graph.upsert_node("P1", ["Product"], {"category": "electronics"})
    graph.upsert_node("W1", ["Warehouse"], {"country": "Spain"})
    graph.upsert_node("W2", ["Warehouse"], {"country": "France"})
    first = graph.upsert_edge("E1", "P1", "STOCKED_AT", "W1", {"quantity": 10, "old": True})
    second = graph.upsert_edge("E2", "P1", "STOCKED_AT", "W1", {"quantity": 25})
    assert graph.get("edge", "E1") == first
    assert graph.topology() == [first, second]
    assert graph.topology(PropertyFilter("quantity", "range", lower=20)) == [second]
    assert graph.topology(PropertyFilter("country", "eq", value="Spain")) == []

    changed = graph.upsert_edge("E1", "P1", "STOCKED_AT", "W2", {"quantity": 5})
    assert graph.get("edge", "E1") == changed
    assert graph.topology() == [changed, second]
    assert ids(graph.topology(PropertyFilter("old", "eq", value=True))) == []
    assert ids(graph.find("edge", PropertyFilter("quantity", "eq", value=10))) == []
    with pytest.raises(ValueError, match="Edge type is immutable"):
        graph.upsert_edge("E1", "P1", "SUPPLIES", "W2", {"quantity": 5})
    graph.delete_edge("E1")
    assert graph.get("edge", "E1") is None
    assert graph.find("edge") == [second]
    assert graph.topology() == [second]
    graph.delete_edge("E1")  # Repeated deletion is safe.


def test_text_vector_and_hybrid_chunk_search(nidx: EmbeddedNidx) -> None:
    graph = Graph.create(nidx, str(uuid4()))
    product = graph.upsert_node(
        "P1",
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
    vegetable = graph.upsert_node(
        "P2",
        ["Product"],
        {"category": "vegetable"},
        text_chunks={"description": ["red pepper from France"]},
        embeddings={"description": [[1.0, 0.0, 0.0]]},
    )
    edge = graph.upsert_edge(
        "E1",
        "P1",
        "STOCKED_AT",
        "P2",
        {"quantity": 5},
        text_chunks={
            "note": ["inspected at warehouse", "cold storage"],
            "inspection": ["sealed package"],
        },
        embeddings={
            "note": [[0.0, 0.0, 1.0], [0.0, 0.8, 0.2]],
            "inspection": [[1.0, 0.0, 0.0]],
        },
    )

    general_property_hits = graph.search(text="crisp sweet", kind="node")
    assert [(hit.property, hit.text) for hit in general_property_hits] == [
        ("features", "crisp sweet fruit")
    ]
    specific_property_hits = graph.search(
        text="crisp sweet", kind="node", text_property="features"
    )
    assert [(hit.property, hit.text) for hit in specific_property_hits] == [
        ("features", "crisp sweet fruit")
    ]
    general_property_vector_hits = graph.search(vector=[0.0, 0.0, 1.0])
    assert {hit.property for hit in general_property_vector_hits} == {
        "description",
        "features",
        "inspection",
        "note",
    }
    specific_property_vector_hits = graph.search(
        vector=[0.0, 0.0, 1.0], kind="node", text_property="features"
    )
    assert specific_property_vector_hits[0].property == "features"

    text_hits = graph.search(
        text="red apple",
        kind="node",
        text_property="description",
        filters=[PropertyFilter("category", "eq", value="fruit")],
    )
    assert len(text_hits) == 1
    assert (text_hits[0].record, text_hits[0].property, text_hits[0].chunk_index) == (
        product,
        "description",
        0,
    )
    assert text_hits[0].text == "red apple from Spain"
    assert text_hits[0].text_rank == 1
    assert text_hits[0].vector_rank is None

    vector_hits = graph.search(
        vector=[0.0, 1.0, 0.0],
        kind="node",
        text_property="description",
        filters=[PropertyFilter("category", "eq", value="fruit")],
    )
    assert vector_hits[0].record == product
    assert vector_hits[0].chunk_index == 1
    assert vector_hits[0].text == "quiet green orchard"
    assert vector_hits[0].text_rank is None
    assert vector_hits[0].vector_rank == 1

    hybrid_hits = graph.search(
        text="red apple",
        vector=[0.0, 1.0, 0.0],
        kind="node",
        text_property="description",
        filters=[PropertyFilter("category", "eq", value="fruit")],
    )
    assert {(hit.record.id, hit.chunk_index) for hit in hybrid_hits} == {("P1", 0), ("P1", 1)}
    by_chunk = {hit.chunk_index: hit for hit in hybrid_hits}
    assert by_chunk[0].text_rank == 1 and by_chunk[0].vector_rank is not None
    assert by_chunk[1].text_rank is None and by_chunk[1].vector_rank == 1
    assert by_chunk[0].score == pytest.approx(
        1 / (60 + by_chunk[0].text_rank) + 1 / (60 + by_chunk[0].vector_rank)
    )
    assert by_chunk[1].score == pytest.approx(1 / (60 + by_chunk[1].vector_rank))

    edge_hits = graph.search(vector=[0.0, 0.0, 1.0], kind="edge", text_property="note", top_k=5)
    assert edge_hits[0].record == edge
    assert edge_hits[0].property == "note"
    assert edge_hits[0].chunk_index == 0
    all_edge_property_hits = graph.search(text="sealed package", kind="edge")
    assert [(hit.property, hit.text) for hit in all_edge_property_hits] == [
        ("inspection", "sealed package")
    ]
    specific_edge_property_hits = graph.search(
        vector=[1.0, 0.0, 0.0], kind="edge", text_property="inspection"
    )
    assert specific_edge_property_hits[0].property == "inspection"

    other_tenant = Graph.create(nidx, str(uuid4()))
    other = other_tenant.upsert_node(
        "P1",
        ["Product"],
        {"category": "fruit"},
        text_chunks={"description": ["red apple from Spain"]},
        embeddings={"description": [[1.0, 0.0, 0.0]]},
    )
    isolated = other_tenant.search(vector=[1.0, 0.0, 0.0], kind="node", text_property="description")
    assert [hit.record for hit in isolated] == [other]
    main_tenant_hits = graph.search(vector=[1.0, 0.0, 0.0], kind="node", text_property="description")
    assert main_tenant_hits
    assert {hit.record.tenant for hit in main_tenant_hits} == {graph.tenant}

    changed = graph.upsert_node(
        "P1",
        ["Product"],
        {"category": "fruit"},
        text_chunks={"description": ["mellow blue berry"]},
        embeddings={"description": [[0.0, 1.0, 0.0]]},
    )
    assert graph.get("node", "P1") == changed
    replaced_text_hits = graph.search(text="red apple", kind="node", text_property="description")
    assert [hit.record.id for hit in replaced_text_hits] == ["P2"]
    assert replaced_text_hits[0].text == "red pepper from France"
    old_vector_results = graph.search(vector=[1.0, 0.0, 0.0], kind="node", text_property="description")
    assert old_vector_results[0].record == vegetable


def test_text_chunks_and_embeddings_can_be_added_independently(nidx: EmbeddedNidx) -> None:
    graph = Graph.create(nidx, str(uuid4()))
    with pytest.raises(ValueError, match="missing node"):
        graph.update_text_properties("node", "missing", text_chunks={"description": ["text"]})
    node = graph.upsert_node("P1", ["Product"], {"category": "fruit"})
    graph.upsert_node("W1", ["Warehouse"], {"country": "Spain"})
    edge = graph.upsert_edge("E1", "P1", "STOCKED_AT", "W1", {"quantity": 5})

    node = graph.update_text_properties(
        "node",
        "P1",
        text_chunks={
            "description": ["red apple from Spain"],
            "features": ["crisp sweet fruit"],
        },
    )
    edge = graph.update_text_properties(
        "edge",
        "E1",
        text_chunks={
            "note": ["inspected at warehouse"],
            "inspection": ["sealed package"],
        },
    )
    assert node.properties == {"category": "fruit"}
    assert edge.properties == {"quantity": 5}
    assert graph.search(text="crisp sweet", kind="node", text_property="features")[0].record == node
    assert graph.search(text="sealed package", kind="edge", text_property="inspection")[0].record == edge
    assert graph.search(vector=[1.0, 0.0, 0.0], kind="node", text_property="description") == []
    with pytest.raises(ValueError, match="non-text property"):
        graph.update_text_properties("node", "P1", embeddings={"unknown": [[1.0, 0.0, 0.0]]})

    node = graph.update_text_properties(
        "node",
        "P1",
        embeddings={
            "description": [[1.0, 0.0, 0.0]],
            "features": [[0.0, 1.0, 0.0]],
        },
    )
    edge = graph.update_text_properties(
        "edge",
        "E1",
        embeddings={
            "note": [[0.0, 0.0, 1.0]],
            "inspection": [[1.0, 0.0, 0.0]],
        },
    )
    assert graph.search(vector=[1.0, 0.0, 0.0], kind="node", text_property="description")[0].record == node
    assert graph.search(vector=[1.0, 0.0, 0.0], kind="edge", text_property="inspection")[0].record == edge

    graph.update_text_properties("node", "P1", text_chunks={"tagline": ["limited edition"]})
    assert graph.search(vector=[1.0, 0.0, 0.0], kind="node", text_property="description")
    assert graph.search(vector=[0.0, 1.0, 0.0], kind="node", text_property="features")
    assert graph.search(text="limited edition", kind="node", text_property="tagline")

    graph.update_text_properties("node", "P1", text_chunks={"description": ["blue berry"]})
    assert graph.search(vector=[1.0, 0.0, 0.0], kind="node", text_property="description") == []
    assert graph.search(vector=[0.0, 1.0, 0.0], kind="node", text_property="features")


@pytest.mark.parametrize(
    "chunks,vectors",
    [
        ({}, {"description": [[1.0, 0.0, 0.0]]}),
        ({"description": ["one", "two"]}, {"description": [[1.0, 0.0, 0.0]]}),
        ({"description": ["one"]}, {"missing": [[1.0, 0.0, 0.0]]}),
        ({"description": ["one"]}, {"description": [[1.0, 0.0]]}),
        ({"description": ["one"]}, {"description": [[0.0, 0.0, 0.0]]}),
        ({"description": ["one"]}, {"description": [[float("inf"), 0.0, 0.0]]}),
    ],
)
def test_invalid_text_chunk_embeddings(
    nidx: EmbeddedNidx,
    chunks: dict[str, list[str]],
    vectors: Mapping[str, Sequence[Sequence[float]]],
) -> None:
    graph = Graph.create(nidx, str(uuid4()))
    with pytest.raises(ValueError):
        graph.upsert_node("P1", ["Product"], {}, text_chunks=chunks, embeddings=vectors)


def test_invalid_hybrid_search_queries(nidx: EmbeddedNidx) -> None:
    graph = Graph.create(nidx, str(uuid4()))
    with pytest.raises(ValueError, match="Provide"):
        graph.search()
    with pytest.raises(ValueError, match="exactly 3"):
        graph.search(vector=[1.0, 0.0])
    with pytest.raises(ValueError, match="Zero vectors"):
        graph.search(vector=[0.0, 0.0, 0.0])
    with pytest.raises(ValueError, match="top_k"):
        graph.search(text="query", top_k=0)


def test_node_replacement_and_tenant_isolation(nidx: EmbeddedNidx) -> None:
    first = Graph.create(nidx, str(uuid4()))
    second = Graph.create(nidx, str(uuid4()))
    first.upsert_node("P1", ["Product"], {"price": 150, "old": "remove me"})
    other = second.upsert_node("P1", ["Client"], {"price": 999})
    updated = first.upsert_node("P1", ["Product"], {"price": 80})
    assert first.find("node") == [updated]
    assert first.find("node", PropertyFilter("old", "eq", value="remove me")) == []
    assert first.find("node", PropertyFilter("price", "eq", value=150)) == []
    assert second.find("node") == [other]
    with pytest.raises(ValueError, match="Node labels/types are immutable"):
        first.upsert_node("P1", ["Discounted"], {"price": 80})
    with pytest.raises(ValueError, match="does not belong"):
        Graph(nidx, first.tenant, second.shard_id)
    first.upsert_node("W1", ["Warehouse"], {})
    second.upsert_node("W1", ["Warehouse"], {})
    first.upsert_edge("E1", "P1", "STOCKED_AT", "W1", {"quantity": 10})
    isolated_edge = second.upsert_edge("E1", "P1", "STOCKED_AT", "W1", {"quantity": 999})
    assert second.topology() == [isolated_edge]
    assert first.topology(PropertyFilter("quantity", "eq", value=999)) == []
    first.delete_node("P1", detach=True)
    assert ids(first.find("node")) == ["W1"]
    assert first.topology() == []
    assert second.get("node", "P1") == other
    assert second.topology() == [isolated_edge]


def test_cascade_and_resume_from_persisted_marker(nidx: EmbeddedNidx) -> None:
    graph = Graph.create(nidx, str(uuid4()))
    graph.upsert_node("P1", ["Product"], {})
    graph.upsert_node("W1", ["Warehouse"], {})
    graph.upsert_node("C1", ["Client"], {})
    graph.upsert_edge("E1", "P1", "STOCKED_AT", "W1", {})
    graph.upsert_edge("E2", "P1", "STOCKED_AT", "W1", {})
    graph.upsert_edge("E3", "C1", "BUYS", "P1", {})
    graph.upsert_edge("E4", "C1", "VISITS", "W1", {})
    graph.upsert_edge("E5", "P1", "RELATED_TO", "P1", {})
    with pytest.raises(ValueError, match="incident edges"):
        graph.delete_node("P1")
    before = graph.get("node", "P1")
    assert before is not None and before.deleting is False

    # Simulate interruption after the durable marker and one incident-edge deletion.
    graph._put(Record(graph.tenant, "node", "P1", {}, labels=["Product"], deleting=True))
    graph.delete_edge("E1")
    reopened = Graph(nidx, graph.tenant, graph.shard_id)
    with pytest.raises(ValueError, match="missing or being deleted"):
        reopened.upsert_edge("late", "P1", "STOCKED_AT", "W1", {})
    reopened.delete_node("P1", detach=True)
    assert reopened.get("node", "P1") is None
    assert ids(reopened.find("node")) == ["C1", "W1"]
    assert ids(reopened.find("edge")) == ["E4"]
    assert ids(reopened.topology()) == ["E4"]
    reopened.delete_node("P1", detach=True)


def test_invalid_endpoints_and_candidate_limit(nidx: EmbeddedNidx) -> None:
    graph = Graph.create(nidx, str(uuid4()))
    graph.upsert_node("P1", ["Product"], {})
    with pytest.raises(ValueError, match="missing"):
        graph.upsert_edge("E1", "P1", "STOCKED_AT", "missing", {})
    assert graph.find("edge") == []
    graph.upsert_node("W1", ["Warehouse"], {})
    graph.upsert_edge("E1", "P1", "STOCKED_AT", "W1", {})
    graph.upsert_edge("E2", "P1", "STOCKED_AT", "W1", {})
    bounded = Graph(nidx, graph.tenant, graph.shard_id, max_records=1)
    with pytest.raises(RuntimeError, match="limit exceeded"):
        bounded.find("node")
    with pytest.raises(RuntimeError, match="limit exceeded"):
        bounded.topology()
    with pytest.raises(RuntimeError, match="limit exceeded"):
        bounded.delete_node("P1", detach=True)
    unchanged = graph.get("node", "P1")
    assert unchanged is not None and unchanged.deleting is False


def test_binding_restart_and_demo(database_url: str, tmp_path: Path) -> None:
    original = EmbeddedNidx(database_url, tmp_path)
    try:
        original.lpg_client = MockNidxLpgClient(original)
        graph = Graph.create(original, str(uuid4()))
        product = graph.upsert_node("P1", ["Product"], {"price": 150})
        graph.upsert_node("W1", ["Warehouse"], {})
        edge = graph.upsert_edge("E1", "P1", "STOCKED_AT", "W1", {"quantity": 10})
        tenant, shard_id = graph.tenant, graph.shard_id
    finally:
        original.close()

    restarted = EmbeddedNidx(database_url, tmp_path)
    try:
        restarted.lpg_client = MockNidxLpgClient(restarted)
        reopened = Graph(restarted, tenant, shard_id)
        assert reopened.get("node", "P1") == product
        assert reopened.get("edge", "E1") == edge
        assert reopened.topology(PropertyFilter("quantity", "eq", value=10)) == [edge]
        reopened.delete_node("P1", detach=True)
        assert reopened.find("edge") == []
    finally:
        restarted.close()

    demo = subprocess.run(
        [sys.executable, "-m", "nidx.examples.lpg.demo", "--directory", str(tmp_path)],
        env={**os.environ, "LPG_DATABASE_URL": database_url},
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert demo.returncode == 0, demo.stdout + demo.stderr
    assert "Reopen, replacement, and cascading deletion verified." in demo.stdout


@pytest.mark.parametrize(
    "predicate",
    [
        PropertyFilter("x", "eq", value=None),
        PropertyFilter("x", "range"),
        PropertyFilter("x", "range", lower=True),
        PropertyFilter("x", "range", lower=1, upper=2.0),
        PropertyFilter("x", "range", lower=5, upper=2),
        PropertyFilter("x", "eq", value=float("nan")),
        PropertyFilter("x", "eq", value=2**63),
    ],
)
def test_invalid_property_filters(predicate: PropertyFilter) -> None:
    with pytest.raises(ValueError):
        predicate.expression()
