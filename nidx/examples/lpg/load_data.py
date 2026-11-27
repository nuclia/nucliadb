# Copyright (C) 2021 Bosutech XXI S.L.
#
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Validate synthetic JSONL and load it into a remote Rust NidxLPG service.

{"graph_id": "97c826fb-bc40-4342-b4b7-6c2187dfb443", "created": true, "tenant": "45e479cc-92ba-4a38-b9c2-da5c015778b4"}

"""

import argparse
import hashlib
import json
import math
import os
import sqlite3
import sys
import time
from collections.abc import Iterator
from contextlib import closing, contextmanager, nullcontext
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, Lock, Thread
from time import perf_counter
from uuid import UUID, uuid4

import grpc
from nidx_protos import nidx_pb2, nidx_pb2_grpc, nodereader_pb2, noderesources_pb2, nodewriter_pb2

from nucliadb_protos import utils_pb2

MAX_REQUEST_BYTES = 3 * 1024 * 1024
PROGRESS_INTERVAL = 5.0


@dataclass
class Checkpoint:
    connection: sqlite3.Connection
    path: Path
    state: dict[str, object]

    def save(self, **updates: object) -> None:
        self.state.update(updates)
        self.connection.execute(
            "INSERT OR REPLACE INTO checkpoint (id, state) VALUES (1, ?)",
            (json.dumps(self.state),),
        )
        self.connection.commit()


@contextmanager
def open_checkpoint(path: Path, *, resume: bool) -> Iterator[Checkpoint]:
    if resume:
        if not path.is_file():
            raise ValueError(f"Checkpoint does not exist: {path}")
    else:
        with path.open("x"):
            pass
    with closing(sqlite3.connect(path, timeout=0)) as connection:
        connection.execute("PRAGMA locking_mode = EXCLUSIVE")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("BEGIN EXCLUSIVE")
        if resume:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS checkpoint (id INTEGER PRIMARY KEY, state TEXT NOT NULL)"
            )
            row = connection.execute("SELECT state FROM checkpoint WHERE id = 1").fetchone()
            state = json.loads(row[0]) if row is not None else {"version": 1}
            if not isinstance(state, dict) or state.get("version") != 1:
                raise ValueError("Unsupported or invalid checkpoint")
        else:
            connection.execute("CREATE TABLE checkpoint (id INTEGER PRIMARY KEY, state TEXT NOT NULL)")
            state = {"version": 1}
        checkpoint = Checkpoint(connection, path, state)
        checkpoint.save()
        yield checkpoint


class Progress:
    def __init__(self, phase: str, **values: str | int | float | bool) -> None:
        self.values = {"phase": phase, **values}
        self.started = perf_counter()
        self.lock = Lock()

    def update(self, **values: str | int | float | bool) -> None:
        with self.lock:
            self.values.update(values)

    def report(self, status: str = "running") -> None:
        with self.lock:
            values = self.values.copy()
        elapsed = perf_counter() - self.started
        values.update(status=status, elapsed_seconds=round(elapsed, 2))
        completed = values.get("acknowledged", values.get("records"))
        if isinstance(completed, int):
            resumed_from = values.get("resumed_from", 0)
            processed = completed - resumed_from if isinstance(resumed_from, int) else completed
            values["records_per_second"] = round(processed / elapsed, 2) if elapsed > 0 else 0
        submitted = values.get("submitted_records")
        if isinstance(submitted, int):
            values["submitted_records_per_second"] = round(submitted / elapsed, 2) if elapsed > 0 else 0
        total = values.get("total")
        acknowledged = values.get("acknowledged")
        if isinstance(total, int) and total > 0 and isinstance(acknowledged, int):
            values["percent"] = round(100 * acknowledged / total, 2)
        total_bytes, bytes_read = values.get("total_bytes"), values.get("bytes_read")
        if isinstance(total_bytes, int) and total_bytes > 0 and isinstance(bytes_read, int):
            values["percent"] = round(100 * bytes_read / total_bytes, 2)
        print(json.dumps(values), file=sys.stderr, flush=True)


@contextmanager
def progress(phase: str, **values: str | int | float | bool) -> Iterator[Progress]:
    state = Progress(phase, **values)
    stopped = Event()

    def heartbeat() -> None:
        while not stopped.wait(PROGRESS_INTERVAL):
            state.report()

    state.report()
    worker = Thread(target=heartbeat, daemon=True)
    worker.start()
    completed = False
    try:
        yield state
        completed = True
    finally:
        stopped.set()
        worker.join()
        state.report(status="complete" if completed else "failed")


def object_map(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("Expected a JSON object")
    return {key: item for key, item in value.items()}


def nonempty_string(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Expected a nonempty string")
    return value


def string_list(value: object) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError("Expected a list of strings")
    return [str(item) for item in value]


def property_value(value: object) -> nidx_pb2.LpgProperty:
    if value is None:
        return nidx_pb2.LpgProperty(null_value=True)
    if isinstance(value, bool):
        return nidx_pb2.LpgProperty(boolean=value)
    if isinstance(value, str):
        return nidx_pb2.LpgProperty(text=value)
    if isinstance(value, int) and -(2**63) <= value < 2**63:
        return nidx_pb2.LpgProperty(integer=value)
    if isinstance(value, float) and math.isfinite(value):
        return nidx_pb2.LpgProperty(float=value)
    if isinstance(value, list):
        return nidx_pb2.LpgProperty(strings=nidx_pb2.LpgStringList(values=string_list(value)))
    raise ValueError("Properties require string, bool, int64, finite float, null or string list")


def to_record(value: object) -> nidx_pb2.LpgRecord:
    row = object_map(value)
    kind = row.get("kind")
    fields = {"kind", "id", "properties", "text_chunks", "embeddings"}
    if kind == "node":
        fields.add("labels")
    elif kind == "edge":
        fields.update(("source", "target", "edge_type"))
    else:
        raise ValueError("kind must be 'node' or 'edge'")
    if extra := row.keys() - fields:
        raise ValueError(f"Unexpected fields: {sorted(extra)}")
    record = nidx_pb2.LpgRecord(
        key=nidx_pb2.LpgRecordKey(
            kind=nidx_pb2.LPG_NODE if kind == "node" else nidx_pb2.LPG_EDGE,
            id=nonempty_string(row.get("id")),
        ),
        properties={
            name: property_value(item) for name, item in object_map(row.get("properties")).items()
        },
        revision=uuid4().hex,
    )
    if kind == "node":
        labels = string_list(row.get("labels"))
        if not labels:
            raise ValueError("Nodes require at least one label/type")
        for label in labels:
            nonempty_string(label)
        if len(set(labels)) != len(labels):
            raise ValueError("Node labels/types must be unique")
        record.labels.extend(labels)
    else:
        record.source = nonempty_string(row.get("source"))
        record.target = nonempty_string(row.get("target"))
        record.edge_type = nonempty_string(row.get("edge_type"))
    for name, chunks in object_map(row.get("text_chunks", {})).items():
        texts = string_list(chunks)
        if not texts:
            raise ValueError(f"Text property {name!r} must contain chunks")
        for text in texts:
            nonempty_string(text)
        record.text_properties[name].chunks.extend(texts)
    for name, embeddings in object_map(row.get("embeddings", {})).items():
        if name not in record.text_properties:
            raise ValueError(f"Embeddings refer to missing text property {name!r}")
        if not isinstance(embeddings, list) or len(embeddings) != len(
            record.text_properties[name].chunks
        ):
            raise ValueError(f"Expected one embedding per chunk for {name!r}")
        for values in embeddings:
            if not isinstance(values, list) or not values:
                raise ValueError("Embeddings must be nonempty numeric lists")
            numbers = []
            for number in values:
                if (
                    isinstance(number, bool)
                    or not isinstance(number, (int, float))
                    or not -3.4028234663852886e38 <= number <= 3.4028234663852886e38
                ):
                    raise ValueError("Embedding values must be finite float32 numbers")
                numbers.append(float(number))
            vector = nidx_pb2.LpgVector(values=numbers)
            if not any(vector.values):
                raise ValueError("Embeddings must be nonzero after float32 conversion")
            record.text_properties[name].embeddings.append(vector)
    return record


@dataclass
class Dataset:
    connection: sqlite3.Connection
    node_count: int
    edge_count: int
    vector_dimension: int
    vectorset: str = ""
    digest: str = ""

    def decode(self, data: bytes) -> nidx_pb2.LpgRecord:
        record = nidx_pb2.LpgRecord.FromString(data)
        record.vectorset = self.vectorset
        return record

    def records(self, kind: int, skip: int = 0) -> Iterator[nidx_pb2.LpgRecord]:
        for (data,) in self.connection.execute(
            "SELECT data FROM records WHERE kind = ? ORDER BY position LIMIT -1 OFFSET ?", (kind, skip)
        ):
            yield self.decode(data)

    def node(self, node_id: str) -> nidx_pb2.LpgRecord:
        row = self.connection.execute(
            "SELECT data FROM records WHERE kind = ? AND id = ?", (nidx_pb2.LPG_NODE, node_id)
        ).fetchone()
        if row is None:
            raise ValueError(f"Node {node_id!r} is absent from the dataset")
        return self.decode(row[0])

    def node_labels(self, node_id: str) -> nidx_pb2.LpgStringList:
        row = self.connection.execute(
            "SELECT labels FROM records WHERE kind = ? AND id = ?", (nidx_pb2.LPG_NODE, node_id)
        ).fetchone()
        if row is None:
            raise ValueError(f"Node {node_id!r} is absent from the dataset")
        return nidx_pb2.LpgStringList.FromString(row[0])

    def mark_acknowledged(self, records: list[nidx_pb2.LpgRecord]) -> int:
        before = self.connection.total_changes
        self.connection.executemany(
            "INSERT OR IGNORE INTO acknowledged (kind, id) VALUES (?, ?)",
            ((record.key.kind, record.key.id) for record in records),
        )
        return self.connection.total_changes - before


def file_identity(path: Path) -> dict[str, str | int]:
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "device": stat.st_dev,
        "inode": stat.st_ino,
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "ctime_ns": stat.st_ctime_ns,
    }


@contextmanager
def dataset_store(checkpoint: Checkpoint | None) -> Iterator[sqlite3.Connection]:
    if checkpoint is not None:
        yield checkpoint.connection
    else:
        with TemporaryDirectory(prefix="nidx-lpg-") as directory:
            with closing(sqlite3.connect(Path(directory) / "dataset.sqlite")) as connection:
                yield connection


@contextmanager
def read_dataset(path: Path, checkpoint: Checkpoint | None = None) -> Iterator[Dataset]:
    with dataset_store(checkpoint) as connection:
        if checkpoint is not None and checkpoint.state.get("snapshot_ready"):
            if checkpoint.state.get("source") != file_identity(path):
                raise ValueError("Checkpoint input file was replaced or changed")
            nodes = checkpoint.state.get("snapshot_nodes")
            edges = checkpoint.state.get("snapshot_edges")
            dimension = checkpoint.state.get("snapshot_dimension")
            digest = checkpoint.state.get("snapshot_digest")
            if (
                not isinstance(nodes, int)
                or not isinstance(edges, int)
                or not isinstance(dimension, int)
                or not isinstance(digest, str)
            ):
                raise ValueError("Invalid checkpoint snapshot metadata")
            dataset = Dataset(connection, nodes, edges, dimension, digest=digest)
            print(
                json.dumps({"phase": "snapshot", "status": "reused", "nodes": nodes, "edges": edges}),
                file=sys.stderr,
                flush=True,
            )
            yield dataset
            return
        if checkpoint is not None and "digest" in checkpoint.state:
            raise ValueError("Checkpoint snapshot is missing; cannot safely resume")
        with progress("preparing_snapshot", input=str(path)):
            connection.execute("PRAGMA cache_size = -8192")
            connection.execute("PRAGMA temp_store = FILE")
            if checkpoint is not None:
                connection.execute("DROP TABLE IF EXISTS records")
                connection.execute("DROP TABLE IF EXISTS acknowledged")
            connection.execute(
                """CREATE TABLE records (
                    position INTEGER PRIMARY KEY,
                    kind INTEGER NOT NULL,
                    id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    target TEXT NOT NULL,
                    labels BLOB NOT NULL,
                    data BLOB NOT NULL,
                    UNIQUE (kind, id)
                )"""
            )
            connection.execute(
                "CREATE TABLE acknowledged (kind INTEGER, id TEXT, PRIMARY KEY (kind, id)) WITHOUT ROWID"
            )
            dataset = Dataset(connection, 0, 0, 0)
            bytes_read = 0
            digest = hashlib.sha256()
            identity = file_identity(path)
            if (
                checkpoint is not None
                and "preparation_source" in checkpoint.state
                and checkpoint.state["preparation_source"] != identity
            ):
                raise ValueError("Checkpoint input file was replaced or changed")
            with (
                path.open("rb") as source,
                progress(
                    "reading",
                    input=str(path),
                    total_bytes=path.stat().st_size,
                    bytes_read=0,
                    records=0,
                    nodes=0,
                    edges=0,
                ) as reading,
            ):
                initial_stat = os.fstat(source.fileno())
                for line_number, line in enumerate(source, 1):
                    try:
                        record = to_record(json.loads(line.decode("utf-8")))
                        digest.update(line)
                        for text_property in record.text_properties.values():
                            for vector in text_property.embeddings:
                                dimension = len(vector.values)
                                if dataset.vector_dimension not in (0, dimension):
                                    raise ValueError("All embeddings must have the same dimension")
                                dataset.vector_dimension = dimension
                        try:
                            connection.execute(
                                "INSERT INTO records (kind, id, source, target, labels, data) VALUES (?, ?, ?, ?, ?, ?)",
                                (
                                    record.key.kind,
                                    record.key.id,
                                    record.source,
                                    record.target,
                                    nidx_pb2.LpgStringList(values=record.labels).SerializeToString(),
                                    record.SerializeToString(),
                                ),
                            )
                        except sqlite3.IntegrityError as error:
                            raise ValueError(
                                f"Duplicate record key {(record.key.kind, record.key.id)}"
                            ) from error
                        if record.key.kind == nidx_pb2.LPG_NODE:
                            dataset.node_count += 1
                        else:
                            dataset.edge_count += 1
                        bytes_read += len(line)
                        reading.update(
                            bytes_read=bytes_read,
                            records=dataset.node_count + dataset.edge_count,
                            nodes=dataset.node_count,
                            edges=dataset.edge_count,
                        )
                    except ValueError as error:
                        raise ValueError(f"{path}:{line_number}: {error}") from error
                if not dataset.node_count + dataset.edge_count:
                    raise ValueError(f"{path}: dataset is empty")
                final_stat = os.fstat(source.fileno())
                if (
                    initial_stat.st_size != final_stat.st_size
                    or initial_stat.st_mtime_ns != final_stat.st_mtime_ns
                    or initial_stat.st_ctime_ns != final_stat.st_ctime_ns
                    or bytes_read != final_stat.st_size
                    or identity != file_identity(path)
                ):
                    raise ValueError(f"{path}: input changed during validation")
                dataset.digest = digest.hexdigest()
            with progress("validating_endpoints", total_edges=dataset.edge_count):
                missing = connection.execute(
                    """SELECT edge.id FROM records AS edge
                    LEFT JOIN records AS source ON source.kind = ? AND source.id = edge.source
                    LEFT JOIN records AS target ON target.kind = ? AND target.id = edge.target
                    WHERE edge.kind = ? AND (source.id IS NULL OR target.id IS NULL)
                    LIMIT 1""",
                    (nidx_pb2.LPG_NODE, nidx_pb2.LPG_NODE, nidx_pb2.LPG_EDGE),
                ).fetchone()
                if missing is not None:
                    raise ValueError(f"Edge {missing[0]!r} refers to a node absent from the dataset")
            with progress("building_indexes"):
                connection.execute("CREATE INDEX record_order ON records (kind, position)")
                connection.commit()
            if checkpoint is not None:
                checkpoint.save(
                    snapshot_ready=True,
                    source=identity,
                    snapshot_nodes=dataset.node_count,
                    snapshot_edges=dataset.edge_count,
                    snapshot_dimension=dataset.vector_dimension,
                    snapshot_digest=dataset.digest,
                )
        yield dataset


def batches(
    dataset: Dataset,
    graph_id: str,
    batch_size: int,
    skip: int = 0,
) -> Iterator[nidx_pb2.BulkLoadRequest]:
    if not 1 <= batch_size <= 1000:
        raise ValueError("batch_size must be between 1 and 1000")
    request = nidx_pb2.BulkLoadRequest(graph_id=graph_id)
    if not 0 <= skip <= dataset.node_count + dataset.edge_count:
        raise ValueError("resume position is outside the dataset")
    records = (
        record
        for kind, count in (
            (nidx_pb2.LPG_NODE, min(skip, dataset.node_count)),
            (nidx_pb2.LPG_EDGE, max(0, skip - dataset.node_count)),
        )
        for record in dataset.records(kind, count)
    )
    for record in records:
        required: dict[str, nidx_pb2.LpgStringList] = {}
        if record.key.kind == nidx_pb2.LPG_EDGE:
            for endpoint in dict.fromkeys((record.source, record.target)):
                required[endpoint] = dataset.node_labels(endpoint)
        additions = {
            endpoint: labels
            for endpoint, labels in required.items()
            if endpoint not in request.endpoint_labels
        }
        request.records.append(record)
        for endpoint, labels in additions.items():
            request.endpoint_labels[endpoint].CopyFrom(labels)
        if len(request.records) > batch_size or request.ByteSize() > MAX_REQUEST_BYTES:
            del request.records[-1:]
            for endpoint in additions:
                del request.endpoint_labels[endpoint]
            if request.records:
                yield request
            request = nidx_pb2.BulkLoadRequest(
                graph_id=graph_id, records=[record], endpoint_labels=required
            )
            if request.ByteSize() > MAX_REQUEST_BYTES:
                raise ValueError(
                    f"Record {record.key.id!r} and its endpoint labels exceed the 3 MiB request limit"
                )
    if request.records:
        yield request


def load(
    api: nidx_pb2_grpc.NidxApiStub,
    lpg: nidx_pb2_grpc.NidxLPGStub,
    dataset: Dataset,
    *,
    graph_id: str | None,
    tenant: str | None,
    vectorset: str,
    batch_size: int,
    timeout: float,
    shard_wait: float = 0,
    checkpoint: Checkpoint | None = None,
    retry_uncertain: bool = False,
    resume_after: int | None = None,
    expected_tenant: str | None = None,
) -> str:
    if checkpoint is None and (graph_id is None) == (tenant is None):
        raise ValueError("Provide exactly one of graph_id or tenant")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")
    if not math.isfinite(shard_wait) or shard_wait < 0:
        raise ValueError("shard_wait must be finite and nonnegative")
    if expected_tenant is not None:
        expected_tenant = str(UUID(expected_tenant))
        if checkpoint is None:
            raise ValueError("expected_tenant requires a checkpoint")
        if tenant is not None and str(UUID(tenant)) != expected_tenant:
            raise ValueError("Expected tenant does not match the requested tenant")
    if checkpoint is not None:
        state = checkpoint.state
        if "digest" not in state:
            if (graph_id is None) == (tenant is None):
                raise ValueError("Provide exactly one of graph_id or tenant")
            if resume_after is not None and (graph_id is None or not retry_uncertain):
                raise ValueError("resume_after requires an existing graph and retry_uncertain")
            initial = resume_after or 0
            if not 0 <= initial <= dataset.node_count + dataset.edge_count:
                raise ValueError("resume position is outside the dataset")
            checkpoint.save(
                digest=dataset.digest,
                nodes=dataset.node_count,
                edges=dataset.edge_count,
                vectorset=vectorset,
                graph_id=str(UUID(graph_id)) if graph_id else None,
                tenant=str(UUID(tenant)) if tenant else expected_tenant,
                acknowledged=initial,
                status="ready",
            )
        else:
            if resume_after is not None:
                raise ValueError("resume_after can only initialize a new checkpoint")
            if (
                state.get("digest") != dataset.digest
                or state.get("nodes") != dataset.node_count
                or state.get("edges") != dataset.edge_count
                or state.get("vectorset") != vectorset
            ):
                raise ValueError("Checkpoint does not match the input dataset or vectorset")
            saved_graph = state.get("graph_id")
            saved_tenant = state.get("tenant")
            if expected_tenant is not None and saved_tenant != expected_tenant:
                raise ValueError("Checkpoint tenant ID does not match")
            if state.get("status") not in ("ready", "creating_shard", "in_flight"):
                raise ValueError("Invalid checkpoint status")
            creating = state.get("status") == "creating_shard"
            if graph_id is not None and not creating and str(UUID(graph_id)) != saved_graph:
                raise ValueError("Checkpoint graph ID does not match")
            if tenant is not None and str(UUID(tenant)) != saved_tenant:
                raise ValueError("Checkpoint tenant ID does not match")
            if creating:
                if graph_id is None:
                    raise ValueError(
                        "Shard creation outcome is uncertain; resume with the recovered --graph-id"
                    )
                saved_graph = str(UUID(graph_id))
            if state.get("status") == "in_flight" and not retry_uncertain:
                raise ValueError(
                    "Previous batch outcome is uncertain; ensure it has finished, "
                    "then resume with --retry-uncertain"
                )
            if saved_graph is None and state.get("status") == "ready" and isinstance(saved_tenant, str):
                graph_id = None
                tenant = saved_tenant
            elif isinstance(saved_graph, str):
                graph_id = saved_graph
                tenant = None
            else:
                raise ValueError("Checkpoint has no graph ID")
        acknowledged_value = checkpoint.state.get("acknowledged")
        if not isinstance(acknowledged_value, int) or isinstance(acknowledged_value, bool):
            raise ValueError("Invalid checkpoint position")
        initial = acknowledged_value
        if not 0 <= initial <= dataset.node_count + dataset.edge_count:
            raise ValueError("Invalid checkpoint position")
    else:
        if resume_after is not None or retry_uncertain:
            raise ValueError("Resume options require a checkpoint")
        initial = 0
    if tenant is not None or dataset.vector_dimension:
        nonempty_string(vectorset)
    dataset.vectorset = vectorset if dataset.vector_dimension else ""
    # Validate request sizes before creating a shard or submitting any writes.
    with progress("preflight", batches_checked=0, checked_records=0) as preflight:
        checked_records = 0
        preflight_graph = str(UUID(graph_id)) if graph_id else str(UUID(int=0))
        requests = (
            batches(dataset, preflight_graph, batch_size, skip=initial)
            if initial
            else batches(dataset, preflight_graph, batch_size)
        )
        for count, request in enumerate(requests, 1):
            checked_records += len(request.records)
            preflight.update(batches_checked=count, checked_records=checked_records)
    with progress("creating_shard" if tenant is not None else "checking_shard"):
        if tenant is not None:
            if checkpoint is not None:
                checkpoint.save(status="creating_shard")
            created = api.NewShard(
                nodewriter_pb2.NewShardRequest(
                    kbid=str(UUID(tenant)),
                    vectorsets_configs={
                        vectorset: nodewriter_pb2.VectorIndexConfig(
                            # nidx requires a vector index even when no embeddings are supplied.
                            similarity=utils_pb2.COSINE,
                            vector_dimension=dataset.vector_dimension or 3,
                        )
                    },
                ),
                timeout=timeout,
            )
            graph_id = str(UUID(created.id))
            if checkpoint is not None:
                checkpoint.save(graph_id=graph_id, status="ready")
        else:
            assert graph_id is not None
            graph_id = str(UUID(graph_id))
            shard = api.GetShard(
                nodereader_pb2.GetShardRequest(shard_id=noderesources_pb2.ShardId(id=graph_id)),
                timeout=timeout,
            )
            if checkpoint is not None:
                actual_tenant = str(UUID(shard.metadata.kbid))
                saved_tenant = checkpoint.state.get("tenant")
                if saved_tenant is not None and saved_tenant != actual_tenant:
                    raise ValueError("Checkpoint tenant does not match the graph")
                checkpoint.save(tenant=actual_tenant, graph_id=graph_id, status="ready")
            if dataset.vector_dimension:
                available = api.ListVectorSets(noderesources_pb2.ShardId(id=graph_id), timeout=timeout)
                if vectorset not in available.vectorsets:
                    raise ValueError(f"Graph lacks vectorset {vectorset!r}; no writes submitted")
    identity: dict[str, str | bool] = {"graph_id": graph_id, "created": tenant is not None}
    if checkpoint is not None and isinstance(checkpoint.state.get("tenant"), str):
        identity["tenant"] = str(checkpoint.state["tenant"])
    elif tenant is not None:
        identity["tenant"] = str(UUID(tenant))
    print(json.dumps(identity), file=sys.stderr, flush=True)
    if tenant is not None and shard_wait:
        with progress("startup_wait", graph_id=graph_id, shard_wait_seconds=shard_wait):
            time.sleep(shard_wait)
    dataset.connection.execute("DELETE FROM acknowledged")
    acknowledged = initial
    submitted = 0
    total = dataset.node_count + dataset.edge_count
    progress_values: dict[str, str | int] = {
        "graph_id": graph_id,
        "total": total,
        "acknowledged": acknowledged,
        "resumed_from": initial,
        "submitted_records": 0,
        "in_flight_records": 0,
    }
    if checkpoint is not None:
        progress_values["checkpoint"] = str(checkpoint.path)
    with progress("loading", **progress_values) as loading:
        requests = (
            batches(dataset, graph_id, batch_size, skip=initial)
            if initial
            else batches(dataset, graph_id, batch_size)
        )
        for request in requests:
            expected = list(request.records)
            loading.update(in_flight_records=len(expected))
            if checkpoint is not None:
                checkpoint.save(status="in_flight", in_flight_records=len(expected))
            try:
                response = lpg.BulkLoad(request, timeout=timeout)
            except grpc.RpcError as error:
                raise RuntimeError(
                    f"Graph {graph_id}: RPC failed after {acknowledged} acknowledged records; "
                    f"current batch outcome is uncertain. No automatic recovery or retry. {error}"
                ) from error
            if len(response.results) != len(expected):
                raise RuntimeError(f"Graph {graph_id}: invalid result count; batch outcome is uncertain")
            applied = sum(
                result.status == nidx_pb2.LPG_OPERATION_STATUS_APPLIED for result in response.results
            )
            for record, result in zip(expected, response.results, strict=True):
                if result.status != nidx_pb2.LPG_OPERATION_STATUS_APPLIED:
                    raise RuntimeError(
                        f"Graph {graph_id}: {record.key.id} status={result.status} "
                        f"{result.error_code}: {result.error_message}; {acknowledged} previously acknowledged, "
                        f"{applied} applied in current batch. Earlier writes remain; no automatic retry."
                    )
            acknowledged += (
                len(expected) if checkpoint is not None else dataset.mark_acknowledged(expected)
            )
            submitted += len(expected)
            if checkpoint is not None:
                checkpoint.save(status="ready", acknowledged=acknowledged, in_flight_records=0)
            loading.update(acknowledged=acknowledged, submitted_records=submitted, in_flight_records=0)
    return graph_id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path(__file__).with_name("synthetic.jsonl"))
    parser.add_argument(
        "--address", required=True, help="Trusted plaintext API gRPC target, e.g. localhost:10000"
    )
    target = parser.add_mutually_exclusive_group()
    target.add_argument("--graph-id", help="Existing dedicated LPG shard UUID")
    target.add_argument("--tenant", help="Tenant UUID for a new dedicated shard; generated if omitted")
    parser.add_argument("--vectorset", help="Vectorset name (default: lpg-property-v1)")
    parser.add_argument(
        "--expected-tenant",
        help="Verify an existing checkpoint's graph belongs to this tenant UUID",
    )
    parser.add_argument(
        "--checkpoint", type=Path, help="Persistent checkpoint file (created only for a new load)"
    )
    parser.add_argument("--resume", action="store_true", help="Resume an existing checkpoint")
    parser.add_argument(
        "--retry-uncertain",
        action="store_true",
        help="Retry an uncertain batch after confirming its previous RPC has finished",
    )
    parser.add_argument(
        "--resume-after",
        type=int,
        help="Initialize a new checkpoint from a trusted previously acknowledged record count",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Maximum records written per request; endpoint label references do not count (1-1000)",
    )
    parser.add_argument(
        "--shard-wait",
        type=float,
        default=5,
        help="Optional seconds to pause after creating a shard, before writes (default: 5)",
    )
    parser.add_argument("--timeout", type=float, help="Per-RPC seconds; default 30 * batch-size + 60")
    args = parser.parse_args()
    if (
        args.resume or args.retry_uncertain or args.resume_after is not None or args.expected_tenant
    ) and not args.checkpoint:
        parser.error("Resume options and --expected-tenant require --checkpoint")
    if args.graph_id is None and args.tenant is None and not args.resume:
        args.tenant = str(uuid4())
    try:
        checkpoint_context = (
            open_checkpoint(args.checkpoint, resume=args.resume)
            if args.checkpoint
            else nullcontext(None)
        )
        with checkpoint_context as checkpoint:
            if checkpoint is not None:
                address = checkpoint.state.get("address")
                if address is not None and address != args.address:
                    raise ValueError("Checkpoint API address does not match")
                if not args.resume or "preparation_graph_id" not in checkpoint.state:
                    if args.resume and args.graph_id is None and args.tenant is None:
                        raise ValueError(
                            "Checkpoint preparation was interrupted before saving its target; "
                            "resume with the original --graph-id or --tenant and bootstrap options"
                        )
                    if args.vectorset is None:
                        args.vectorset = "lpg-property-v1"
                    checkpoint.save(
                        address=args.address,
                        preparation_source=file_identity(args.input),
                        preparation_graph_id=args.graph_id,
                        preparation_tenant=args.tenant,
                        preparation_resume_after=args.resume_after,
                        preparation_expected_tenant=args.expected_tenant,
                        preparation_retry_uncertain=args.retry_uncertain,
                        preparation_vectorset=args.vectorset,
                    )
                elif "digest" not in checkpoint.state:
                    if checkpoint.state.get("preparation_source") != file_identity(args.input):
                        raise ValueError("Checkpoint input file was replaced or changed")
                    saved_graph = checkpoint.state.get("preparation_graph_id")
                    saved_tenant = checkpoint.state.get("preparation_tenant")
                    saved_count = checkpoint.state.get("preparation_resume_after")
                    saved_expected_tenant = checkpoint.state.get("preparation_expected_tenant")
                    saved_vectorset = checkpoint.state.get("preparation_vectorset")
                    if args.graph_id is not None and args.graph_id != saved_graph:
                        raise ValueError("Checkpoint graph ID does not match")
                    if args.tenant is not None and args.tenant != saved_tenant:
                        raise ValueError("Checkpoint tenant ID does not match")
                    if args.resume_after is not None and args.resume_after != saved_count:
                        raise ValueError("Checkpoint initial resume position does not match")
                    if (
                        args.expected_tenant is not None
                        and args.expected_tenant != saved_expected_tenant
                    ):
                        raise ValueError("Checkpoint tenant ID does not match")
                    if args.vectorset is not None and args.vectorset != saved_vectorset:
                        raise ValueError("Checkpoint vectorset does not match")
                    if args.graph_id is None and args.tenant is None:
                        args.graph_id, args.tenant = saved_graph, saved_tenant
                    if args.resume_after is None:
                        args.resume_after = saved_count
                    if args.expected_tenant is None:
                        args.expected_tenant = saved_expected_tenant
                    if args.vectorset is None:
                        args.vectorset = saved_vectorset
                    args.retry_uncertain = args.retry_uncertain or bool(
                        checkpoint.state.get("preparation_retry_uncertain")
                    )
                elif args.vectorset is None:
                    args.vectorset = checkpoint.state.get("vectorset")
            if args.vectorset is None:
                args.vectorset = "lpg-property-v1"
            with (
                read_dataset(args.input, checkpoint) as dataset,
                grpc.insecure_channel(args.address) as channel,
            ):
                graph_id = load(
                    nidx_pb2_grpc.NidxApiStub(channel),
                    nidx_pb2_grpc.NidxLPGStub(channel),
                    dataset,
                    graph_id=args.graph_id,
                    tenant=args.tenant,
                    vectorset=args.vectorset,
                    batch_size=args.batch_size,
                    timeout=args.timeout if args.timeout is not None else 30 * args.batch_size + 60,
                    shard_wait=args.shard_wait,
                    checkpoint=checkpoint,
                    retry_uncertain=args.retry_uncertain,
                    resume_after=args.resume_after,
                    expected_tenant=args.expected_tenant,
                )
                nodes, edges = dataset.node_count, dataset.edge_count
                checkpoint_tenant = checkpoint.state.get("tenant") if checkpoint is not None else None
    except (OSError, ValueError, RuntimeError, sqlite3.Error, grpc.RpcError) as error:
        print(f"Load failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    summary: dict[str, str | int] = {
        "graph_id": graph_id,
        "nodes": nodes,
        "edges": edges,
    }
    if args.tenant is not None:
        summary["tenant"] = str(UUID(args.tenant))
    elif isinstance(checkpoint_tenant, str):
        summary["tenant"] = checkpoint_tenant
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
