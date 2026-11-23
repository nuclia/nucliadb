# Copyright (C) 2021 Bosutech XXI S.L.
#
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Validate synthetic JSONL and load it into a remote Rust NidxLPG service."""

import argparse
import json
import math
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from uuid import UUID, uuid4

import grpc
from nidx_protos import nidx_pb2, nidx_pb2_grpc, nodereader_pb2, noderesources_pb2, nodewriter_pb2

from nucliadb_protos import utils_pb2

MAX_REQUEST_BYTES = 3 * 1024 * 1024


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
    nodes: list[nidx_pb2.LpgRecord]
    edges: list[nidx_pb2.LpgRecord]
    vector_dimension: int


def read_dataset(path: Path) -> Dataset:
    dataset = Dataset([], [], 0)
    keys: set[tuple[int, str]] = set()
    with path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            try:
                record = to_record(json.loads(line))
                key = (record.key.kind, record.key.id)
                if key in keys:
                    raise ValueError(f"Duplicate record key {key}")
                keys.add(key)
                for text_property in record.text_properties.values():
                    for vector in text_property.embeddings:
                        dimension = len(vector.values)
                        if dataset.vector_dimension not in (0, dimension):
                            raise ValueError("All embeddings must have the same dimension")
                        dataset.vector_dimension = dimension
                (dataset.nodes if record.key.kind == nidx_pb2.LPG_NODE else dataset.edges).append(record)
            except ValueError as error:
                raise ValueError(f"{path}:{line_number}: {error}") from error
    if not keys:
        raise ValueError(f"{path}: dataset is empty")
    node_ids = {record.key.id for record in dataset.nodes}
    for edge in dataset.edges:
        if edge.source not in node_ids or edge.target not in node_ids:
            raise ValueError(f"Edge {edge.key.id!r} refers to a node absent from the dataset")
    return dataset


def batches(
    dataset: Dataset,
    graph_id: str,
    batch_size: int,
) -> Iterator[nidx_pb2.BulkLoadRequest]:
    if not 1 <= batch_size <= 1000:
        raise ValueError("batch_size must be between 1 and 1000")
    nodes = {record.key.id: record for record in dataset.nodes}
    request = nidx_pb2.BulkLoadRequest(graph_id=graph_id)
    included: set[tuple[int, str]] = set()
    for record in [*dataset.nodes, *dataset.edges]:
        required = []
        if record.key.kind == nidx_pb2.LPG_EDGE:
            for endpoint in dict.fromkeys((record.source, record.target)):
                if endpoint not in nodes:
                    raise ValueError(f"Edge {record.key.id!r} refers to a node absent from the dataset")
                required.append(nodes[endpoint])
        required.append(record)
        additions = [item for item in required if (item.key.kind, item.key.id) not in included]
        before = len(request.records)
        request.records.extend(additions)
        if len(request.records) > batch_size or request.ByteSize() > MAX_REQUEST_BYTES:
            del request.records[before:]
            if request.records:
                yield request
            request = nidx_pb2.BulkLoadRequest(graph_id=graph_id, records=required)
            if len(request.records) > batch_size:
                raise ValueError(
                    f"Record {record.key.id!r} and its endpoint nodes require "
                    f"{len(request.records)} records; increase batch_size"
                )
            if request.ByteSize() > MAX_REQUEST_BYTES:
                raise ValueError(
                    f"Record {record.key.id!r} and its endpoints exceed the 3 MiB request limit"
                )
            included = {(item.key.kind, item.key.id) for item in required}
        else:
            included.update((item.key.kind, item.key.id) for item in additions)
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
    shard_wait: float = 5,
) -> str:
    if (graph_id is None) == (tenant is None):
        raise ValueError("Provide exactly one of graph_id or tenant")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")
    if not math.isfinite(shard_wait) or shard_wait < 0:
        raise ValueError("shard_wait must be finite and nonnegative")
    if tenant is not None or dataset.vector_dimension:
        nonempty_string(vectorset)
    if dataset.vector_dimension:
        for record in [*dataset.nodes, *dataset.edges]:
            record.vectorset = vectorset
    # Validate request sizes before creating a shard or submitting any writes.
    requests = list(batches(dataset, str(UUID(graph_id)) if graph_id else str(UUID(int=0)), batch_size))
    if tenant is not None:
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
    else:
        assert graph_id is not None
        graph_id = str(UUID(graph_id))
        api.GetShard(
            nodereader_pb2.GetShardRequest(shard_id=noderesources_pb2.ShardId(id=graph_id)),
            timeout=timeout,
        )
        if dataset.vector_dimension:
            available = api.ListVectorSets(noderesources_pb2.ShardId(id=graph_id), timeout=timeout)
            if vectorset not in available.vectorsets:
                raise ValueError(f"Graph lacks vectorset {vectorset!r}; no writes submitted")
    identity: dict[str, str | bool] = {"graph_id": graph_id, "created": tenant is not None}
    if tenant is not None:
        identity["tenant"] = str(UUID(tenant))
    print(json.dumps(identity), file=sys.stderr, flush=True)
    if tenant is not None and shard_wait:
        print(
            json.dumps({"graph_id": graph_id, "shard_wait_seconds": shard_wait}),
            file=sys.stderr,
            flush=True,
        )
        time.sleep(shard_wait)
    verified_keys: set[tuple[int, str]] = set()
    submitted = 0
    total = len(dataset.nodes) + len(dataset.edges)
    started = perf_counter()
    for request in requests:
        request.graph_id = graph_id
        expected = list(request.records)
        try:
            response = lpg.BulkLoad(request, timeout=timeout)
        except grpc.RpcError as error:
            raise RuntimeError(
                f"Graph {graph_id}: RPC failed after {len(verified_keys)} verified records; "
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
                    f"{result.error_code}: {result.error_message}; {len(verified_keys)} previously verified, "
                    f"{applied} applied in current batch. Earlier writes remain; no automatic retry."
                )
        persisted = lpg.GetRecords(
            nidx_pb2.GetRecordsRequest(graph_id=graph_id, keys=[record.key for record in expected]),
            timeout=timeout,
        )
        if list(persisted.records) != expected:
            raise RuntimeError(f"Graph {graph_id}: read-back verification failed; writes remain applied")
        verified_keys.update((record.key.kind, record.key.id) for record in expected)
        verified = len(verified_keys)
        submitted += len(expected)
        elapsed = perf_counter() - started
        print(
            json.dumps(
                {
                    "graph_id": graph_id,
                    "verified": verified,
                    "total": total,
                    "percent": round(100 * verified / total, 2),
                    "records_per_second": round(verified / elapsed, 2) if elapsed > 0 else 0,
                    "elapsed_seconds": round(elapsed, 2),
                    "submitted_records": submitted,
                    "submitted_records_per_second": round(submitted / elapsed, 2) if elapsed > 0 else 0,
                }
            ),
            file=sys.stderr,
            flush=True,
        )
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
    parser.add_argument("--vectorset", default="lpg-property-v1")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Maximum records per request, including repeated endpoints (1-1000)",
    )
    parser.add_argument(
        "--shard-wait",
        type=float,
        default=5,
        help="Seconds to pause after creating a shard, before writes (default: 5; 0 disables)",
    )
    parser.add_argument("--timeout", type=float, help="Per-RPC seconds; default 30 * batch-size + 60")
    args = parser.parse_args()
    if args.graph_id is None and args.tenant is None:
        args.tenant = str(uuid4())
    try:
        dataset = read_dataset(args.input)
        with grpc.insecure_channel(args.address) as channel:
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
            )
    except (OSError, ValueError, RuntimeError, grpc.RpcError) as error:
        print(f"Load failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    summary: dict[str, str | int] = {
        "graph_id": graph_id,
        "nodes": len(dataset.nodes),
        "edges": len(dataset.edges),
    }
    if args.tenant is not None:
        summary["tenant"] = str(UUID(args.tenant))
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
