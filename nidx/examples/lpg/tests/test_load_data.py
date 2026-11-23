# Copyright (C) 2021 Bosutech XXI S.L.
#
# SPDX-License-Identifier: AGPL-3.0-or-later
import json
import subprocess
import sys
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import UUID, uuid4

import grpc
import pytest
from nidx_protos import nidx_pb2, nidx_pb2_grpc, nodereader_pb2, noderesources_pb2, nodewriter_pb2

from nidx.examples.lpg import load_data
from nidx.examples.lpg.generate_data import Config, write_dataset


class FakeService(nidx_pb2_grpc.NidxApiServicer, nidx_pb2_grpc.NidxLPGServicer):
    def __init__(self) -> None:
        self.graph_id = str(uuid4())
        self.tenant = str(uuid4())
        self.created: list[nodewriter_pb2.NewShardRequest] = []
        self.records: dict[tuple[int, str], nidx_pb2.LpgRecord] = {}
        self.requests: list[nidx_pb2.BulkLoadRequest] = []
        self.fail_index: int | None = None
        self.corrupt_read = False
        self.transport_failure = False
        self.vectorsets = ["lpg-property-v1"]

    def BulkLoad(
        self,
        request: nidx_pb2.BulkLoadRequest,
        context: grpc.ServicerContext,
    ) -> nidx_pb2.LpgOperationResponse:
        assert request.graph_id == self.graph_id
        node_ids = {record.key.id for record in request.records if record.key.kind == nidx_pb2.LPG_NODE}
        assert len({(record.key.kind, record.key.id) for record in request.records}) == len(
            request.records
        )
        for record in request.records:
            if record.key.kind == nidx_pb2.LPG_EDGE:
                assert record.source in node_ids
                assert record.target in node_ids
        self.requests.append(request)
        if self.transport_failure:
            context.abort(grpc.StatusCode.UNAVAILABLE, "test downstream unavailable")
        response = nidx_pb2.LpgOperationResponse()
        failed = False
        for index, record in enumerate(request.records):
            if failed:
                response.results.add(status=nidx_pb2.LPG_OPERATION_STATUS_SKIPPED)
            elif index == self.fail_index:
                response.results.add(
                    status=nidx_pb2.LPG_OPERATION_STATUS_FAILED,
                    error_code="FailedPrecondition",
                    error_message="test rejection",
                )
                failed = True
            else:
                copy = nidx_pb2.LpgRecord()
                copy.CopyFrom(record)
                self.records[record.key.kind, record.key.id] = copy
                response.results.add(status=nidx_pb2.LPG_OPERATION_STATUS_APPLIED)
        return response

    def NewShard(
        self, request: nodewriter_pb2.NewShardRequest, context: grpc.ServicerContext
    ) -> noderesources_pb2.ShardCreated:
        if not request.vectorsets_configs:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Can't create shard without a vector index")
        self.created.append(request)
        return noderesources_pb2.ShardCreated(id=self.graph_id)

    def GetShard(
        self, request: nodereader_pb2.GetShardRequest, context: grpc.ServicerContext
    ) -> noderesources_pb2.Shard:
        assert request.shard_id.id == self.graph_id
        return noderesources_pb2.Shard(
            shard_id=self.graph_id, metadata=noderesources_pb2.ShardMetadata(kbid=self.tenant)
        )

    def ListVectorSets(
        self, request: noderesources_pb2.ShardId, context: grpc.ServicerContext
    ) -> noderesources_pb2.VectorSetList:
        return noderesources_pb2.VectorSetList(vectorsets=self.vectorsets)

    def GetRecords(
        self, request: nidx_pb2.GetRecordsRequest, context: grpc.ServicerContext
    ) -> nidx_pb2.GetRecordsResponse:
        if self.corrupt_read:
            return nidx_pb2.GetRecordsResponse()
        return nidx_pb2.GetRecordsResponse(
            records=[self.records[key.kind, key.id] for key in request.keys]
        )


@pytest.fixture
def remote() -> Iterator[tuple[FakeService, str]]:
    service = FakeService()
    with ThreadPoolExecutor(max_workers=2) as pool:
        server = grpc.server(pool)
        nidx_pb2_grpc.add_NidxApiServicer_to_server(service, server)
        nidx_pb2_grpc.add_NidxLPGServicer_to_server(service, server)
        port = server.add_insecure_port("127.0.0.1:0")
        assert port
        server.start()
        try:
            yield service, f"127.0.0.1:{port}"
        finally:
            server.stop(0).wait()


def tiny_dataset(tmp_path: Path, dimension: int = 0) -> load_data.Dataset:
    path = tmp_path / "data.jsonl"
    write_dataset(path, Config(products=3, warehouses=2, clients=2, vector_dimension=dimension))
    return load_data.read_dataset(path)


def run_load(
    service: FakeService,
    address: str,
    dataset: load_data.Dataset,
    *,
    create: bool = False,
    shard_wait: float = 0,
) -> str:
    with grpc.insecure_channel(address) as channel:
        return load_data.load(
            nidx_pb2_grpc.NidxApiStub(channel),
            nidx_pb2_grpc.NidxLPGStub(channel),
            dataset,
            graph_id=None if create else service.graph_id,
            tenant=service.tenant if create else None,
            vectorset="lpg-property-v1",
            batch_size=4,
            timeout=5,
            shard_wait=shard_wait,
        )


@pytest.mark.parametrize("create,dimension", [(False, 0), (True, 0), (True, 3), (False, 3), (True, 384)])
def test_remote_load(
    tmp_path: Path, remote: tuple[FakeService, str], create: bool, dimension: int
) -> None:
    service, address = remote
    dataset = tiny_dataset(tmp_path, dimension)
    assert dataset.vector_dimension == dimension
    assert run_load(service, address, dataset, create=create) == service.graph_id
    assert len(service.records) == 30
    assert all(1 <= len(request.records) <= 4 for request in service.requests)
    assert all(request.graph_id == service.graph_id for request in service.requests)
    product = service.records[nidx_pb2.LPG_NODE, "P00000001"]
    assert product.properties["tags"].WhichOneof("value") == "strings"
    assert product.properties["available"].WhichOneof("value") == "boolean"
    assert product.properties["price"].WhichOneof("value") == "float"
    assert bool(product.text_properties["description"].embeddings) == bool(dimension)
    if create:
        assert service.created[0].kbid == service.tenant
        configs = service.created[0].vectorsets_configs
        assert configs["lpg-property-v1"].vector_dimension == (dimension or 3)
        if not dimension:
            assert all(
                not text_property.embeddings
                for record in service.records.values()
                for text_property in record.text_properties.values()
            )
    else:
        assert not service.created


def test_self_contained_batches(
    tmp_path: Path,
    remote: tuple[FakeService, str],
) -> None:
    service, address = remote
    dataset = tiny_dataset(tmp_path, 384)
    run_load(service, address, dataset, create=True)
    assert len(service.records) == 30
    assert sum(len(request.records) for request in service.requests) > 30
    assert all(request.ByteSize() <= load_data.MAX_REQUEST_BYTES for request in service.requests)
    for request in service.requests:
        nodes = {
            record.key.id: record for record in request.records if record.key.kind == nidx_pb2.LPG_NODE
        }
        for record in request.records:
            if record.key.kind == nidx_pb2.LPG_EDGE:
                for endpoint in (record.source, record.target):
                    assert nodes[endpoint] == next(
                        node for node in dataset.nodes if node.key.id == endpoint
                    )


def test_bulk_failure_and_manual_retry(
    tmp_path: Path,
    remote: tuple[FakeService, str],
) -> None:
    service, address = remote
    dataset = tiny_dataset(tmp_path)
    service.fail_index = 1
    with pytest.raises(RuntimeError, match="test rejection"):
        run_load(service, address, dataset, create=True)
    assert len(service.requests) == 1
    service.fail_index = None
    reloaded = load_data.read_dataset(tmp_path / "data.jsonl")
    run_load(service, address, reloaded)
    assert len(service.records) == 30


def test_bulk_cli(tmp_path: Path, remote: tuple[FakeService, str]) -> None:
    service, address = remote
    path = tmp_path / "bulk.jsonl"
    write_dataset(path, Config(products=3, warehouses=2, clients=2, vector_dimension=384))
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "nidx.examples.lpg.load_data",
            "--input",
            str(path),
            "--address",
            address,
            "--shard-wait",
            "0",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(result.stdout)["graph_id"] == service.graph_id
    assert len(service.records) == 30
    assert service.requests
    assert '"percent": 100.0' in result.stderr


def test_progress(
    tmp_path: Path,
    remote: tuple[FakeService, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    service, address = remote
    clock = 100

    def tick() -> int:
        nonlocal clock
        clock += 2
        return clock

    monkeypatch.setattr(load_data, "perf_counter", tick)
    run_load(service, address, tiny_dataset(tmp_path))
    progress = [
        json.loads(line)
        for line in capsys.readouterr().err.splitlines()
        if line.startswith("{") and "verified" in json.loads(line)
    ]
    seen: set[tuple[int, str]] = set()
    submitted = 0
    for index, update in enumerate(progress, 1):
        records = service.requests[index - 1].records
        seen.update((record.key.kind, record.key.id) for record in records)
        submitted += len(records)
        assert update["verified"] == len(seen)
        assert update["total"] == 30
        assert update["percent"] == round(update["verified"] / 30 * 100, 2)
        assert update["elapsed_seconds"] == index * 2
        assert update["records_per_second"] == round(update["verified"] / (index * 2), 2)
        assert update["submitted_records"] == submitted
        assert update["submitted_records_per_second"] == round(submitted / (index * 2), 2)
    assert progress[-1]["percent"] == 100
    assert progress[-1]["verified"] == 30
    assert progress[-1]["submitted_records"] > 30


@pytest.mark.parametrize("create,wait", [(True, 5), (True, 0), (False, 5)])
def test_shard_wait(
    tmp_path: Path,
    remote: tuple[FakeService, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    create: bool,
    wait: float,
) -> None:
    service, address = remote
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        assert service.created
        assert not service.requests
        assert service.graph_id in capsys.readouterr().err
        sleeps.append(seconds)

    monkeypatch.setattr(load_data.time, "sleep", sleep)
    run_load(service, address, tiny_dataset(tmp_path), create=create, shard_wait=wait)
    assert sleeps == ([wait] if create and wait else [])
    assert len(service.records) == 30


@pytest.mark.parametrize("wait", [-1, float("nan"), float("inf")])
def test_invalid_shard_wait(
    tmp_path: Path,
    remote: tuple[FakeService, str],
    wait: float,
) -> None:
    service, address = remote
    with pytest.raises(ValueError, match="shard_wait must be finite and nonnegative"):
        run_load(service, address, tiny_dataset(tmp_path), create=True, shard_wait=wait)
    assert not service.created
    assert not service.requests


def test_partial_failure_stops_loader(tmp_path: Path, remote: tuple[FakeService, str]) -> None:
    service, address = remote
    service.fail_index = 1
    with pytest.raises(RuntimeError, match="test rejection; 0 previously verified, 1 applied"):
        run_load(service, address, tiny_dataset(tmp_path))
    assert len(service.requests) == 1
    assert len(service.records) == 1


@pytest.mark.parametrize("failure", ["corrupt_read", "transport_failure"])
def test_uncertain_or_unverified_write(
    tmp_path: Path, remote: tuple[FakeService, str], failure: str
) -> None:
    service, address = remote
    setattr(service, failure, True)
    message = (
        "read-back verification failed" if failure == "corrupt_read" else "test downstream unavailable"
    )
    with pytest.raises(RuntimeError, match=message):
        run_load(service, address, tiny_dataset(tmp_path))
    assert len(service.requests) == 1


def test_missing_vectorset_submits_no_writes(tmp_path: Path, remote: tuple[FakeService, str]) -> None:
    service, address = remote
    service.vectorsets = []
    with pytest.raises(ValueError, match="Graph lacks vectorset"):
        run_load(service, address, tiny_dataset(tmp_path, 3))
    assert not service.requests


@pytest.mark.parametrize(
    "row,message",
    [
        ({"properties": {"x": 2**63}}, "int64"),
        ({"properties": {"x": float("inf")}}, "finite float"),
        ({"properties": {"x": [1]}}, "list of strings"),
        ({"source": "unexpected"}, "Unexpected fields"),
        ({"embeddings": {"missing": [[1]]}}, "missing text property"),
        ({"text_chunks": {"description": [""]}}, "nonempty string"),
        ({"embeddings": {"description": [[0, 0]]}}, "nonzero"),
        ({"embeddings": {"description": [[True]]}}, "finite float32"),
        ({"embeddings": {"description": [[float("nan")]]}}, "finite float32"),
        ({"embeddings": {"description": [[1e100]]}}, "finite float32"),
    ],
)
def test_invalid_row(row: dict[str, object], message: str) -> None:
    valid = {
        "kind": "node",
        "id": "P1",
        "labels": ["Product"],
        "properties": {},
        "text_chunks": {"description": ["A product"]},
    }
    with pytest.raises(ValueError, match=message):
        load_data.to_record({**valid, **row})


def test_nodes_require_types_and_edges_have_one_type() -> None:
    node = load_data.to_record(
        {"kind": "node", "id": "P1", "labels": ["Product", "Sellable"], "properties": {}}
    )
    assert list(node.labels) == ["Product", "Sellable"]
    edge = load_data.to_record(
        {
            "kind": "edge",
            "id": "E1",
            "source": "P1",
            "target": "P1",
            "edge_type": "BUYS",
            "properties": {},
        }
    )
    assert edge.edge_type == "BUYS"
    assert not edge.labels
    with pytest.raises(ValueError, match="at least one label/type"):
        load_data.to_record({"kind": "node", "id": "P2", "labels": [], "properties": {}})
    with pytest.raises(ValueError, match="unique"):
        load_data.to_record(
            {"kind": "node", "id": "P3", "labels": ["Product", "Product"], "properties": {}}
        )
    with pytest.raises(ValueError, match="Unexpected fields"):
        load_data.to_record(
            {
                "kind": "edge",
                "id": "E2",
                "source": "P1",
                "target": "P1",
                "edge_type": "BUYS",
                "labels": ["Relationship"],
                "properties": {},
            }
        )


@pytest.mark.parametrize("failure", ["duplicate", "missing_endpoint", "dimension", "blank"])
def test_invalid_file(tmp_path: Path, failure: str) -> None:
    path = tmp_path / "invalid.jsonl"
    rows: list[dict[str, object]] = [
        {"kind": "node", "id": "P1", "labels": ["Product"], "properties": {}},
    ]
    if failure == "duplicate":
        rows *= 2
    elif failure == "missing_endpoint":
        rows.append(
            {
                "kind": "edge",
                "id": "E1",
                "properties": {},
                "source": "P1",
                "target": "missing",
                "edge_type": "BUYS",
            }
        )
    elif failure == "dimension":
        rows[0].update(text_chunks={"x": ["text"]}, embeddings={"x": [[1]]})
        rows.append(
            {
                "kind": "node",
                "id": "P2",
                "labels": ["Product"],
                "properties": {},
                "text_chunks": {"x": ["text"]},
                "embeddings": {"x": [[1, 2]]},
            }
        )
    path.write_text(
        "\n".join(json.dumps(row) for row in rows) + ("\n\n" if failure == "blank" else "\n")
    )
    with pytest.raises(ValueError):
        load_data.read_dataset(path)


def test_batch_byte_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    records = [
        load_data.to_record({"kind": "node", "id": str(index), "labels": ["Node"], "properties": {}})
        for index in range(3)
    ]
    single_size = nidx_pb2.BulkLoadRequest(graph_id="graph", records=[records[0]]).ByteSize()
    monkeypatch.setattr(load_data, "MAX_REQUEST_BYTES", single_size)
    dataset = load_data.Dataset(records, [], 0)
    requests = list(load_data.batches(dataset, "graph", 10))
    assert [len(request.records) for request in requests] == [1, 1, 1]
    monkeypatch.setattr(load_data, "MAX_REQUEST_BYTES", single_size - 1)
    with pytest.raises(ValueError, match="request limit"):
        list(load_data.batches(dataset, "graph", 10))


def test_endpoint_batch_limits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dataset = tiny_dataset(tmp_path)
    with pytest.raises(ValueError, match="increase batch_size"):
        list(load_data.batches(dataset, "graph", 2))
    edge = dataset.edges[0]
    endpoints = [node for node in dataset.nodes if node.key.id in (edge.source, edge.target)]
    required_size = nidx_pb2.BulkLoadRequest(graph_id="graph", records=[*endpoints, edge]).ByteSize()
    monkeypatch.setattr(load_data, "MAX_REQUEST_BYTES", required_size - 1)
    with pytest.raises(ValueError, match="endpoints exceed"):
        list(load_data.batches(dataset, "graph", 1000))


def test_small_batch_fails_before_shard_creation(
    tmp_path: Path, remote: tuple[FakeService, str]
) -> None:
    service, address = remote
    with grpc.insecure_channel(address) as channel:
        with pytest.raises(ValueError, match="increase batch_size"):
            load_data.load(
                nidx_pb2_grpc.NidxApiStub(channel),
                nidx_pb2_grpc.NidxLPGStub(channel),
                tiny_dataset(tmp_path),
                graph_id=None,
                tenant=service.tenant,
                vectorset="lpg-property-v1",
                batch_size=2,
                timeout=5,
                shard_wait=0,
            )
    assert not service.created
    assert not service.requests


def test_self_loop_endpoint_is_included_once() -> None:
    node = load_data.to_record({"kind": "node", "id": "P1", "labels": ["Node"], "properties": {}})
    edge = load_data.to_record(
        {
            "kind": "edge",
            "id": "E1",
            "source": "P1",
            "target": "P1",
            "edge_type": "LINK",
            "properties": {},
        }
    )
    requests = list(load_data.batches(load_data.Dataset([node], [edge], 0), "graph", 2))
    assert len(requests) == 1
    assert list(requests[0].records) == [node, edge]


@pytest.mark.parametrize("target", ["automatic", "tenant", "graph"])
@pytest.mark.parametrize("dimension", [0, 384])
def test_cli(tmp_path: Path, remote: tuple[FakeService, str], target: str, dimension: int) -> None:
    service, address = remote
    path = tmp_path / "cli.jsonl"
    write_dataset(path, Config(products=3, warehouses=2, clients=2, vector_dimension=dimension))
    command = [
        sys.executable,
        "-m",
        "nidx.examples.lpg.load_data",
        "--input",
        str(path),
        "--address",
        address,
        "--shard-wait",
        "0",
    ]
    if target == "tenant":
        command.extend(["--tenant", service.tenant])
    elif target == "graph":
        command.extend(["--graph-id", service.graph_id])
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    summary = json.loads(result.stdout)
    if target == "graph":
        assert not service.created
        assert "tenant" not in summary
    else:
        tenant = summary.pop("tenant")
        assert str(UUID(tenant)) == tenant
        assert service.created[0].kbid == tenant
        assert service.created[0].vectorsets_configs["lpg-property-v1"].vector_dimension == (
            dimension or 3
        )
        progress = [json.loads(line) for line in result.stderr.splitlines() if line.startswith("{")]
        assert progress[0]["tenant"] == tenant
        if target == "tenant":
            assert tenant == service.tenant
        else:
            assert UUID(tenant).version == 4
    assert summary == {"graph_id": service.graph_id, "nodes": 7, "edges": 23}
    assert '"verified": 30' in result.stderr
    path.write_text("not json\n")
    service.created.clear()
    result = subprocess.run(command, capture_output=True, text=True)
    assert result.returncode == 1
    assert f"{path}:1:" in result.stderr
    assert not service.created
