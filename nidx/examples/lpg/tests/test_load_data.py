# Copyright (C) 2021 Bosutech XXI S.L.
#
# SPDX-License-Identifier: AGPL-3.0-or-later
import json
import sqlite3
import subprocess
import sys
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event, current_thread, main_thread
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
        self.search_unavailable = False
        self.read_requests = 0
        self.transport_failure = False
        self.fail_after_apply = False
        self.vectorsets = ["lpg-property-v1"]
        self.before_bulk: Callable[[], None] | None = None
        self.after_bulk: Callable[[], None] | None = None

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
                for endpoint in (record.source, record.target):
                    assert endpoint in node_ids or endpoint in request.endpoint_labels
                    if endpoint not in node_ids:
                        node = self.records[nidx_pb2.LPG_NODE, endpoint]
                        assert list(request.endpoint_labels[endpoint].values) == list(node.labels)
        self.requests.append(request)
        if self.before_bulk is not None:
            self.before_bulk()
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
        if self.after_bulk is not None:
            self.after_bulk()
        if self.fail_after_apply:
            context.abort(grpc.StatusCode.UNAVAILABLE, "response lost after indexing")
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
        self.read_requests += 1
        if self.search_unavailable:
            context.abort(grpc.StatusCode.UNAVAILABLE, "records are not search-visible yet")
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


@pytest.fixture
def dataset_factory(tmp_path: Path) -> Iterator[Callable[[int], load_data.Dataset]]:
    with ExitStack() as stack:

        def create(dimension: int) -> load_data.Dataset:
            path = tmp_path / "data.jsonl"
            write_dataset(path, Config(products=3, warehouses=2, clients=2, vector_dimension=dimension))
            return stack.enter_context(load_data.read_dataset(path))

        yield create


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
    dataset_factory: Callable[[int], load_data.Dataset],
    remote: tuple[FakeService, str],
    create: bool,
    dimension: int,
) -> None:
    service, address = remote
    dataset = dataset_factory(dimension)
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


def test_endpoint_references_do_not_rewrite_nodes(
    dataset_factory: Callable[[int], load_data.Dataset],
    remote: tuple[FakeService, str],
) -> None:
    service, address = remote
    dataset = dataset_factory(384)
    run_load(service, address, dataset, create=True)
    assert len(service.records) == 30
    written = [record for request in service.requests for record in request.records]
    assert len(written) == len({(record.key.kind, record.key.id) for record in written}) == 30
    assert sum(record.key.kind == nidx_pb2.LPG_NODE for record in written) == dataset.node_count
    assert all(request.ByteSize() <= load_data.MAX_REQUEST_BYTES for request in service.requests)
    for request in service.requests:
        for record in request.records:
            if record.key.kind == nidx_pb2.LPG_EDGE:
                for endpoint in (record.source, record.target):
                    assert list(request.endpoint_labels[endpoint].values) == list(
                        dataset.node(endpoint).labels
                    )


def test_disk_backed_snapshot_with_edges_before_nodes(
    tmp_path: Path, remote: tuple[FakeService, str], capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "unordered.jsonl"
    write_dataset(path, Config(products=3, warehouses=2, clients=2, vector_dimension=3))
    path.write_text("\n".join(reversed(path.read_text().splitlines())) + "\n")
    service, address = remote
    with load_data.read_dataset(path) as dataset:
        database_path = Path(dataset.connection.execute("PRAGMA database_list").fetchone()[2])
        assert database_path.is_file()
        assert (dataset.node_count, dataset.edge_count) == (7, 23)
        path.write_text("changed after validation\n")
        for create in (True, False):
            run_load(service, address, dataset, create=create)
            progress = [
                json.loads(line)
                for line in capsys.readouterr().err.splitlines()
                if '"acknowledged"' in line
            ]
            assert progress[-1]["acknowledged"] == 30
            assert progress[-1]["percent"] == 100
        assert len(service.records) == 30
    assert not database_path.exists()


@pytest.mark.parametrize("failure", ["parse", "consumer"])
def test_temporary_store_cleanup_on_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    monkeypatch.setattr(load_data, "TemporaryDirectory", partial(TemporaryDirectory, dir=tmp_path))
    path = tmp_path / "data.jsonl"
    path.write_text(
        "invalid JSON\n"
        if failure == "parse"
        else '{"kind":"node","id":"N1","labels":["Node"],"properties":{}}\n'
    )
    with pytest.raises(ValueError if failure == "parse" else RuntimeError):
        with load_data.read_dataset(path):
            raise RuntimeError("consumer failed")
    assert not list(tmp_path.glob("nidx-lpg-*"))


def test_load_consumes_batches_lazily(
    dataset_factory: Callable[[int], load_data.Dataset],
    remote: tuple[FakeService, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, address = remote
    original = load_data.batches
    yielded: list[int] = []

    def tracked_batches(
        dataset: load_data.Dataset, graph_id: str, batch_size: int
    ) -> Iterator[nidx_pb2.BulkLoadRequest]:
        current_pass = len(yielded)
        yielded.append(0)
        for request in original(dataset, graph_id, batch_size):
            if current_pass == 1:
                assert len(service.requests) == yielded[current_pass]
            yielded[current_pass] += 1
            yield request

    monkeypatch.setattr(load_data, "batches", tracked_batches)
    run_load(service, address, dataset_factory(3))
    assert len(yielded) == 2
    assert yielded[0] == yielded[1] == len(service.requests)
    assert yielded[0] > 1


def test_bulk_failure_and_manual_retry(
    tmp_path: Path,
    dataset_factory: Callable[[int], load_data.Dataset],
    remote: tuple[FakeService, str],
) -> None:
    service, address = remote
    dataset = dataset_factory(0)
    service.fail_index = 1
    with pytest.raises(RuntimeError, match="test rejection"):
        run_load(service, address, dataset, create=True)
    assert len(service.requests) == 1
    service.fail_index = None
    with load_data.read_dataset(tmp_path / "data.jsonl") as reloaded:
        run_load(service, address, reloaded)
    assert len(service.records) == 30


def test_checkpoint_resume_after_uncertain_batch_with_new_batch_size(
    tmp_path: Path, remote: tuple[FakeService, str], capsys: pytest.CaptureFixture[str]
) -> None:
    service, address = remote
    path = tmp_path / "unordered.jsonl"
    checkpoint_path = tmp_path / "checkpoint.sqlite"
    write_dataset(path, Config(products=3, warehouses=2, clients=2, vector_dimension=3))
    path.write_text("\n".join(reversed(path.read_text().splitlines())) + "\n")
    command = [
        sys.executable,
        "-m",
        "nidx.examples.lpg.load_data",
        "--input",
        str(path),
        "--address",
        address,
        "--checkpoint",
        str(checkpoint_path),
        "--shard-wait",
        "0",
    ]

    def interrupt_second_batch() -> None:
        if len(service.requests) == 2:
            service.fail_after_apply = True

    service.before_bulk = interrupt_second_batch
    first = subprocess.run(
        [*command, "--tenant", service.tenant, "--batch-size", "4"],
        capture_output=True,
        text=True,
    )
    assert first.returncode == 1
    assert "response lost after indexing" in first.stderr
    assert len(service.records) == 8
    with load_data.open_checkpoint(checkpoint_path, resume=True) as checkpoint:
        assert checkpoint.state["graph_id"] == service.graph_id
        assert checkpoint.state["tenant"] == service.tenant
        assert checkpoint.state["acknowledged"] == 4
        assert checkpoint.state["status"] == "in_flight"
    service.before_bulk = None
    service.fail_after_apply = False
    before = len(service.requests)
    denied = subprocess.run([*command, "--resume"], capture_output=True, text=True)
    assert denied.returncode == 1
    assert "Previous batch outcome is uncertain" in denied.stderr
    assert len(service.requests) == before
    resumed = subprocess.run(
        [*command, "--resume", "--retry-uncertain", "--batch-size", "7"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(resumed.stdout) == {
        "graph_id": service.graph_id,
        "tenant": service.tenant,
        "nodes": 7,
        "edges": 23,
    }
    assert len(service.created) == 1
    assert len(service.records) == 30
    assert len(service.requests[before].records) == 7
    assert service.requests[before].records[0].key.id == service.requests[1].records[0].key.id
    assert '"status": "reused"' in resumed.stderr
    completed = [
        json.loads(line)
        for line in resumed.stderr.splitlines()
        if line.startswith("{") and json.loads(line).get("phase") == "loading"
    ][-1]
    assert (completed["resumed_from"], completed["acknowledged"], completed["percent"]) == (4, 30, 100)
    with load_data.open_checkpoint(checkpoint_path, resume=True) as checkpoint:
        assert checkpoint.state["status"] == "ready"
        assert checkpoint.state["acknowledged"] == 30
    before = len(service.requests)
    subprocess.run([*command, "--resume"], capture_output=True, text=True, check=True)
    assert len(service.requests) == before
    wrong_address = command.copy()
    wrong_address[wrong_address.index("--address") + 1] = "127.0.0.1:1"
    result = subprocess.run([*wrong_address, "--resume"], capture_output=True, text=True)
    assert result.returncode == 1
    assert "Checkpoint API address does not match" in result.stderr
    assert len(service.requests) == before
    capsys.readouterr()


def test_resume_before_shard_creation_reuses_saved_tenant(
    tmp_path: Path, remote: tuple[FakeService, str]
) -> None:
    service, address = remote
    path = tmp_path / "dataset.jsonl"
    checkpoint_path = tmp_path / "checkpoint.sqlite"
    write_dataset(path, Config(products=3, warehouses=2, clients=2))
    command = [
        sys.executable,
        "-m",
        "nidx.examples.lpg.load_data",
        "--input",
        str(path),
        "--address",
        address,
        "--checkpoint",
        str(checkpoint_path),
        "--shard-wait",
        "0",
    ]
    failed = subprocess.run(
        [*command, "--tenant", service.tenant, "--batch-size", "0"],
        capture_output=True,
        text=True,
    )
    assert failed.returncode == 1
    assert not service.created
    with load_data.open_checkpoint(checkpoint_path, resume=True) as checkpoint:
        assert checkpoint.state["status"] == "ready"
        assert checkpoint.state["graph_id"] is None
        assert checkpoint.state["tenant"] == service.tenant
    resumed = subprocess.run([*command, "--resume"], capture_output=True, text=True, check=True)
    assert json.loads(resumed.stdout)["graph_id"] == service.graph_id
    assert len(service.created) == 1
    assert len(service.records) == 30


def test_checkpoint_rejects_mismatches_and_concurrent_use(
    tmp_path: Path, remote: tuple[FakeService, str]
) -> None:
    service, address = remote
    path = tmp_path / "dataset.jsonl"
    checkpoint_path = tmp_path / "checkpoint.sqlite"
    write_dataset(path, Config(products=3, warehouses=2, clients=2, vector_dimension=3))
    with load_data.open_checkpoint(checkpoint_path, resume=False) as checkpoint:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            with load_data.open_checkpoint(checkpoint_path, resume=True):
                pytest.fail("Checkpoint lock not enforced")
        with load_data.read_dataset(path) as dataset, grpc.insecure_channel(address) as channel:
            kwargs = {
                "api": nidx_pb2_grpc.NidxApiStub(channel),
                "lpg": nidx_pb2_grpc.NidxLPGStub(channel),
                "dataset": dataset,
                "tenant": None,
                "vectorset": "lpg-property-v1",
                "batch_size": 10,
                "timeout": 5,
                "checkpoint": checkpoint,
            }
            load_data.load(graph_id=service.graph_id, **kwargs)
            before = len(service.requests)
            with pytest.raises(ValueError, match="graph ID does not match"):
                load_data.load(graph_id=str(uuid4()), **kwargs)
            kwargs["vectorset"] = "other"
            with pytest.raises(ValueError, match="dataset or vectorset"):
                load_data.load(graph_id=None, **kwargs)
            kwargs["vectorset"] = "lpg-property-v1"
            dataset.digest = "different"
            with pytest.raises(ValueError, match="dataset or vectorset"):
                load_data.load(graph_id=None, **kwargs)
            assert len(service.requests) == before
    with pytest.raises(FileExistsError):
        with load_data.open_checkpoint(checkpoint_path, resume=False):
            pytest.fail("Existing checkpoint was overwritten")


def test_hard_interrupt_during_rpc_keeps_uncertain_checkpoint(
    tmp_path: Path, remote: tuple[FakeService, str]
) -> None:
    service, address = remote
    path = tmp_path / "dataset.jsonl"
    checkpoint_path = tmp_path / "checkpoint.sqlite"
    write_dataset(path, Config(products=3, warehouses=2, clients=2))
    entered, release, applied = Event(), Event(), Event()

    def pause_rpc() -> None:
        entered.set()
        assert release.wait(5)

    service.before_bulk = pause_rpc
    service.after_bulk = applied.set
    command = [
        sys.executable,
        "-m",
        "nidx.examples.lpg.load_data",
        "--input",
        str(path),
        "--address",
        address,
        "--graph-id",
        service.graph_id,
        "--checkpoint",
        str(checkpoint_path),
        "--batch-size",
        "4",
    ]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert entered.wait(10), "Loader did not send its first batch"
        process.kill()
        process.communicate(timeout=10)
        release.set()
        assert applied.wait(5), "Interrupted request did not finish in the test service"
    finally:
        release.set()
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=10)
    with load_data.open_checkpoint(checkpoint_path, resume=True) as checkpoint:
        assert checkpoint.state["status"] == "in_flight"
        assert checkpoint.state["acknowledged"] == 0
    assert len(service.records) == 4
    service.before_bulk = None
    service.after_bulk = None
    subprocess.run(
        [*command, "--resume", "--retry-uncertain"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert len(service.records) == 30


def test_persistent_snapshot_is_reused_and_rejects_modified_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "dataset.jsonl"
    checkpoint_path = tmp_path / "checkpoint.sqlite"
    write_dataset(path, Config(products=3, warehouses=2, clients=2, vector_dimension=3))
    with load_data.open_checkpoint(checkpoint_path, resume=False) as checkpoint:
        with load_data.read_dataset(path, checkpoint) as dataset:
            original = dataset.node("P00000001")
            digest = dataset.digest
    with load_data.open_checkpoint(checkpoint_path, resume=True) as checkpoint:

        def unexpected_parse(_: object) -> nidx_pb2.LpgRecord:
            pytest.fail("Resuming should not parse the JSONL again")

        monkeypatch.setattr(load_data, "to_record", unexpected_parse)
        with load_data.read_dataset(path, checkpoint) as dataset:
            assert dataset.digest == digest
            assert dataset.node("P00000001") == original
            assert (dataset.node_count, dataset.edge_count) == (7, 23)
        data = path.read_bytes()
        path.write_bytes(data.replace(b'"EUR"', b'"USD"', 1))
        assert path.stat().st_size == len(data)
        with pytest.raises(ValueError, match="input file was replaced or changed"):
            with load_data.read_dataset(path, checkpoint):
                pytest.fail("Modified file was accepted")


def test_interrupted_snapshot_can_be_rebuilt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "dataset.jsonl"
    checkpoint_path = tmp_path / "checkpoint.sqlite"
    write_dataset(path, Config(products=3, warehouses=2, clients=2))
    convert = load_data.to_record
    count = 0

    def interrupted(value: object) -> nidx_pb2.LpgRecord:
        nonlocal count
        count += 1
        if count == 3:
            raise RuntimeError("interrupted snapshot")
        return convert(value)

    monkeypatch.setattr(load_data, "to_record", interrupted)
    with load_data.open_checkpoint(checkpoint_path, resume=False) as checkpoint:
        with pytest.raises(RuntimeError, match="interrupted snapshot"):
            with load_data.read_dataset(path, checkpoint):
                pytest.fail("Expected interrupted snapshot")
    monkeypatch.setattr(load_data, "to_record", convert)
    with load_data.open_checkpoint(checkpoint_path, resume=True) as checkpoint:
        with load_data.read_dataset(path, checkpoint) as dataset:
            assert dataset.node_count + dataset.edge_count == 30
            assert checkpoint.state["snapshot_ready"] is True


def test_interrupted_bootstrap_snapshot_restores_resume_position(
    tmp_path: Path, remote: tuple[FakeService, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    service, address = remote
    path = tmp_path / "dataset.jsonl"
    checkpoint_path = tmp_path / "checkpoint.sqlite"
    write_dataset(path, Config(products=3, warehouses=2, clients=2))
    with load_data.read_dataset(path) as dataset:
        for index, record in enumerate(dataset.records(nidx_pb2.LPG_NODE)):
            if index == 4:
                break
            service.records[record.key.kind, record.key.id] = record
    command = [
        sys.executable,
        "-m",
        "nidx.examples.lpg.load_data",
        "--input",
        str(path),
        "--address",
        address,
        "--checkpoint",
        str(checkpoint_path),
    ]

    def interrupt(_: object) -> nidx_pb2.LpgRecord:
        raise RuntimeError("interrupted snapshot")

    with monkeypatch.context() as patch:
        patch.setattr(load_data, "to_record", interrupt)
        patch.setattr(
            sys,
            "argv",
            [
                *command[2:],
                "--graph-id",
                service.graph_id,
                "--expected-tenant",
                service.tenant,
                "--resume-after",
                "4",
                "--retry-uncertain",
                "--vectorset",
                "custom-vectorset",
            ],
        )
        with pytest.raises(SystemExit):
            load_data.main()
    assert not service.requests
    wrong_path = tmp_path / "other.jsonl"
    write_dataset(wrong_path, Config(products=3, warehouses=2, clients=2))
    wrong_command = command.copy()
    wrong_command[wrong_command.index("--input") + 1] = str(wrong_path)
    rejected = subprocess.run([*wrong_command, "--resume"], capture_output=True, text=True)
    assert rejected.returncode == 1
    assert "input file was replaced or changed" in rejected.stderr
    result = subprocess.run([*command, "--resume"], capture_output=True, text=True, check=True)
    assert '"resumed_from": 4' in result.stderr
    assert sum(len(request.records) for request in service.requests) == 26
    assert len(service.records) == 30
    with load_data.open_checkpoint(checkpoint_path, resume=True) as checkpoint:
        assert checkpoint.state["vectorset"] == "custom-vectorset"


def test_checkpoint_rejects_changed_file_before_remote_writes(
    tmp_path: Path, remote: tuple[FakeService, str]
) -> None:
    service, address = remote
    path = tmp_path / "dataset.jsonl"
    checkpoint_path = tmp_path / "checkpoint.sqlite"
    write_dataset(path, Config(products=3, warehouses=2, clients=2))
    with load_data.open_checkpoint(checkpoint_path, resume=False) as checkpoint:
        with load_data.read_dataset(path) as dataset, grpc.insecure_channel(address) as channel:
            load_data.load(
                nidx_pb2_grpc.NidxApiStub(channel),
                nidx_pb2_grpc.NidxLPGStub(channel),
                dataset,
                graph_id=service.graph_id,
                tenant=None,
                vectorset="lpg-property-v1",
                batch_size=10,
                timeout=5,
                checkpoint=checkpoint,
            )
    content = path.read_bytes()
    path.write_bytes(content.replace(b'"EUR"', b'"USD"', 1))
    assert len(path.read_bytes()) == len(content)
    before = len(service.requests)
    with load_data.open_checkpoint(checkpoint_path, resume=True) as checkpoint:
        with load_data.read_dataset(path) as dataset, grpc.insecure_channel(address) as channel:
            with pytest.raises(ValueError, match="dataset or vectorset"):
                load_data.load(
                    nidx_pb2_grpc.NidxApiStub(channel),
                    nidx_pb2_grpc.NidxLPGStub(channel),
                    dataset,
                    graph_id=None,
                    tenant=None,
                    vectorset="lpg-property-v1",
                    batch_size=10,
                    timeout=5,
                    checkpoint=checkpoint,
                )
    assert len(service.requests) == before


def test_uncertain_shard_creation_requires_explicit_verified_graph(
    tmp_path: Path, remote: tuple[FakeService, str]
) -> None:
    service, address = remote
    path = tmp_path / "dataset.jsonl"
    checkpoint_path = tmp_path / "checkpoint.sqlite"
    write_dataset(path, Config(products=3, warehouses=2, clients=2))
    with load_data.read_dataset(path) as dataset, grpc.insecure_channel(address) as channel:
        with load_data.open_checkpoint(checkpoint_path, resume=False) as checkpoint:
            checkpoint.save(
                digest=dataset.digest,
                nodes=dataset.node_count,
                edges=dataset.edge_count,
                vectorset="lpg-property-v1",
                tenant=service.tenant,
                graph_id=None,
                acknowledged=0,
                status="creating_shard",
            )
            api = nidx_pb2_grpc.NidxApiStub(channel)
            lpg = nidx_pb2_grpc.NidxLPGStub(channel)
            with pytest.raises(ValueError, match="outcome is uncertain"):
                load_data.load(
                    api,
                    lpg,
                    dataset,
                    graph_id=None,
                    tenant=None,
                    vectorset="lpg-property-v1",
                    batch_size=10,
                    timeout=5,
                    checkpoint=checkpoint,
                )
            with pytest.raises(ValueError, match="tenant ID does not match"):
                load_data.load(
                    api,
                    lpg,
                    dataset,
                    graph_id=service.graph_id,
                    tenant=None,
                    vectorset="lpg-property-v1",
                    batch_size=10,
                    timeout=5,
                    checkpoint=checkpoint,
                    expected_tenant=str(uuid4()),
                )
            assert not service.requests
            load_data.load(
                api,
                lpg,
                dataset,
                graph_id=service.graph_id,
                tenant=None,
                vectorset="lpg-property-v1",
                batch_size=10,
                timeout=5,
                checkpoint=checkpoint,
                expected_tenant=service.tenant,
            )
            assert checkpoint.state["graph_id"] == service.graph_id
            assert checkpoint.state["status"] == "ready"
            assert len(service.records) == 30


def test_checkpoint_bootstraps_legacy_acknowledged_count(
    tmp_path: Path, remote: tuple[FakeService, str]
) -> None:
    service, address = remote
    path = tmp_path / "dataset.jsonl"
    checkpoint_path = tmp_path / "checkpoint.sqlite"
    write_dataset(path, Config(products=3, warehouses=2, clients=2))
    with load_data.read_dataset(path) as dataset, grpc.insecure_channel(address) as channel:
        api = nidx_pb2_grpc.NidxApiStub(channel)
        lpg = nidx_pb2_grpc.NidxLPGStub(channel)
        for record in list(dataset.records(nidx_pb2.LPG_NODE))[:4]:
            service.records[record.key.kind, record.key.id] = record
        with load_data.open_checkpoint(checkpoint_path, resume=False) as checkpoint:
            with pytest.raises(ValueError, match="requires an existing graph and retry_uncertain"):
                load_data.load(
                    api,
                    lpg,
                    dataset,
                    graph_id=service.graph_id,
                    tenant=None,
                    vectorset="lpg-property-v1",
                    batch_size=10,
                    timeout=5,
                    checkpoint=checkpoint,
                    resume_after=4,
                )
            load_data.load(
                api,
                lpg,
                dataset,
                graph_id=service.graph_id,
                tenant=None,
                vectorset="lpg-property-v1",
                batch_size=10,
                timeout=5,
                checkpoint=checkpoint,
                resume_after=4,
                retry_uncertain=True,
            )
            assert checkpoint.state["tenant"] == service.tenant
            assert checkpoint.state["acknowledged"] == 30
        assert sum(len(request.records) for request in service.requests) == 26
        assert len(service.records) == 30


def test_cli_bootstraps_checkpoint_from_existing_progress(
    tmp_path: Path, remote: tuple[FakeService, str]
) -> None:
    service, address = remote
    path = tmp_path / "dataset.jsonl"
    checkpoint_path = tmp_path / "checkpoint.sqlite"
    write_dataset(path, Config(products=3, warehouses=2, clients=2))
    with load_data.read_dataset(path) as dataset:
        for record in list(dataset.records(nidx_pb2.LPG_NODE))[:4]:
            service.records[record.key.kind, record.key.id] = record
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "nidx.examples.lpg.load_data",
            "--input",
            str(path),
            "--address",
            address,
            "--graph-id",
            service.graph_id,
            "--expected-tenant",
            service.tenant,
            "--checkpoint",
            str(checkpoint_path),
            "--resume-after",
            "4",
            "--retry-uncertain",
            "--batch-size",
            "10",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(result.stdout)["tenant"] == service.tenant
    assert len(service.records) == 30
    assert sum(len(request.records) for request in service.requests) == 26
    with load_data.open_checkpoint(checkpoint_path, resume=True) as checkpoint:
        assert checkpoint.state["acknowledged"] == 30
        assert checkpoint.state["snapshot_ready"] is True


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
    dataset_factory: Callable[[int], load_data.Dataset],
    remote: tuple[FakeService, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    service, address = remote
    clock = 100

    def tick() -> None:
        nonlocal clock
        clock += 2

    service.before_bulk = tick
    monkeypatch.setattr(load_data, "perf_counter", lambda: clock)
    run_load(service, address, dataset_factory(0))
    progress = [
        json.loads(line)
        for line in capsys.readouterr().err.splitlines()
        if line.startswith("{") and json.loads(line).get("phase") == "loading"
    ]
    assert len(progress) == 2
    assert progress[0]["status"] == "running"
    assert progress[0]["acknowledged"] == 0
    assert progress[0]["elapsed_seconds"] == 0
    seen: set[tuple[int, str]] = set()
    submitted = 0
    for request in service.requests:
        seen.update((record.key.kind, record.key.id) for record in request.records)
        submitted += len(request.records)
    final = progress[-1]
    elapsed = len(service.requests) * 2
    assert final["status"] == "complete"
    assert final["acknowledged"] == len(seen) == final["total"] == 30
    assert final["percent"] == 100
    assert final["elapsed_seconds"] == elapsed
    assert final["records_per_second"] == round(30 / elapsed, 2)
    assert final["submitted_records"] == submitted == 30
    assert final["submitted_records_per_second"] == round(submitted / elapsed, 2)
    assert final["in_flight_records"] == 0


@pytest.mark.parametrize("phase", ["reading", "loading"])
def test_progress_heartbeat_during_blocking_work(
    dataset_factory: Callable[[int], load_data.Dataset],
    remote: tuple[FakeService, str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    phase: str,
) -> None:
    service, address = remote
    emitted = Event()
    report = load_data.Progress.report

    def record_heartbeat(self: load_data.Progress, status: str = "running") -> None:
        report(self, status)
        with self.lock:
            values = self.values.copy()
        if current_thread() is not main_thread() and values["phase"] == phase:
            if phase == "reading" or values.get("in_flight_records", 0):
                emitted.set()

    monkeypatch.setattr(load_data, "PROGRESS_INTERVAL", 0.01)
    monkeypatch.setattr(load_data.Progress, "report", record_heartbeat)
    if phase == "reading":
        to_record = load_data.to_record

        def blocked_conversion(value: object) -> nidx_pb2.LpgRecord:
            assert emitted.wait(2), "No progress while reading"
            return to_record(value)

        monkeypatch.setattr(load_data, "to_record", blocked_conversion)
    else:

        def blocked_rpc() -> None:
            assert emitted.wait(2), "No progress while waiting for BulkLoad"

        service.before_bulk = blocked_rpc
    run_load(service, address, dataset_factory(3))
    assert emitted.is_set()
    updates = [
        json.loads(line)
        for line in capsys.readouterr().err.splitlines()
        if json.loads(line).get("phase") == phase
    ]
    assert len([update for update in updates if update["status"] == "running"]) >= 2
    assert updates[-1]["status"] == "complete"


def test_reading_progress_counts_utf8_bytes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = tmp_path / "unicode.jsonl"
    path.write_bytes(
        (
            json.dumps(
                {"kind": "node", "id": "N1", "labels": ["Node"], "properties": {"x": "\u00e9"}},
                ensure_ascii=False,
            )
            + "\r\n"
        ).encode("utf-8")
    )
    with load_data.read_dataset(path):
        pass
    updates = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    final = next(
        update for update in updates if update["phase"] == "reading" and update["status"] == "complete"
    )
    assert final["bytes_read"] == final["total_bytes"] == path.stat().st_size
    assert final["percent"] == 100
    assert final["records"] == final["nodes"] == 1
    assert final["edges"] == 0
    assert [update["phase"] for update in updates if update["status"] == "complete"] == [
        "reading",
        "validating_endpoints",
        "building_indexes",
        "preparing_snapshot",
    ]


@pytest.mark.parametrize("create,wait", [(True, 5), (True, 0), (False, 5)])
def test_shard_wait(
    dataset_factory: Callable[[int], load_data.Dataset],
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
    run_load(service, address, dataset_factory(0), create=create, shard_wait=wait)
    assert sleeps == ([wait] if create and wait else [])
    assert len(service.records) == 30


@pytest.mark.parametrize("wait", [-1, float("nan"), float("inf")])
def test_invalid_shard_wait(
    dataset_factory: Callable[[int], load_data.Dataset],
    remote: tuple[FakeService, str],
    wait: float,
) -> None:
    service, address = remote
    with pytest.raises(ValueError, match="shard_wait must be finite and nonnegative"):
        run_load(service, address, dataset_factory(0), create=True, shard_wait=wait)
    assert not service.created
    assert not service.requests


def test_partial_failure_stops_loader(
    dataset_factory: Callable[[int], load_data.Dataset], remote: tuple[FakeService, str]
) -> None:
    service, address = remote
    service.fail_index = 1
    with pytest.raises(RuntimeError, match="test rejection; 0 previously acknowledged, 1 applied"):
        run_load(service, address, dataset_factory(0))
    assert len(service.requests) == 1
    assert len(service.records) == 1


def test_load_does_not_wait_for_search_visibility(
    dataset_factory: Callable[[int], load_data.Dataset], remote: tuple[FakeService, str]
) -> None:
    service, address = remote
    service.search_unavailable = True
    assert run_load(service, address, dataset_factory(3), create=True) == service.graph_id
    assert len(service.records) == 30
    assert service.read_requests == 0


def test_uncertain_write(
    dataset_factory: Callable[[int], load_data.Dataset],
    remote: tuple[FakeService, str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    service, address = remote
    service.transport_failure = True
    with pytest.raises(RuntimeError, match="test downstream unavailable"):
        run_load(service, address, dataset_factory(0))
    assert len(service.requests) == 1
    final = json.loads(capsys.readouterr().err.splitlines()[-1])
    assert final["phase"] == "loading"
    assert final["status"] == "failed"
    assert final["acknowledged"] == 0


def test_missing_vectorset_submits_no_writes(
    dataset_factory: Callable[[int], load_data.Dataset], remote: tuple[FakeService, str]
) -> None:
    service, address = remote
    service.vectorsets = []
    with pytest.raises(ValueError, match="Graph lacks vectorset"):
        run_load(service, address, dataset_factory(3))
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
        with load_data.read_dataset(path):
            pytest.fail("Invalid dataset was accepted")


def test_batch_byte_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        {"kind": "node", "id": str(index), "labels": ["Node"], "properties": {}} for index in range(3)
    ]
    path = tmp_path / "nodes.jsonl"
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    with load_data.read_dataset(path) as dataset:
        record = dataset.node("0")
        single_size = nidx_pb2.BulkLoadRequest(graph_id="graph", records=[record]).ByteSize()
        monkeypatch.setattr(load_data, "MAX_REQUEST_BYTES", single_size)
        requests = list(load_data.batches(dataset, "graph", 10))
        assert [len(request.records) for request in requests] == [1, 1, 1]
        monkeypatch.setattr(load_data, "MAX_REQUEST_BYTES", single_size - 1)
        with pytest.raises(ValueError, match="request limit"):
            list(load_data.batches(dataset, "graph", 10))


def test_endpoint_batch_limits(
    dataset_factory: Callable[[int], load_data.Dataset], monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset = dataset_factory(0)
    for batch_size in (1, 2):
        requests = list(load_data.batches(dataset, "graph", batch_size))
        assert all(len(request.records) <= batch_size for request in requests)
        assert sum(len(request.records) for request in requests) == 30
    edge = next(dataset.records(nidx_pb2.LPG_EDGE))
    endpoints = {endpoint: dataset.node_labels(endpoint) for endpoint in (edge.source, edge.target)}
    required_size = nidx_pb2.BulkLoadRequest(
        graph_id="graph", records=[edge], endpoint_labels=endpoints
    ).ByteSize()
    monkeypatch.setattr(load_data, "MAX_REQUEST_BYTES", required_size - 1)
    with pytest.raises(ValueError, match="request limit"):
        list(load_data.batches(dataset, "graph", 1000))


def test_oversized_request_fails_before_shard_creation(
    dataset_factory: Callable[[int], load_data.Dataset],
    remote: tuple[FakeService, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, address = remote
    monkeypatch.setattr(load_data, "MAX_REQUEST_BYTES", 1)
    with grpc.insecure_channel(address) as channel:
        with pytest.raises(ValueError, match="request limit"):
            load_data.load(
                nidx_pb2_grpc.NidxApiStub(channel),
                nidx_pb2_grpc.NidxLPGStub(channel),
                dataset_factory(0),
                graph_id=None,
                tenant=service.tenant,
                vectorset="lpg-property-v1",
                batch_size=2,
                timeout=5,
                shard_wait=0,
            )
    assert not service.created
    assert not service.requests


def test_self_loop_endpoint_is_referenced_once(tmp_path: Path) -> None:
    node = {"kind": "node", "id": "P1", "labels": ["Node"], "properties": {}}
    edge = {
        "kind": "edge",
        "id": "E1",
        "source": "P1",
        "target": "P1",
        "edge_type": "LINK",
        "properties": {},
    }
    path = tmp_path / "self-loop.jsonl"
    path.write_text(json.dumps(node) + "\n" + json.dumps(edge) + "\n")
    with load_data.read_dataset(path) as dataset:
        requests = list(load_data.batches(dataset, "graph", 1))
        assert len(requests) == 2
        assert list(requests[0].records) == [dataset.node("P1")]
        assert list(requests[1].records) == [next(dataset.records(nidx_pb2.LPG_EDGE))]
        assert list(requests[1].endpoint_labels) == ["P1"]
        assert list(requests[1].endpoint_labels["P1"].values) == ["Node"]


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
        identity = next(update for update in progress if "created" in update)
        assert identity["tenant"] == tenant
        if target == "tenant":
            assert tenant == service.tenant
        else:
            assert UUID(tenant).version == 4
    assert summary == {"graph_id": service.graph_id, "nodes": 7, "edges": 23}
    assert '"acknowledged": 30' in result.stderr
    path.write_text("not json\n")
    service.created.clear()
    result = subprocess.run(command, capture_output=True, text=True)
    assert result.returncode == 1
    assert f"{path}:1:" in result.stderr
    assert not service.created
