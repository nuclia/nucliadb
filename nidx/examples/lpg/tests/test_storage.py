import os
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from nidx.examples.lpg.storage import EmbeddedNidx, Graph, PropertyFilter, Record


@pytest.fixture
def database_url() -> Iterator[str]:
    name = f"nidx-lpg-test-{uuid4().hex}"
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            name,
            "-e",
            "POSTGRES_HOST_AUTH_METHOD=trust",
            "-p",
            "127.0.0.1::5432",
            "postgres:16-alpine",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    try:
        port = (
            subprocess.run(
                ["docker", "port", name, "5432/tcp"],
                check=True,
                capture_output=True,
                text=True,
                timeout=5,
            )
            .stdout.strip()
            .rsplit(":", 1)[1]
        )
        url = f"postgresql://postgres@127.0.0.1:{port}/postgres"
        deadline = time.monotonic() + 30
        while True:
            try:
                with psycopg.connect(url, connect_timeout=1) as connection:
                    connection.execute("SELECT 1")
                break
            except psycopg.OperationalError as error:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Dedicated LPG test PostgreSQL did not become ready") from error
                time.sleep(0.1)
        yield url
    finally:
        subprocess.run(["docker", "rm", "-f", name], check=True, capture_output=True, timeout=15)


@pytest.fixture
def nidx(database_url: str, tmp_path: Path) -> Iterator[EmbeddedNidx]:
    instance = EmbeddedNidx(database_url, tmp_path)
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

    changed = graph.upsert_edge("E1", "P1", "SUPPLIES", "W2", {"quantity": 5})
    assert graph.get("edge", "E1") == changed
    assert graph.topology() == [changed, second]
    assert ids(graph.topology(PropertyFilter("old", "eq", value=True))) == []
    assert ids(graph.find("edge", PropertyFilter("quantity", "eq", value=10))) == []
    graph.delete_edge("E1")
    assert graph.get("edge", "E1") is None
    assert graph.find("edge") == [second]
    assert graph.topology() == [second]
    graph.delete_edge("E1")  # Repeated deletion is safe.


def test_node_replacement_and_tenant_isolation(nidx: EmbeddedNidx) -> None:
    first = Graph.create(nidx, str(uuid4()))
    second = Graph.create(nidx, str(uuid4()))
    first.upsert_node("P1", ["Product"], {"price": 150, "old": "remove me"})
    other = second.upsert_node("P1", ["Client"], {"price": 999})
    updated = first.upsert_node("P1", ["Discounted"], {"price": 80})
    assert first.find("node") == [updated]
    assert first.find("node", PropertyFilter("old", "eq", value="remove me")) == []
    assert first.find("node", PropertyFilter("price", "eq", value=150)) == []
    assert second.find("node") == [other]
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
        graph = Graph.create(original, str(uuid4()))
        product = graph.upsert_node("P1", ["Product"], {"price": 150})
        graph.upsert_node("W1", ["Warehouse"], {})
        edge = graph.upsert_edge("E1", "P1", "STOCKED_AT", "W1", {"quantity": 10})
        tenant, shard_id = graph.tenant, graph.shard_id
    finally:
        original.close()

    restarted = EmbeddedNidx(database_url, tmp_path)
    try:
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
