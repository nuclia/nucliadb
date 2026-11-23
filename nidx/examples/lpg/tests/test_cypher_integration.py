# Copyright (C) 2021 Bosutech XXI S.L.
#
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Exercise Cypher against a real NidxLPG service and an isolated test graph."""

import json
import os
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import grpc
import pytest
from nidx_protos import nidx_pb2, nidx_pb2_grpc, noderesources_pb2, nodewriter_pb2

from nidx.examples.lpg.cypher import Cypher
from nidx.examples.lpg.cypher import Path as GraphPath
from nidx.examples.lpg.load_data import to_record


def record(row: dict[str, object], name: str) -> nidx_pb2.LpgRecord:
    value = row[name]
    assert isinstance(value, nidx_pb2.LpgRecord)
    return value


def graph_path(row: dict[str, object]) -> GraphPath:
    value = row["path"]
    assert isinstance(value, GraphPath)
    return value


@pytest.fixture
def lpg_address(database_url: str, tmp_path: Path) -> Iterator[str]:
    root = Path(__file__).resolve().parents[4]
    build = subprocess.run(
        [
            "cargo",
            "build",
            "--manifest-path",
            str(root / "nidx/Cargo.toml"),
            "--no-default-features",
            "--example",
            "lpg_test_server",
            "--message-format=json",
        ],
        env={**os.environ, "SQLX_OFFLINE": "true"},
        check=True,
        capture_output=True,
        text=True,
        timeout=600,
    )
    executable = next(
        message["executable"]
        for line in build.stdout.splitlines()
        if (message := json.loads(line)).get("reason") == "compiler-artifact"
        and message["target"]["name"] == "lpg_test_server"
        and message.get("executable")
    )
    ready = tmp_path / "ready"
    log_path = tmp_path / "nidx.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [executable, str(ready)],
            env={**os.environ, "LPG_TEST_DATABASE_URL": database_url},
            stdin=subprocess.PIPE,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 30
            while not ready.exists():
                if process.poll() is not None:
                    raise RuntimeError(f"LPG test server failed:\n{log_path.read_text()}")
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"LPG test server did not start:\n{log_path.read_text()}")
                time.sleep(0.1)
            address = ready.read_text()
            with grpc.insecure_channel(address) as channel:
                grpc.channel_ready_future(channel).result(timeout=10)
            yield address
        finally:
            assert process.stdin is not None
            process.stdin.close()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                raise TimeoutError(f"LPG test server did not stop:\n{log_path.read_text()}")
            if process.returncode:
                raise RuntimeError(
                    f"LPG test server exited with {process.returncode}:\n{log_path.read_text()}"
                )


@pytest.fixture
def lpg_graph(lpg_address: str) -> Iterator[tuple[nidx_pb2_grpc.NidxLPGStub, str]]:
    with grpc.insecure_channel(lpg_address) as channel:
        grpc.channel_ready_future(channel).result(timeout=10)
        api = nidx_pb2_grpc.NidxApiStub(channel)
        graph_id = api.NewShard(
            nodewriter_pb2.NewShardRequest(
                kbid=str(uuid4()),
                vectorsets_configs={"default": nodewriter_pb2.VectorIndexConfig(vector_dimension=3)},
            ),
            timeout=30,
        ).id
        try:
            yield nidx_pb2_grpc.NidxLPGStub(channel), graph_id
        finally:
            api.DeleteShard(noderesources_pb2.ShardId(id=graph_id), timeout=30)


@pytest.fixture
def cypher(lpg_graph: tuple[nidx_pb2_grpc.NidxLPGStub, str]) -> Cypher:
    client, graph_id = lpg_graph
    nodes = [
        (
            "p1",
            ["Person", "Entity"],
            {
                "name": "Alice",
                "email": "alice@example.com",
                "country": "ES",
                "age": 34,
                "active": True,
                "rating": 4.5,
            },
        ),
        (
            "p2",
            ["Person", "Entity"],
            {
                "name": "Bob",
                "country": "ES",
                "age": 42,
                "active": False,
                "rating": 4.0,
            },
        ),
        (
            "p3",
            ["Person"],
            {
                "name": "Cara",
                "country": "FR",
                "age": 29,
                "active": True,
                "rating": 4.5,
            },
        ),
        (
            "c1",
            ["Company", "Entity"],
            {
                "name": "Acme",
                "country": "ES",
                "founded": 2001,
                "public": True,
                "revenue": 12.5,
            },
        ),
        (
            "c2",
            ["Company"],
            {
                "name": "Other",
                "country": "FR",
                "founded": 2015,
                "public": False,
                "revenue": 3.2,
            },
        ),
        (
            "d1",
            ["Document"],
            {
                "title": "Report",
                "language": "en",
                "pages": 12,
                "published": True,
                "version": 1.5,
            },
        ),
    ]
    edges = [
        ("w1", "p1", "c1", "WORKS_WITH", {"role": "engineer", "since": 2020, "primary": True}),
        ("w2", "p1", "c1", "WORKS_WITH", {"role": "advisor", "since": 2023, "primary": False}),
        ("r1", "c1", "p2", "REPORTS_TO", {"since": 2021, "direct": True}),
        ("r2", "p2", "p3", "REPORTS_TO", {"since": 2024, "direct": False}),
        ("pub", "d1", "c1", "PUBLISHED_BY", {"year": 2025, "reviewed": True}),
        ("f1", "p1", "c1", "WORKS_FOR", {"hours": 40, "remote": True}),
        ("f2", "p2", "c1", "WORKS_FOR", {"hours": 20, "remote": False}),
        ("f3", "p1", "c2", "WORKS_FOR", {"hours": 8, "remote": True}),
        ("k1", "p1", "p2", "KNOWS", {"since": 2019, "strength": 0.9}),
        ("k2", "p2", "p3", "KNOWS", {"since": 2022, "strength": 0.7}),
        ("loop", "p1", "p1", "KNOWS", {"since": 1991, "strength": 1.0}),
        (
            "e1",
            "p1",
            "c1",
            "RELATED_TO",
            {
                "confidence": 0.8,
                "origin": "curated",
                "evidence_count": 3,
                "verified": True,
            },
        ),
        (
            "e2",
            "p2",
            "c1",
            "RELATED_TO",
            {
                "confidence": 0.5,
                "origin": "inferred",
                "evidence_count": 1,
                "verified": False,
            },
        ),
        (
            "e3",
            "p2",
            "p1",
            "RELATED_TO",
            {
                "confidence": 1,
                "origin": "curated",
                "evidence_count": 2,
                "verified": True,
            },
        ),
        (
            "e4",
            "p1",
            "c1",
            "RELATED_TO",
            {
                "origin": "curated",
                "evidence_count": 3,
                "verified": True,
            },
        ),
    ]
    records = [
        to_record({"kind": "node", "id": id, "labels": labels, "properties": properties})
        for id, labels, properties in nodes
    ] + [
        to_record(
            {
                "kind": "edge",
                "id": id,
                "source": source,
                "target": target,
                "edge_type": kind,
                "properties": properties,
            }
        )
        for id, source, target, kind, properties in edges
    ]
    request = nidx_pb2.BulkLoadRequest(graph_id=graph_id, records=records)
    deadline = time.monotonic() + 60
    while True:
        try:
            response = client.BulkLoad(request, timeout=40)
            break
        except grpc.RpcError as error:
            # The search indexes may not have synchronized at startup.
            if (
                error.code() != grpc.StatusCode.INTERNAL
                or "text searcher is required when text prefilter is needed"
                not in (error.details() or "")
                or time.monotonic() >= deadline
            ):
                raise
            time.sleep(0.1)
    assert len(response.results) == len(records)
    assert all(result.status == nidx_pb2.LPG_OPERATION_STATUS_APPLIED for result in response.results)

    return Cypher(client, graph_id, timeout=30)


def test_seven_queries_against_real_lpg(cypher: Cypher) -> None:
    lookup = """
        MATCH (p:Person {email: $email, country: $country, age: $age, active: $active, rating: $rating})
        RETURN p
    """
    parameters = {
        "email": "alice@example.com",
        "country": "ES",
        "age": 34,
        "active": True,
        "rating": 4.5,
    }
    found = cypher.execute(lookup, parameters)
    assert [record(row, "p").key.id for row in found] == ["p1"]
    assert record(found[0], "p").properties["name"].text == "Alice"
    for key, mismatch in {
        "email": "missing",
        "country": "FR",
        "age": 42,
        "active": False,
        "rating": 4.0,
    }.items():
        assert cypher.execute(lookup, {**parameters, key: mismatch}) == []

    neighborhood = cypher.execute(
        """
        MATCH (p:Person {name: $name})-[r]-(neighbor)
        RETURN p, r, neighbor
        LIMIT 10
        """,
        {"name": "Alice"},
    )
    assert {(record(row, "r").key.id, record(row, "neighbor").key.id) for row in neighborhood} == {
        ("w1", "c1"),
        ("w2", "c1"),
        ("f1", "c1"),
        ("f3", "c2"),
        ("k1", "p2"),
        ("loop", "p1"),
        ("e1", "c1"),
        ("e3", "p2"),
        ("e4", "c1"),
    }

    paths = cypher.execute(
        """
        MATCH path = (p:Person {name: $name})-[:WORKS_WITH|REPORTS_TO*1..3]->(other)
        RETURN path
        LIMIT 10
        """,
        {"name": "Alice"},
    )
    assert {tuple(edge.key.id for edge in graph_path(row).relationships) for row in paths} == {
        ("w1",),
        ("w2",),
        ("w1", "r1"),
        ("w2", "r1"),
        ("w1", "r1", "r2"),
        ("w2", "r1", "r2"),
    }
    assert all(graph_path(row).nodes[0].key.id == "p1" for row in paths)

    joined = cypher.execute(
        """
        MATCH (p:Person)-[:WORKS_WITH]->(c:Company)<-[:PUBLISHED_BY]-(d:Document)
        WHERE p.name = $name AND p.active = $active
          AND c.public = $public AND d.pages >= $minimum_pages
        RETURN p, c, d
        """,
        {"name": "Alice", "active": True, "public": True, "minimum_pages": 10},
    )
    assert [
        (record(row, "p").key.id, record(row, "c").key.id, record(row, "d").key.id) for row in joined
    ] == [
        ("p1", "c1", "d1"),
        ("p1", "c1", "d1"),
    ]
    related = cypher.execute(
        """
        MATCH (a:Entity)-[r:RELATED_TO]->(b:Entity)
        WHERE r.confidence >= $minimum
        RETURN a, r, b
        """,
        {"minimum": 0.8},
    )
    assert {
        (record(row, "a").key.id, record(row, "r").key.id, record(row, "b").key.id) for row in related
    } == {
        ("p1", "e1", "c1"),
        ("p2", "e3", "p1"),
    }
    typed_relationship_query = """
        MATCH (a:Entity)-[r:RELATED_TO]->(b:Entity)
        WHERE r.confidence >= $minimum AND r.origin = $origin
          AND r.evidence_count >= $evidence_count AND r.verified = $verified
          AND a.active = $active AND b.country = $country
        RETURN a, r, b
    """
    relationship_parameters = {
        "minimum": 0.8,
        "origin": "curated",
        "evidence_count": 3,
        "verified": True,
        "active": True,
        "country": "ES",
    }
    typed_relationships = cypher.execute(typed_relationship_query, relationship_parameters)
    assert [record(row, "r").key.id for row in typed_relationships] == ["e1"]
    for key, mismatch in {
        "minimum": 0.9,
        "origin": "inferred",
        "evidence_count": 4,
        "verified": False,
        "active": False,
        "country": "FR",
    }.items():
        assert cypher.execute(typed_relationship_query, {**relationship_parameters, key: mismatch}) == []
    assert cypher.execute(
        """
        MATCH (c:Company)<-[:WORKS_FOR]-(p:Person)
        RETURN c.name, count(p) AS employees
        ORDER BY employees DESC
        LIMIT 2
        """,
        {},
    ) == [
        {"c.name": "Acme", "employees": 2},
        {"c.name": "Other", "employees": 1},
    ]
    shortest = cypher.execute(
        """
        MATCH path = shortestPath(
          (a:Person {name: $start, active: $active})-[:KNOWS*..2]-
          (b:Person {name: $end, country: $country, rating: $rating})
        )
        RETURN path
        """,
        {"start": "Alice", "active": True, "end": "Cara", "country": "FR", "rating": 4.5},
    )
    assert len(shortest) == 1
    assert [node.key.id for node in graph_path(shortest[0]).nodes] == ["p1", "p2", "p3"]
    assert [edge.key.id for edge in graph_path(shortest[0]).relationships] == ["k1", "k2"]


def expansion_rows(response: nidx_pb2.ExpandResponse) -> set[tuple[str, str, str]]:
    rows = {
        (
            match.node_id,
            response.edges[match.edge_index].key.id,
            response.neighbors[match.neighbor_index].key.id,
        )
        for match in response.matches
    }
    assert len(rows) == len(response.matches)
    return rows


def test_expand_batches_directions_and_filters(
    cypher: Cypher, lpg_graph: tuple[nidx_pb2_grpc.NidxLPGStub, str]
) -> None:
    client, graph_id = lpg_graph[0], cypher.graph_id
    outgoing = client.Expand(
        nidx_pb2.ExpandRequest(
            graph_id=graph_id,
            node_ids=["p1", "p2", "p1"],
            direction=nidx_pb2.LPG_OUTGOING,
            edge_types=["WORKS_FOR"],
        ),
        timeout=30,
    )
    assert expansion_rows(outgoing) == {
        ("p1", "f1", "c1"),
        ("p1", "f3", "c2"),
        ("p2", "f2", "c1"),
    }
    assert outgoing.examined_edges == 3
    assert len(outgoing.neighbors) == 2
    incoming = client.Expand(
        nidx_pb2.ExpandRequest(
            graph_id=graph_id,
            node_ids=["c1"],
            direction=nidx_pb2.LPG_INCOMING,
            edge_types=["WORKS_FOR"],
        ),
        timeout=30,
    )
    assert expansion_rows(incoming) == {("c1", "f1", "p1"), ("c1", "f2", "p2")}
    assert all(edge.target == "c1" for edge in incoming.edges)
    both = client.Expand(
        nidx_pb2.ExpandRequest(
            graph_id=graph_id,
            node_ids=["p1", "p2"],
            direction=nidx_pb2.LPG_BOTH,
            edge_types=["KNOWS"],
        ),
        timeout=30,
    )
    assert expansion_rows(both) == {
        ("p1", "k1", "p2"),
        ("p2", "k1", "p1"),
        ("p2", "k2", "p3"),
        ("p1", "loop", "p1"),
    }
    assert both.examined_edges == 3
    assert len(both.edges) == 3
    parallel = client.Expand(
        nidx_pb2.ExpandRequest(
            graph_id=graph_id,
            node_ids=["p1"],
            direction=nidx_pb2.LPG_OUTGOING,
            edge_types=["WORKS_WITH", "KNOWS", "WORKS_WITH"],
        ),
        timeout=30,
    )
    assert expansion_rows(parallel) == {
        ("p1", "w1", "c1"),
        ("p1", "w2", "c1"),
        ("p1", "k1", "p2"),
        ("p1", "loop", "p1"),
    }
    filtered = client.Expand(
        nidx_pb2.ExpandRequest(
            graph_id=graph_id,
            node_ids=["p1", "p2"],
            direction=nidx_pb2.LPG_OUTGOING,
            edge_types=["WORKS_FOR"],
            edge_filters=[
                nidx_pb2.LpgPropertyFilter(property="remote", boolean=True),
                nidx_pb2.LpgPropertyFilter(
                    property="hours", integer_range=nidx_pb2.LpgIntegerRange(lower=20)
                ),
            ],
            neighbor_label="Company",
            neighbor_filters=[
                nidx_pb2.LpgPropertyFilter(property="country", text="ES"),
                nidx_pb2.LpgPropertyFilter(property="public", boolean=True),
                nidx_pb2.LpgPropertyFilter(property="founded", integer=2001),
                nidx_pb2.LpgPropertyFilter(
                    property="revenue", float_range=nidx_pb2.LpgFloatRange(lower=12.0, upper=13.0)
                ),
            ],
        ),
        timeout=30,
    )
    assert expansion_rows(filtered) == {("p1", "f1", "c1")}
    assert filtered.examined_edges == 1
    neighbor_filtered = client.Expand(
        nidx_pb2.ExpandRequest(
            graph_id=graph_id,
            node_ids=["p1", "p2"],
            direction=nidx_pb2.LPG_OUTGOING,
            edge_types=["WORKS_FOR"],
            neighbor_filters=[
                nidx_pb2.LpgPropertyFilter(property="country", text="FR"),
            ],
        ),
        timeout=30,
    )
    assert expansion_rows(neighbor_filtered) == {("p1", "f3", "c2")}
    assert neighbor_filtered.examined_edges == 3
    empty = client.Expand(
        nidx_pb2.ExpandRequest(
            graph_id=graph_id,
            node_ids=["d1"],
            direction=nidx_pb2.LPG_INCOMING,
        ),
        timeout=30,
    )
    assert not empty.matches
    untyped = client.Expand(
        nidx_pb2.ExpandRequest(graph_id=graph_id, node_ids=["p1"], direction=nidx_pb2.LPG_BOTH),
        timeout=30,
    )
    assert {match[1] for match in expansion_rows(untyped)} == {
        "w1",
        "w2",
        "f1",
        "f3",
        "k1",
        "loop",
        "e1",
        "e3",
        "e4",
    }
    no_neighbors = client.Expand(
        nidx_pb2.ExpandRequest(
            graph_id=graph_id,
            node_ids=["p1", "p2"],
            direction=nidx_pb2.LPG_OUTGOING,
            edge_types=["WORKS_FOR"],
            neighbor_label="Person",
        ),
        timeout=30,
    )
    assert not no_neighbors.matches
    assert no_neighbors.examined_edges == 3


def test_expand_limits_and_invalid_inputs(
    cypher: Cypher, lpg_graph: tuple[nidx_pb2_grpc.NidxLPGStub, str]
) -> None:
    client, graph_id = lpg_graph[0], cypher.graph_id
    request = nidx_pb2.ExpandRequest(
        graph_id=graph_id,
        node_ids=["p1", "p2"],
        direction=nidx_pb2.LPG_OUTGOING,
        edge_types=["WORKS_FOR"],
        candidate_limit=3,
        max_results=3,
    )
    assert len(client.Expand(request, timeout=30).matches) == 3
    for field in ("candidate_limit", "max_results"):
        bounded = nidx_pb2.ExpandRequest()
        bounded.CopyFrom(request)
        setattr(bounded, field, 2)
        with pytest.raises(grpc.RpcError) as error:
            client.Expand(bounded, timeout=30)
        assert error.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    request.candidate_limit = 2
    request.neighbor_label = "DoesNotExist"
    with pytest.raises(grpc.RpcError) as error:
        client.Expand(request, timeout=30)
    assert error.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    for invalid in [
        nidx_pb2.ExpandRequest(node_ids=["p1"]),
        nidx_pb2.ExpandRequest(direction=nidx_pb2.LPG_BOTH),
        nidx_pb2.ExpandRequest(node_ids=[""], direction=nidx_pb2.LPG_BOTH),
        nidx_pb2.ExpandRequest(node_ids=["p1"], direction=nidx_pb2.LPG_BOTH, edge_types=[""]),
        nidx_pb2.ExpandRequest(node_ids=["p1"], direction=nidx_pb2.LPG_BOTH, neighbor_label=""),
        nidx_pb2.ExpandRequest(node_ids=["p1"], direction=nidx_pb2.LPG_BOTH, candidate_limit=10001),
        nidx_pb2.ExpandRequest(node_ids=["p1"], direction=nidx_pb2.LPG_BOTH, max_results=10001),
        nidx_pb2.ExpandRequest(
            node_ids=["p1"],
            direction=nidx_pb2.LPG_BOTH,
            neighbor_filters=[nidx_pb2.LpgPropertyFilter(property="age")],
        ),
        nidx_pb2.ExpandRequest(
            node_ids=["p1"],
            direction=nidx_pb2.LPG_BOTH,
            edge_filters=[nidx_pb2.LpgPropertyFilter(property="since", float=float("nan"))],
        ),
    ]:
        invalid.graph_id = graph_id
        with pytest.raises(grpc.RpcError) as error:
            client.Expand(invalid, timeout=30)
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    with pytest.raises(grpc.RpcError) as error:
        client.Expand(
            nidx_pb2.ExpandRequest(
                graph_id=graph_id,
                node_ids=["missing"],
                direction=nidx_pb2.LPG_BOTH,
            ),
            timeout=30,
        )
    assert error.value.code() == grpc.StatusCode.NOT_FOUND
    wide = nidx_pb2.ExpandRequest(
        graph_id=graph_id,
        node_ids=["p1", "p2"],
        direction=nidx_pb2.LPG_BOTH,
        edge_types=["KNOWS"],
        candidate_limit=3,
        max_results=3,
    )
    with pytest.raises(grpc.RpcError) as error:
        client.Expand(wide, timeout=30)
    assert error.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED

    endpoints = client.GetRecords(
        nidx_pb2.GetRecordsRequest(
            graph_id=graph_id,
            keys=[
                nidx_pb2.LpgRecordKey(kind=nidx_pb2.LPG_NODE, id="p1"),
                nidx_pb2.LpgRecordKey(kind=nidx_pb2.LPG_NODE, id="d1"),
            ],
        ),
        timeout=30,
    ).records
    endpoints[1].properties["payload"].text = "x" * 1_600_000
    large_edge = to_record(
        {
            "kind": "edge",
            "id": "large",
            "source": "p1",
            "target": "d1",
            "edge_type": "LARGE",
            "properties": {"payload": "x" * 1_600_000},
        }
    )
    loaded = client.BulkLoad(
        nidx_pb2.BulkLoadRequest(graph_id=graph_id, records=[*endpoints, large_edge]),
        timeout=90,
    )
    assert all(result.status == nidx_pb2.LPG_OPERATION_STATUS_APPLIED for result in loaded.results)
    with pytest.raises(grpc.RpcError) as error:
        client.Expand(
            nidx_pb2.ExpandRequest(
                graph_id=graph_id,
                node_ids=["p1"],
                direction=nidx_pb2.LPG_OUTGOING,
                edge_types=["LARGE"],
            ),
            timeout=30,
        )
    assert error.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    assert "response byte limit" in (error.value.details() or "")
