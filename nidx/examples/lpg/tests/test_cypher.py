# Copyright (C) 2021 Bosutech XXI S.L.
#
# SPDX-License-Identifier: AGPL-3.0-or-later
import json
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
import pytest
from nidx_protos import nidx_pb2, nidx_pb2_grpc

from nidx.examples.lpg.cypher import Cypher, QueryLimitExceeded, UnsupportedQuery, parse
from nidx.examples.lpg.cypher import Path as GraphPath
from nidx.examples.lpg.load_data import to_record


class Service(nidx_pb2_grpc.NidxLPGServicer):
    def __init__(self) -> None:
        self.requests: list[nidx_pb2.FindRecordsRequest] = []
        self.repeat_cursor = False
        self.fail_find = False
        self.expand_error: grpc.StatusCode | None = None
        self.expand_requests: list[nidx_pb2.ExpandRequest] = []
        self.path_requests: list[nidx_pb2.ShortestPathRequest] = []
        self.records = [
            to_record(row)
            for row in [
                {
                    "kind": "node",
                    "id": "p1",
                    "labels": ["Person", "Entity"],
                    "properties": {"name": "Alice", "email": "a", "age": 1},
                },
                {
                    "kind": "node",
                    "id": "p2",
                    "labels": ["Person", "Entity"],
                    "properties": {"name": "Bob", "age": 1.0},
                },
                {"kind": "node", "id": "p3", "labels": ["Person"], "properties": {"name": "Cara"}},
                {
                    "kind": "node",
                    "id": "c1",
                    "labels": ["Company", "Entity"],
                    "properties": {"name": "Acme"},
                },
                {"kind": "node", "id": "c2", "labels": ["Company"], "properties": {"name": "Acme"}},
                {"kind": "node", "id": "d", "labels": ["Document"], "properties": {}},
                *[
                    {
                        "kind": "edge",
                        "id": id,
                        "source": source,
                        "target": target,
                        "edge_type": edge_type,
                        "properties": properties,
                    }
                    for id, source, target, edge_type, properties in [
                        ("w1", "p1", "c1", "WORKS_WITH", {}),
                        ("w2", "p2", "c1", "WORKS_WITH", {}),
                        ("r1", "c1", "p2", "REPORTS_TO", {}),
                        ("r2", "p2", "p1", "REPORTS_TO", {}),
                        ("pub", "d", "c1", "PUBLISHED_BY", {}),
                        ("f1", "p1", "c1", "WORKS_FOR", {}),
                        ("f2", "p2", "c1", "WORKS_FOR", {}),
                        ("f3", "p1", "c2", "WORKS_FOR", {}),
                        ("k1", "p1", "p2", "KNOWS", {}),
                        ("k2", "p1", "p2", "KNOWS", {}),
                        ("k3", "p2", "p3", "KNOWS", {}),
                        ("k4", "p1", "p1", "KNOWS", {}),
                        ("e1", "p1", "c1", "RELATED_TO", {"confidence": 0.8}),
                        ("e2", "p2", "c1", "RELATED_TO", {"confidence": 1}),
                        ("e3", "p1", "c1", "RELATED_TO", {"confidence": True}),
                        ("e4", "p1", "c1", "RELATED_TO", {}),
                    ]
                ],
            ]
        ]

    def FindRecords(
        self, request: nidx_pb2.FindRecordsRequest, context: grpc.ServicerContext
    ) -> nidx_pb2.FindRecordsResponse:
        if self.fail_find:
            context.abort(grpc.StatusCode.UNAVAILABLE, "search unavailable")
        self.requests.append(request)
        assert request.graph_id == "graph"
        records = [record for record in self.records if record.key.kind == request.kind]
        for field in ("label", "source", "target", "incident_node", "edge_type"):
            if request.HasField(field):
                value = getattr(request, field)
                if field == "label":
                    records = [record for record in records if value in record.labels]
                elif field == "incident_node":
                    records = [record for record in records if value in (record.source, record.target)]
                else:
                    records = [record for record in records if getattr(record, field) == value]
        for predicate in request.filters:
            kind = predicate.WhichOneof("predicate")
            records = [
                record
                for record in records
                if predicate.property in record.properties
                and record.properties[predicate.property].WhichOneof("value") == kind
                and getattr(record.properties[predicate.property], kind) == getattr(predicate, kind)
            ]
        offset = int(request.page_token or 0)
        end = min(offset + min(request.page_size, 2), len(records))
        return nidx_pb2.FindRecordsResponse(
            records=records[offset:end],
            next_page_token="1" if self.repeat_cursor else str(end) if end < len(records) else "",
        )

    def _incident(self, node: str, direction: int, edge_types: list[str]) -> list[tuple]:
        found = []
        for edge in self.records:
            if edge.key.kind != nidx_pb2.LPG_EDGE or (edge_types and edge.edge_type not in edge_types):
                continue
            outgoing = edge.source == node and direction in (nidx_pb2.LPG_OUTGOING, nidx_pb2.LPG_BOTH)
            incoming = edge.target == node and direction in (nidx_pb2.LPG_INCOMING, nidx_pb2.LPG_BOTH)
            if outgoing:
                found.append((edge, edge.target))
            if incoming and not (outgoing and edge.source == edge.target):
                found.append((edge, edge.source))
        return found

    def Expand(
        self, request: nidx_pb2.ExpandRequest, context: grpc.ServicerContext
    ) -> nidx_pb2.ExpandResponse:
        if self.expand_error:
            context.abort(self.expand_error, "expand failed")
        self.expand_requests.append(request)
        nodes = {record.key.id: record for record in self.records if record.key.kind == nidx_pb2.LPG_NODE}
        response = nidx_pb2.ExpandResponse()
        for node_id in dict.fromkeys(request.node_ids):
            for edge, neighbor_id in self._incident(node_id, request.direction, list(request.edge_types)):
                neighbor = nodes[neighbor_id]
                if request.HasField("neighbor_label") and request.neighbor_label not in neighbor.labels:
                    continue
                response.edges.append(edge)
                response.neighbors.append(neighbor)
                response.matches.append(
                    nidx_pb2.ExpandMatch(
                        node_id=node_id,
                        edge_index=len(response.edges) - 1,
                        neighbor_index=len(response.neighbors) - 1,
                    )
                )
        return response

    def ShortestPath(
        self, request: nidx_pb2.ShortestPathRequest, context: grpc.ServicerContext
    ) -> nidx_pb2.ShortestPathResponse:
        self.path_requests.append(request)
        nodes = {record.key.id: record for record in self.records if record.key.kind == nidx_pb2.LPG_NODE}
        queue = [(request.source, [nodes[request.source]], [])]
        seen = {request.source}
        while queue:
            node, path_nodes, path_edges = queue.pop(0)
            if len(path_edges) == request.max_hops:
                continue
            for edge, neighbor in self._incident(node, request.direction, list(request.edge_types)):
                if neighbor in seen:
                    continue
                seen.add(neighbor)
                step = (neighbor, [*path_nodes, nodes[neighbor]], [*path_edges, edge])
                if neighbor == request.target:
                    return nidx_pb2.ShortestPathResponse(found=True, nodes=step[1], edges=step[2])
                queue.append(step)
        return nidx_pb2.ShortestPathResponse()

    def GetRecords(
        self, request: nidx_pb2.GetRecordsRequest, context: grpc.ServicerContext
    ) -> nidx_pb2.GetRecordsResponse:
        assert request.graph_id == "graph"
        return nidx_pb2.GetRecordsResponse(
            records=[record for key in request.keys for record in self.records if record.key == key]
        )


@pytest.fixture
def graph():
    service = Service()
    with ThreadPoolExecutor(max_workers=2) as pool:
        server = grpc.server(pool)
        nidx_pb2_grpc.add_NidxLPGServicer_to_server(service, server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                yield Cypher(nidx_pb2_grpc.NidxLPGStub(channel), "graph"), service
        finally:
            server.stop(0).wait()


@pytest.fixture
def queries() -> list[str]:
    text = (Path(__file__).parent.parent / "queries.md").read_text()
    return [query.strip() for query in re.findall(r"(?ms)^MATCH .*?(?=\n\n|\Z)", text)]


def ids(rows: list[dict[str, object]], name: str) -> list[str]:
    return [row[name].key.id for row in rows]


def test_all_seven_supplied_query_shapes(graph, queries) -> None:
    client, service = graph
    assert len(queries) == 7
    assert [parse(query)[0] for query in queries] == [
        "lookup",
        "neighborhood",
        "traverse",
        "join",
        "edge_filter",
        "aggregate",
        "shortest",
    ]
    assert ids(client.execute(queries[0], {"email": "a"}), "p") == ["p1"]
    assert any(request.filters and request.filters[0].text == "a" for request in service.requests)

    neighbors = client.execute(queries[1], {"name": "Alice"})
    assert {"k1", "k2", "k4", "e1", "e3", "e4"}.issubset(set(ids(neighbors, "r")))
    assert ids([row for row in neighbors if row["r"].key.id == "k4"], "neighbor") == ["p1"]
    assert service.expand_requests[-1].direction == nidx_pb2.LPG_BOTH

    paths = client.execute(queries[2], {"name": "Alice"})
    paths = [row["path"] for row in paths]
    assert all(isinstance(path, GraphPath) and 1 <= len(path.relationships) <= 3 for path in paths)
    assert any([edge.key.id for edge in path.relationships] == ["w1", "r1", "r2"] for path in paths)

    joined = client.execute(queries[3], {"name": "Alice"})
    assert [(row["p"].key.id, row["c"].key.id, row["d"].key.id) for row in joined] == [("p1", "c1", "d")]
    related = client.execute(queries[4], {"minimum": 0.8})
    assert set(ids(related, "r")) == {"e1", "e2"}
    assert client.execute(queries[5], {}) == [{"c.name": "Acme", "employees": 3}]
    shortest = client.execute(queries[6], {"start": "Alice", "end": "Cara"})
    assert len(shortest) == 1
    assert [node.key.id for node in shortest[0]["path"].nodes] == ["p1", "p2", "p3"]
    assert len(shortest[0]["path"].relationships) == 2
    request = service.path_requests[-1]
    assert (request.source, request.target, request.direction) == ("p1", "p3", nidx_pb2.LPG_BOTH)
    assert list(request.edge_types) == ["KNOWS"] and request.max_hops == 6


def test_parameters_limits_and_rejections(graph, queries) -> None:
    client, _service = graph
    assert ids(client.execute("match (x:Person {age: $age}) return x", {"age": 1}), "x") == ["p1", "p2"]
    assert client.execute(queries[0], {"email": None}) == []
    assert client.execute(queries[4], {"minimum": None}) == []
    assert len(client.execute(queries[1].replace("100", "1"), {"name": "Alice"})) == 1
    assert client.execute(queries[1].replace("100", "0"), {"name": "Alice"}) == []
    assert client.execute(queries[6], {"start": "Alice", "end": "nobody"}) == []
    with pytest.raises(UnsupportedQuery, match="identical endpoints"):
        client.execute(queries[6], {"start": "Alice", "end": "Alice"})
    with pytest.raises(ValueError, match="Missing parameter"):
        client.execute(queries[0], {})
    with pytest.raises(UnsupportedQuery):
        client.execute(queries[0] + " DELETE p", {"email": "a"})
    with pytest.raises(UnsupportedQuery):
        client.execute(queries[0].replace("RETURN p", "RETURN P"), {"email": "a"})
    with pytest.raises(QueryLimitExceeded, match="path length"):
        client.execute(queries[2].replace("1..3", "1..7"), {"name": "Alice"})
    with pytest.raises(QueryLimitExceeded, match="LIMIT"):
        client.execute(queries[1].replace("100", "1001"), {"name": "Alice"})
    bounded = Cypher(client.client, "graph", max_records=2)
    with pytest.raises(QueryLimitExceeded, match="Record limit"):
        bounded.execute(queries[1], {"name": "Alice"})
    bounded = Cypher(client.client, "graph", max_rpcs=1)
    with pytest.raises(QueryLimitExceeded, match="RPC limit"):
        bounded.execute(queries[1], {"name": "Alice"})
    bounded = Cypher(client.client, "graph", max_results=1)
    with pytest.raises(QueryLimitExceeded, match="Result limit"):
        bounded.execute("MATCH (p:Person {age: $age}) RETURN p", {"age": 1})


def test_multiple_typed_node_parameters(graph) -> None:
    client, service = graph
    for node in service.records:
        if node.key.kind == nidx_pb2.LPG_NODE and "Person" in node.labels:
            node.properties["active"].boolean = node.key.id != "p2"
            node.properties["rating"].float = 4.5
    query = """
        MATCH (p:Person {name: $name, age: $age, active: $active, rating: $rating})
        RETURN p
    """
    parameters = {"name": "Alice", "age": 1.0, "active": True, "rating": 4.5}
    assert ids(client.execute(query, parameters), "p") == ["p1"]
    request = service.requests[-1]
    assert [
        (predicate.property, predicate.WhichOneof("predicate")) for predicate in request.filters
    ] == [("name", "text"), ("active", "boolean")]
    for name, value in {"name": "Bob", "age": 2, "active": False, "rating": 4.0}.items():
        assert client.execute(query, {**parameters, name: value}) == []
    assert client.execute(query, {**parameters, "age": True}) == []
    assert client.execute(query, {**parameters, "active": 1}) == []
    assert client.execute(query, {**parameters, "rating": None}) == []
    assert ids(client.execute(query, {**parameters, "name": "Bob", "active": False}), "p") == ["p2"]
    shortest = client.execute(
        """
        MATCH path = shortestPath(
          (a:Person {name: $start, active: $start_active})-[:KNOWS*..2]-
          (b:Person {name: $end, active: $end_active, age: $age})
        )
        RETURN path
        """,
        {"start": "Alice", "start_active": True, "end": "Bob", "end_active": False, "age": 1},
    )
    assert [node.key.id for node in shortest[0]["path"].nodes] == ["p1", "p2"]
    with pytest.raises(ValueError, match=r"Missing parameter \$rating"):
        client.execute(query, {key: value for key, value in parameters.items() if key != "rating"})
    with pytest.raises(ValueError, match=r"Parameter \$rating must be finite"):
        client.execute(query, {**parameters, "rating": float("nan")})
    with pytest.raises(UnsupportedQuery, match="Duplicate property"):
        client.execute("MATCH (p:Person {name: $name, name: $other}) RETURN p", parameters)


def test_conjoined_edge_and_endpoint_parameters(graph) -> None:
    client, service = graph
    for edge in service.records:
        if edge.key.kind == nidx_pb2.LPG_EDGE and edge.edge_type == "RELATED_TO":
            edge.properties["origin"].text = "curated"
            edge.properties["evidence_count"].integer = 3
            edge.properties["verified"].boolean = edge.key.id != "e2"
    query = """
        MATCH (a:Entity)-[r:RELATED_TO]->(b:Entity)
        WHERE r.confidence >= $minimum AND r.origin = $origin
          AND r.evidence_count >= $evidence_count AND r.verified = $verified
          AND a.name = $person AND b.name = $company
        RETURN a, r, b
    """
    parameters = {
        "minimum": 0.8,
        "origin": "curated",
        "evidence_count": 3.0,
        "verified": True,
        "person": "Alice",
        "company": "Acme",
    }
    assert ids(client.execute(query, parameters), "r") == ["e1"]
    assert [
        (predicate.property, predicate.WhichOneof("predicate"))
        for predicate in service.requests[-1].filters
    ] == [("origin", "text"), ("verified", "boolean")]
    for name, value in {
        "minimum": 0.9,
        "origin": "inferred",
        "evidence_count": 4,
        "verified": False,
        "person": "Bob",
        "company": "Other",
    }.items():
        assert client.execute(query, {**parameters, name: value}) == []
    with pytest.raises(ValueError, match=r"Parameter \$evidence_count for >= must be numeric"):
        client.execute(query, {**parameters, "evidence_count": True})
    with pytest.raises(UnsupportedQuery, match="Unknown predicate variable"):
        client.execute(query.replace("a.name", "unknown.name"), parameters)
    with pytest.raises(UnsupportedQuery):
        client.execute(query.replace("AND r.origin", "OR r.origin"), parameters)


def test_paths_respect_types_direction_bounds_and_relationship_uniqueness(graph, queries) -> None:
    client, _service = graph
    query = queries[2].replace("WORKS_WITH|REPORTS_TO*1..3", "KNOWS*1..3")
    rows = client.execute(query, {"name": "Alice"})
    paths = [row["path"] for row in rows]
    assert {tuple(edge.key.id for edge in path.relationships) for path in paths} == {
        ("k1",),
        ("k2",),
        ("k4",),
        ("k1", "k3"),
        ("k2", "k3"),
        ("k4", "k1"),
        ("k4", "k2"),
        ("k4", "k1", "k3"),
        ("k4", "k2", "k3"),
    }
    reversed_shortest = client.execute(queries[6], {"start": "Cara", "end": "Alice"})
    assert [node.key.id for node in reversed_shortest[0]["path"].nodes] == ["p3", "p2", "p1"]


def test_failures_are_surfaced(graph, queries) -> None:
    client, service = graph
    service.repeat_cursor = True
    with pytest.raises(RuntimeError, match="repeated a pagination token"):
        client.execute("MATCH (p:Person {age: $age}) RETURN p", {"age": 1})
    service.repeat_cursor = False
    service.fail_find = True
    with pytest.raises(grpc.RpcError) as error:
        client.execute(queries[0], {"email": "a"})
    assert error.value.code() == grpc.StatusCode.UNAVAILABLE
    service.fail_find = False
    service.expand_error = grpc.StatusCode.UNAVAILABLE
    with pytest.raises(grpc.RpcError) as error:
        client.execute(queries[1], {"name": "Alice"})
    assert error.value.code() == grpc.StatusCode.UNAVAILABLE
    service.expand_error = grpc.StatusCode.RESOURCE_EXHAUSTED
    with pytest.raises(QueryLimitExceeded, match="Server limit"):
        client.execute(queries[2], {"name": "Alice"})


def test_cli_over_grpc(queries) -> None:
    service = Service()
    with ThreadPoolExecutor(max_workers=2) as pool:
        server = grpc.server(pool)
        nidx_pb2_grpc.add_NidxLPGServicer_to_server(service, server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "nidx.examples.lpg.cypher",
                    "--address",
                    f"127.0.0.1:{port}",
                    "--graph-id",
                    "graph",
                    "--query",
                    queries[6],
                    "--parameters",
                    '{"start":"Alice","end":"Cara"}',
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            path = json.loads(result.stdout)[0]["path"]
            assert [node["key"]["id"] for node in path["nodes"]] == ["p1", "p2", "p3"]
            assert len(path["relationships"]) == 2
        finally:
            server.stop(0).wait()
