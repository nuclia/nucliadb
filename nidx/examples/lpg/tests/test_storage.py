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
import json
import subprocess
import sys
from collections.abc import Iterator, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
from uuid import uuid4

import grpc
import pytest
from nidx_protos import nidx_pb2

from nidx.examples.lpg.storage import (
    Direction,
    Graph,
    NidxClient,
    PropertyFilter,
    PropertyValue,
    Record,
    _from_lpg_property,
    _to_lpg_property,
)


@pytest.fixture
def nidx(lpg_address: str) -> Iterator[NidxClient]:
    instance = NidxClient(lpg_address)
    try:
        yield instance
    finally:
        instance.close()


def ids(records: list[Record]) -> list[str]:
    return [record.id for record in records]


@pytest.fixture
def connected_graph(nidx: NidxClient) -> Graph:
    graph = Graph.create(nidx, str(uuid4()))
    for id, label, active, age, country in [
        ("A", "Person", True, 30, "ES"),
        ("B", "Person", False, 40, "ES"),
        ("C", "Person", True, 35, "FR"),
        ("X", "Company", True, 10, "ES"),
        ("isolated", "Person", True, 25, "ES"),
    ]:
        graph.upsert_node(
            id,
            [label],
            {"active": active, "age": age, "country": country, "rating": 4.5},
        )
    for id, source, edge_type, target, trusted, since, score in [
        ("ab1", "A", "KNOWS", "B", True, 2020, 0.9),
        ("ab2", "A", "KNOWS", "B", False, 2024, 0.4),
        ("bc", "B", "KNOWS", "C", True, 2021, 0.8),
        ("ax", "A", "WORKS_FOR", "X", True, 2018, 0.9),
        ("xc", "X", "EMPLOYS", "C", True, 2023, 0.9),
        ("loop", "A", "KNOWS", "A", True, 1990, 1.0),
    ]:
        graph.upsert_edge(
            id, source, edge_type, target, {"trusted": trusted, "since": since, "score": score}
        )
    return graph


def test_expand_records_filters_and_limits(connected_graph: Graph) -> None:
    graph = connected_graph
    result = graph.expand(
        ["A", "B", "A"],
        direction="outgoing",
        edge_types=["KNOWS"],
        candidate_limit=4,
        max_results=4,
    )
    assert {(match.node_id, match.edge.id, match.neighbor.id) for match in result.matches} == {
        ("A", "ab1", "B"),
        ("A", "ab2", "B"),
        ("A", "loop", "A"),
        ("B", "bc", "C"),
    }
    assert result.examined_edges == 4
    assert all(match.edge.kind == "edge" and match.neighbor.kind == "node" for match in result.matches)
    assert all(match.neighbor.tenant == graph.tenant for match in result.matches)
    incoming = graph.expand(["B"], direction="incoming", edge_types=["KNOWS"])
    assert {(match.edge.id, match.neighbor.id) for match in incoming.matches} == {
        ("ab1", "A"),
        ("ab2", "A"),
    }
    assert all(match.edge.source == "A" and match.edge.target == "B" for match in incoming.matches)
    both = graph.expand(["A", "B"], direction="both", edge_types=["KNOWS"])
    assert len(both.matches) == 6 and both.examined_edges == 4
    filtered = graph.expand(
        ["A", "B"],
        direction="outgoing",
        edge_types=["KNOWS"],
        edge_filters=[
            PropertyFilter("trusted", "eq", value=True),
            PropertyFilter("since", "range", lower=2020),
            PropertyFilter("score", "range", lower=0.8),
        ],
        neighbor_label="Person",
        neighbor_filters=[
            PropertyFilter("active", "eq", value=True),
            PropertyFilter("country", "eq", value="FR"),
            PropertyFilter("age", "eq", value=35),
            PropertyFilter("rating", "eq", value=4.5),
        ],
    )
    assert [(match.node_id, match.edge.id, match.neighbor.id) for match in filtered.matches] == [
        ("B", "bc", "C")
    ]
    assert filtered.examined_edges == 2
    assert filtered.matches[0].edge == graph.get("edge", "bc")
    assert filtered.matches[0].neighbor == graph.get("node", "C")
    alternatives = graph.expand(["A", "X"], direction="outgoing", edge_types=["WORKS_FOR", "EMPLOYS"])
    assert {match.edge.id for match in alternatives.matches} == {"ax", "xc"}
    assert not graph.expand(["isolated"], direction="both").matches
    for limited in (
        lambda: graph.expand(["A", "B"], direction="outgoing", edge_types=["KNOWS"], candidate_limit=3),
        lambda: graph.expand(["A", "B"], direction="outgoing", edge_types=["KNOWS"], max_results=3),
    ):
        with pytest.raises(grpc.RpcError) as error:
            limited()
        assert error.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    with pytest.raises(grpc.RpcError) as error:
        graph.expand(["missing"], direction="outgoing")
    assert error.value.code() == grpc.StatusCode.NOT_FOUND
    with pytest.raises(ValueError, match="sequence"):
        graph.expand("A", direction="outgoing")


def test_shortest_path_records_filters_and_limits(connected_graph: Graph) -> None:
    graph = connected_graph
    path = graph.shortest_path(
        "A",
        "C",
        direction="outgoing",
        edge_types=["KNOWS"],
        edge_filters=[PropertyFilter("trusted", "eq", value=True)],
        max_hops=2,
        max_visited_nodes=4,
        max_examined_edges=3,
    )
    assert path is not None
    assert ids(path.nodes) == ["A", "B", "C"]
    assert ids(path.edges) == ["ab1", "bc"]
    assert path.examined_edges == 3
    assert path.nodes[0] == graph.get("node", "A")
    assert path.edges[0] == graph.get("edge", "ab1")
    directions: tuple[Direction, ...] = ("incoming", "both")
    for direction in directions:
        reversed_path = graph.shortest_path("C", "A", direction=direction, edge_types=["KNOWS"])
        assert reversed_path is not None
        assert ids(reversed_path.nodes) == ["C", "B", "A"]
        assert [(edge.source, edge.target) for edge in reversed_path.edges] == [
            ("B", "C"),
            ("A", "B"),
        ]
    assert graph.shortest_path("C", "A", direction="outgoing", edge_types=["KNOWS"]) is None
    assert graph.shortest_path("isolated", "C", direction="both") is None
    assert graph.shortest_path("A", "C", direction="outgoing", max_hops=1) is None
    assert graph.shortest_path("A", "C", direction="outgoing", node_label="Company") is None
    filtered = graph.shortest_path(
        "A",
        "C",
        direction="outgoing",
        edge_types=["WORKS_FOR", "EMPLOYS"],
        edge_filters=[PropertyFilter("trusted", "eq", value=True)],
        node_label="Company",
        node_filters=[
            PropertyFilter("active", "eq", value=True),
            PropertyFilter("age", "range", lower=10),
            PropertyFilter("country", "eq", value="ES"),
            PropertyFilter("rating", "eq", value=4.5),
        ],
        node_filter_scope="intermediate",
    )
    assert filtered is not None and ids(filtered.nodes) == ["A", "X", "C"]
    assert ids(filtered.edges) == ["ax", "xc"]
    assert (
        graph.shortest_path(
            "A", "C", direction="outgoing", node_filters=[PropertyFilter("age", "eq", value=30)]
        )
        is None
    )
    zero = graph.shortest_path(
        "A", "A", direction="outgoing", node_label="Company", node_filter_scope="intermediate"
    )
    assert zero is not None and ids(zero.nodes) == ["A"] and not zero.edges
    assert graph.shortest_path("A", "A", direction="outgoing", node_label="Company") is None
    for bounded in (
        lambda: graph.shortest_path(
            "A", "C", direction="outgoing", edge_types=["KNOWS"], max_visited_nodes=2
        ),
        lambda: graph.shortest_path(
            "A", "C", direction="outgoing", edge_types=["KNOWS"], max_examined_edges=2
        ),
    ):
        with pytest.raises(grpc.RpcError) as error:
            bounded()
        assert error.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
    with pytest.raises(grpc.RpcError) as error:
        graph.shortest_path("A", "missing", direction="both")
    assert error.value.code() == grpc.StatusCode.NOT_FOUND


def test_records_filters_and_reopen(nidx: NidxClient) -> None:
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


def test_datetime_and_string_list_properties(nidx: NidxClient) -> None:
    graph = Graph.create(nidx, str(uuid4()))
    aware = datetime(2026, 11, 24, 15, 42, 10, 123456, tzinfo=timezone(timedelta(hours=2)))
    naive = datetime(2026, 11, 24, 15, 42, 10)
    node = graph.upsert_node(
        "A", ["Person"], {"created": aware, "local": naive, "tags": ["first", "", "first"]}
    )
    graph.upsert_node("B", ["Person"], {"tags": []})
    edge = graph.upsert_edge("ab", "A", "KNOWS", "B", {"since": aware, "tags": ["friend"]})
    assert graph.get("node", "A") == node
    assert graph.get("edge", "ab") == edge
    assert sorted(ids(graph.find("node"))) == ["A", "B"]
    expansion = graph.expand(["A"], direction="outgoing")
    assert expansion.matches[0].edge == edge
    assert expansion.matches[0].neighbor.properties == {"tags": []}
    path = graph.shortest_path("A", "B", direction="outgoing")
    assert path is not None
    assert path.nodes[0] == node
    assert path.edges == [edge]
    assert _to_lpg_property(aware).WhichOneof("value") == "datetime"
    assert _to_lpg_property([]).WhichOneof("value") == "strings"
    updated = graph.upsert_node("A", ["Person"], {"created": naive, "tags": []})
    reopened = Graph(nidx, graph.tenant, graph.shard_id)
    assert reopened.get("node", "A") == updated


def test_invalid_datetime_and_unsupported_property_types() -> None:
    assert _from_lpg_property(nidx_pb2.LpgProperty(datetime="2026-11-24T12:00:00Z")) == datetime(
        2026, 11, 24, 12, tzinfo=timezone.utc
    )
    with pytest.raises(ValueError, match="Invalid NidxLPG datetime"):
        _from_lpg_property(nidx_pb2.LpgProperty(datetime="not-a-date"))
    with pytest.raises(ValueError, match="not supported"):
        _from_lpg_property(
            nidx_pb2.LpgProperty(integer_range=nidx_pb2.LpgIntegerRange(lower=1, upper=2))
        )


def test_date_filters_across_graph_reads(nidx: NidxClient) -> None:
    graph = Graph.create(nidx, str(uuid4()))
    instant = datetime(2026, 11, 24, 12, 0, 0, 123456, tzinfo=timezone.utc)
    offset = instant.astimezone(timezone(timedelta(hours=2)))
    later = (instant + timedelta(microseconds=1)).replace(tzinfo=None)
    tomorrow = instant + timedelta(days=1)
    graph.upsert_node(
        "A",
        ["Person"],
        {"created": offset, "active": True},
        text_chunks={"note": ["date filtering example"]},
    )
    graph.upsert_node("B", ["Person"], {"created": later, "active": True})
    graph.upsert_node("C", ["Person"], {"created": tomorrow, "active": False})
    graph.upsert_edge("ab", "A", "KNOWS", "B", {"since": instant})
    graph.upsert_edge("bc", "B", "KNOWS", "C", {"since": later})
    graph.upsert_edge("ac", "A", "KNOWS", "C", {"since": tomorrow})

    exact = PropertyFilter("created", "eq", value=instant)
    assert ids(graph.find("node", exact)) == ["A"]
    assert ids(graph.find("node", PropertyFilter("created", "eq", value=later))) == ["B"]
    assert sorted(
        ids(
            graph.find(
                "node",
                PropertyFilter("created", "range", lower=offset, upper=later),
                PropertyFilter("active", "eq", value=True),
            )
        )
    ) == ["A", "B"]
    assert ids(graph.find("node", PropertyFilter("created", "range", lower=tomorrow))) == ["C"]
    assert ids(graph.find("node", PropertyFilter("created", "range", upper=instant))) == ["A"]
    assert [hit.record.id for hit in graph.search(text="filtering", filters=[exact])] == ["A"]
    assert ids(graph.topology(PropertyFilter("since", "eq", value=offset))) == ["ab"]

    expansion = graph.expand(
        ["A"],
        direction="outgoing",
        edge_filters=[PropertyFilter("since", "range", upper=later)],
        neighbor_filters=[PropertyFilter("created", "eq", value=later)],
    )
    assert [(match.edge.id, match.neighbor.id) for match in expansion.matches] == [("ab", "B")]
    edge_filters = [PropertyFilter("since", "range", upper=later)]
    node_filters = [PropertyFilter("created", "range", upper=later)]
    path = graph.shortest_path(
        "A",
        "C",
        direction="outgoing",
        edge_filters=edge_filters,
        node_filters=node_filters,
        node_filter_scope="intermediate",
    )
    assert path is not None
    assert [node.id for node in path.nodes] == ["A", "B", "C"]
    assert (
        graph.shortest_path(
            "A",
            "C",
            direction="outgoing",
            edge_filters=edge_filters,
            node_filters=node_filters,
        )
        is None
    )

    for predicate in [
        nidx_pb2.LpgPropertyFilter(property="created", datetime="invalid"),
        nidx_pb2.LpgPropertyFilter(property="created", datetime="9999-01-01T00:00:00Z"),
        nidx_pb2.LpgPropertyFilter(property="created", date_range=nidx_pb2.LpgDateRange()),
        nidx_pb2.LpgPropertyFilter(
            property="created",
            date_range=nidx_pb2.LpgDateRange(
                lower=tomorrow.isoformat(),
                upper=instant.isoformat(),
            ),
        ),
    ]:
        with pytest.raises(grpc.RpcError) as error:
            graph.lpg.FindRecords(
                nidx_pb2.FindRecordsRequest(
                    graph_id=graph.shard_id,
                    kind=nidx_pb2.LPG_NODE,
                    filters=[predicate],
                ),
                timeout=60,
            )
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@pytest.mark.parametrize("value", [[1], ["valid", False], [["nested"]]])
def test_invalid_string_list_properties(value: PropertyValue) -> None:
    with pytest.raises(ValueError, match="only strings"):
        Record(str(uuid4()), "node", "A", {"tags": value}, labels=["Person"])
    with pytest.raises(ValueError, match="only strings"):
        _to_lpg_property(value)


def test_node_and_edge_type_cardinality() -> None:
    with pytest.raises(ValueError, match="at least one label/type"):
        Record(str(uuid4()), "node", "untyped", {})
    with pytest.raises(ValueError, match="unique"):
        Record(str(uuid4()), "node", "duplicate", {}, labels=["Product", "Product"])
    with pytest.raises(ValueError, match="cannot have node labels"):
        Record(
            str(uuid4()),
            "edge",
            "typed",
            {},
            labels=["Relationship"],
            source="a",
            target="b",
            edge_type="LINKS",
        )


def test_parallel_edges_typed_graph_prefilter_and_replacement(nidx: NidxClient) -> None:
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


def test_text_vector_and_hybrid_chunk_search(nidx: NidxClient) -> None:
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
    specific_property_hits = graph.search(text="crisp sweet", kind="node", text_property="features")
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


def test_text_chunks_and_embeddings_can_be_added_independently(nidx: NidxClient) -> None:
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
    assert (
        graph.search(vector=[1.0, 0.0, 0.0], kind="node", text_property="description")[0].record == node
    )
    assert (
        graph.search(vector=[1.0, 0.0, 0.0], kind="edge", text_property="inspection")[0].record == edge
    )

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
    nidx: NidxClient,
    chunks: dict[str, list[str]],
    vectors: Mapping[str, Sequence[Sequence[float]]],
) -> None:
    graph = Graph.create(nidx, str(uuid4()))
    with pytest.raises(ValueError):
        graph.upsert_node("P1", ["Product"], {}, text_chunks=chunks, embeddings=vectors)


def test_invalid_hybrid_search_queries(nidx: NidxClient) -> None:
    graph = Graph.create(nidx, str(uuid4()))
    with pytest.raises(ValueError, match="Provide"):
        graph.search()
    with pytest.raises(ValueError, match="exactly 3"):
        graph.search(vector=[1.0, 0.0])
    with pytest.raises(ValueError, match="Zero vectors"):
        graph.search(vector=[0.0, 0.0, 0.0])
    with pytest.raises(ValueError, match="top_k"):
        graph.search(text="query", top_k=0)


def _mock_graph(*, max_records: int = 1000) -> Graph:
    tenant = str(uuid4())
    client = Mock()
    client.api.GetShard.return_value.metadata.kbid = tenant
    client.api.ListVectorSets.return_value.vectorsets = ["test"]
    client.lpg_client.GetRecords.return_value = nidx_pb2.GetRecordsResponse()
    client.lpg_client.SearchChunks.return_value = nidx_pb2.SearchChunksResponse()
    return Graph(
        client, tenant, str(uuid4()), vectorset="test", vector_dimension=3, max_records=max_records
    )


def test_search_minimum_scores_are_forwarded() -> None:
    graph = _mock_graph(max_records=1)
    client = graph.nidx
    graph.search(text="query", vector=[1.0, 0.0, 0.0], min_score_semantic=0.75, min_score_keyword=2.5)
    request = client.lpg_client.SearchChunks.call_args.args[0]
    assert request.top_k == 10
    assert request.min_score_semantic == pytest.approx(0.75)
    assert request.min_score_keyword == pytest.approx(2.5)
    graph.search(text="query")
    request = client.lpg_client.SearchChunks.call_args.args[0]
    assert request.min_score_semantic == 0.0
    assert request.min_score_keyword == 0.0
    for parameter in ["min_score_semantic", "min_score_keyword"]:
        for value in [float("nan"), float("inf"), float("-inf"), True, "0.5"]:
            client.lpg_client.SearchChunks.reset_mock()
            with pytest.raises(ValueError, match=parameter):
                graph.search(text="query", **{parameter: value})
            client.lpg_client.SearchChunks.assert_not_called()


def test_find_pages_are_explicit_and_independent_of_total_record_limit() -> None:
    graph = _mock_graph(max_records=1)
    records = [
        nidx_pb2.LpgRecord(key=nidx_pb2.LpgRecordKey(kind=nidx_pb2.LPG_NODE, id=id), labels=["Node"])
        for id in ["b", "a", "c"]
    ]
    graph.lpg.FindRecords.side_effect = [
        nidx_pb2.FindRecordsResponse(records=records[:2], next_page_token="next"),
        nidx_pb2.FindRecordsResponse(records=records[2:]),
    ]
    predicate = PropertyFilter("price", "range", lower=1)
    first = graph.find_page("node", predicate, page_size=2)
    assert [record.id for record in first.records] == ["b", "a"]
    assert first.next_page_token == "next"
    second = graph.find_page("node", predicate, page_size=2, page_token=first.next_page_token)
    assert [record.id for record in second.records] == ["c"]
    assert second.next_page_token == ""
    requests = [call.args[0] for call in graph.lpg.FindRecords.call_args_list]
    assert [request.page_token for request in requests] == ["", "next"]
    assert all(
        request.page_size == 2 and list(request.filters) == [predicate.to_proto()]
        for request in requests
    )
    graph.lpg.FindRecords.reset_mock()
    for arguments in [
        {"page_size": 0},
        {"page_size": 1001},
        {"page_size": True},
        {"page_token": None},
    ]:
        with pytest.raises(ValueError):
            graph.find_page("node", **arguments)
    with pytest.raises(ValueError, match="kind"):
        graph.find_page("invalid")
    graph.lpg.FindRecords.assert_not_called()


def test_find_convenience_preserves_sorting_limits_and_cursor_loop_checks() -> None:
    graph = _mock_graph(max_records=2)
    records = [
        nidx_pb2.LpgRecord(key=nidx_pb2.LpgRecordKey(kind=nidx_pb2.LPG_NODE, id=id), labels=["Node"])
        for id in ["b", "a"]
    ]
    graph.lpg.FindRecords.side_effect = [
        nidx_pb2.FindRecordsResponse(records=records[:1], next_page_token="next"),
        nidx_pb2.FindRecordsResponse(records=records[1:]),
    ]
    assert [record.id for record in graph.find("node")] == ["a", "b"]
    graph.max_records = 1
    graph.lpg.FindRecords.side_effect = None
    graph.lpg.FindRecords.return_value = nidx_pb2.FindRecordsResponse(records=records)
    with pytest.raises(RuntimeError, match="candidate limit exceeded"):
        graph.find("node")
    graph.lpg.FindRecords.return_value = nidx_pb2.FindRecordsResponse(next_page_token="loop")
    with pytest.raises(RuntimeError, match="repeated a pagination token"):
        graph.find("node")


def test_node_replacement_and_tenant_isolation(nidx: NidxClient) -> None:
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
    first.delete_edge("E1")
    first.delete_node("P1")
    assert ids(first.find("node")) == ["W1"]
    assert first.topology() == []
    assert second.get("node", "P1") == other
    assert second.topology() == [isolated_edge]


def test_node_deletion_requires_removing_incident_edges(nidx: NidxClient) -> None:
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
    assert before is not None
    assert ids(graph.find("edge")) == ["E1", "E2", "E3", "E4", "E5"]

    reopened = Graph(nidx, graph.tenant, graph.shard_id)
    for edge_id in ["E1", "E2", "E3", "E5"]:
        with pytest.raises(ValueError, match="incident edges"):
            reopened.delete_node("P1")
        reopened.delete_edge(edge_id)
    reopened.delete_node("P1")
    assert reopened.get("node", "P1") is None
    assert ids(reopened.find("node")) == ["C1", "W1"]
    assert ids(reopened.find("edge")) == ["E4"]
    assert ids(reopened.topology()) == ["E4"]
    reopened.delete_node("P1")


def test_invalid_endpoints_and_candidate_limit(nidx: NidxClient) -> None:
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
        bounded.delete_node("P1")
    unchanged = graph.get("node", "P1")
    assert unchanged is not None


def test_client_reconnect_and_demo(nidx: NidxClient, lpg_address: str) -> None:
    original = NidxClient(lpg_address)
    try:
        graph = Graph.create(original, str(uuid4()))
        product = graph.upsert_node("P1", ["Product"], {"price": 150})
        graph.upsert_node("W1", ["Warehouse"], {})
        edge = graph.upsert_edge("E1", "P1", "STOCKED_AT", "W1", {"quantity": 10})
        tenant, shard_id = graph.tenant, graph.shard_id
    finally:
        original.close()

    restarted = NidxClient(lpg_address)
    try:
        reopened = Graph(restarted, tenant, shard_id)
        assert reopened.get("node", "P1") == product
        assert reopened.get("edge", "E1") == edge
        assert reopened.topology(PropertyFilter("quantity", "eq", value=10)) == [edge]
        reopened.delete_edge("E1")
        reopened.delete_node("P1")
        assert reopened.find("edge") == []
    finally:
        restarted.close()

    demo = subprocess.run(
        [sys.executable, "-m", "nidx.examples.lpg.demo", "--address", lpg_address],
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert demo.returncode == 0, demo.stdout + demo.stderr
    result = json.loads(demo.stdout)
    assert {person["name"] for person in result["people"]} == {
        "Ramon",
        "Ferran",
        "Joan Antoni",
        "Carmen",
        "Oni",
        "Kento",
        "Arnau",
        "Matteu Jonglez",
    }
    people = {person["name"]: person for person in result["people"]}
    assert people["Ramon"]["role"] == "manager"
    assert all(
        people[name]["role"] == "software engineer" and people[name]["team"] == "nucliadb"
        for name in ("Ferran", "Joan Antoni")
    )
    assert {person["name"] for person in result["discovery"]["people"]} == {
        "Carmen",
        "Oni",
        "Kento",
        "Arnau",
    }
    assert result["updated_text_properties"]["text_chunks"]["focus"] == [
        "nucliadb graph search, indexing, and retrieval"
    ]
    for mode in ("text", "vector", "hybrid"):
        assert {hit["name"] for hit in result["search"][mode]} == {"Ferran", "Joan Antoni"}
    assert all(
        hit["text_rank"] is not None and hit["vector_rank"] is not None
        for hit in result["search"]["hybrid"]
    )
    assert {match["person"] for match in result["expand"]["matches"]} == {
        "Ferran",
        "Joan Antoni",
        "Carmen",
        "Oni",
        "Kento",
        "Arnau",
    }
    assert result["shortest_path"]["people"] == ["Ferran", "Ramon", "Matteu Jonglez"]
    assert result["shortest_path"]["relationships"] == ["REPORTS_TO", "REPORTS_TO"]
    cypher = result["cypher"]
    assert cypher["lookup"]["rows"][0]["p"]["name"] == "Ferran"
    assert len(cypher["neighborhood"]["rows"]) == 8
    assert len(cypher["bounded_traversal"]["rows"]) == 2
    assert len(cypher["coworkers"]["rows"]) == 7
    assert {row["other"]["name"] for row in cypher["coworkers"]["rows"]} == (set(people) - {"Ferran"})
    assert len(cypher["relationship_filter"]["rows"]) == 7
    assert any(
        row["a"]["name"] == "Ramon" and row["b"]["name"] == "Matteu Jonglez"
        for row in cypher["relationship_filter"]["rows"]
    )
    assert cypher["employee_count"]["rows"] == [{"c.name": "Progress", "employees": 8}]
    assert [node["name"] for node in cypher["shortest_path"]["rows"][0]["path"]["nodes"]] == [
        "Ferran",
        "Ramon",
        "Matteu Jonglez",
    ]
    assert "incident edges" in result["deletion"]["initial_error"]
    assert result["deletion"]["removed_edges"] == ["arnau-reports-to", "arnau-works-for"]
    assert result["deletion"]["node_exists"] is False
    graph = Graph(nidx, result["graph"]["tenant"], result["graph"]["shard_id"])
    assert graph.get("node", "arnau") is None
    assert graph.get("node", "ramon") is not None
    assert graph.get("edge", "arnau-reports-to") is None
    assert graph.get("edge", "arnau-works-for") is None


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
        PropertyFilter("x", "range", lower=datetime(2026, 1, 1), upper=2),
        PropertyFilter("x", "range", lower=datetime(2026, 1, 2), upper=datetime(2026, 1, 1)),
    ],
)
def test_invalid_property_filters(predicate: PropertyFilter) -> None:
    with pytest.raises(ValueError):
        predicate.to_proto()
