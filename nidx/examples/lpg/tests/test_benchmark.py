# Copyright (C) 2021 Bosutech XXI S.L.
#
# SPDX-License-Identifier: AGPL-3.0-or-later
import contextlib
import io
import random
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from nidx_protos import nidx_pb2

from nidx.examples.lpg import benchmark as benchmark_module
from nidx.examples.lpg.benchmark import (
    CASE_NAMES,
    Case,
    Result,
    Sample,
    SearchChunk,
    benchmark,
    cases,
    percentile,
    sample_chunk,
    sample_graph,
    select_cases,
)
from nidx.examples.lpg.cypher import QueryLimitExceeded, parse
from nidx.examples.lpg.storage import Record, SearchHit


def node(id: str, label: str) -> Record:
    return Record("00000000-0000-0000-0000-000000000001", "node", id, {"name": id}, labels=[label])


class BenchmarkTests(unittest.TestCase):
    def test_select_cases_in_requested_order(self) -> None:
        available = [Case("first", Mock()), Case("second", Mock()), Case("third", Mock())]
        self.assertEqual(select_cases(available, None), available)
        self.assertEqual(select_cases(available, ["third", "first"]), [available[2], available[0]])
        with self.assertRaisesRegex(ValueError, "Unknown case.*Available: first, second, third"):
            select_cases(available, ["missing"])
        with self.assertRaisesRegex(ValueError, "only be selected once"):
            select_cases(available, ["first", "first"])

    def test_select_case_prefixes_and_deduplicate_overlaps(self) -> None:
        available = [
            Case("python/get", Mock()),
            Case("python/search-vector", Mock()),
            Case("python/search-vector-exact", Mock()),
            Case("cypher/lookup", Mock()),
            Case("cypher/neighborhood", Mock()),
        ]
        self.assertEqual(select_cases(available, ["python/"]), available[:3])
        self.assertEqual(select_cases(available, ["cypher"]), available[3:])
        self.assertEqual(
            select_cases(available, ["cypher", "python/", "python/get"]),
            available[3:] + available[:3],
        )
        self.assertEqual(select_cases(available, ["python/search-vector"]), [available[1]])
        with self.assertRaisesRegex(ValueError, "Unknown case"):
            select_cases(available, ["python/missing"])

    def test_cli_runs_only_selected_cases(self) -> None:
        first = Case("first", Mock())
        selected = Case("second", Mock())
        sample = Sample(
            node("P1", "Product"),
            node("W1", "Warehouse"),
            node("C1", "Client"),
            node("W1", "Warehouse"),
        )
        with (
            patch.object(
                sys,
                "argv",
                [
                    "benchmark",
                    "--graph-id",
                    "g",
                    "--tenant",
                    "t",
                    "--case",
                    "python/",
                ],
            ),
            patch.object(benchmark_module, "NidxClient") as client,
            patch.object(benchmark_module, "Graph"),
            patch.object(benchmark_module, "Cypher"),
            patch.object(
                benchmark_module,
                "cases",
                return_value=[
                    Case("python/get", first.run),
                    Case("python/expand", selected.run),
                    Case("python/search-vector-exact", Mock()),
                ],
            ),
            patch.object(benchmark_module, "sample_graph", return_value=[sample]) as sampling,
            patch.object(benchmark_module, "benchmark") as run,
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            benchmark_module.main()
        self.assertEqual(
            [call.args[0].name for call in run.call_args_list],
            ["python/get", "python/expand", "python/search-vector-exact"],
        )
        self.assertEqual(sampling.call_args.kwargs, {"sample_search": True, "sample_vectors": True})
        self.assertEqual(sampling.call_args.args[1], 6)
        self.assertEqual(run.call_count, 3)
        client.return_value.close.assert_called_once()

    def test_sampling_requires_visible_nodes_and_edges(self) -> None:
        graph = Mock()
        graph.get.side_effect = lambda kind, id: node(
            id, {"P": "Product", "W": "Warehouse", "C": "Client"}[id[0]]
        )
        warehouse = node("W1", "Warehouse")
        graph.expand.return_value.matches = [SimpleNamespace(neighbor=warehouse)]
        samples = sample_graph(graph, 2, random.Random(42))
        self.assertEqual(len(samples), 2)
        self.assertIs(samples[0].adjacent_warehouse, warehouse)
        self.assertEqual(graph.get.call_count, 6)
        graph._get_proto.assert_not_called()
        target = SearchChunk(samples[0].product.id, "description", 0, "first", [1.0, 0.0])
        with patch.object(benchmark_module, "sample_chunk", return_value=target) as sample_embedding:
            vector_samples = sample_graph(graph, 1, random.Random(42), sample_vectors=True)
        self.assertIs(vector_samples[0].search_chunk, target)
        self.assertEqual(
            sample_embedding.call_args.args[1],
            (vector_samples[0].product, vector_samples[0].warehouse, vector_samples[0].client),
        )
        graph.get.return_value = None
        graph.get.side_effect = None
        with self.assertRaisesRegex(RuntimeError, "missing or not yet visible"):
            sample_graph(graph, 1, random.Random(42))

    def test_cypher_workloads_are_supported_and_bind_sampled_names(self) -> None:
        graph = Mock()
        cypher = Mock()
        seen: list[tuple[str, dict[str, object]]] = []

        def execute(query: str, parameters: dict[str, object]) -> list[object]:
            seen.append((parse(query)[0], parameters))
            return []

        cypher.execute.side_effect = execute
        sample = Sample(
            node("P1", "Product"), node("W2", "Warehouse"), node("C1", "Client"), node("W1", "Warehouse")
        )
        for case in cases(graph, cypher):
            if case.name.startswith("cypher/"):
                self.assertEqual(case.run(sample).count, 0)
        self.assertEqual(
            [kind for kind, _ in seen],
            [
                "lookup",
                "neighborhood",
                "traverse",
                "join",
                "edge_filter",
                "aggregate",
                "shortest",
                "shortest",
            ],
        )
        self.assertEqual(seen[-2][1], {"start": "P1", "end": "W1"})
        self.assertEqual(seen[-1][1], {"start": "P1", "end": "W2"})

    def test_samples_stored_embedding_and_chunk_outside_timed_case(self) -> None:
        graph = Mock()
        proto = nidx_pb2.LpgRecord()
        proto.text_properties["description"].chunks.extend(["first", "second"])
        proto.text_properties["description"].embeddings.extend(
            [nidx_pb2.LpgVector(values=[1.0, 0.0]), nidx_pb2.LpgVector(values=[0.0, 1.0])]
        )
        graph._get_proto.return_value = proto
        target = sample_chunk(graph, [node("P1", "Product")], random.Random(42), require_vector=True)
        graph._get_proto.assert_called_once_with("node", "P1")
        self.assertEqual(target.node_id, "P1")
        self.assertEqual(target.property, "description")
        self.assertEqual(target.text, proto.text_properties["description"].chunks[target.chunk_index])
        self.assertEqual(
            target.vector,
            list(proto.text_properties["description"].embeddings[target.chunk_index].values),
        )
        graph.search.assert_not_called()
        graph._get_proto.return_value = nidx_pb2.LpgRecord()
        with self.assertRaisesRegex(RuntimeError, "no stored embeddings"):
            sample_chunk(graph, [node("P1", "Product")], random.Random(42), require_vector=True)

    def test_text_sampling_does_not_require_embeddings(self) -> None:
        graph = Mock()
        proto = nidx_pb2.LpgRecord()
        proto.text_properties["description"].chunks.append("exact source chunk")
        graph._get_proto.return_value = proto
        target = sample_chunk(graph, [node("P1", "Product")], random.Random(42), require_vector=False)
        self.assertEqual(target.text, "exact source chunk")
        self.assertIsNone(target.vector)

    def test_samples_use_distinct_nodes_including_warmups(self) -> None:
        graph = Mock()
        graph.get.side_effect = lambda kind, id: node(
            id, {"P": "Product", "W": "Warehouse", "C": "Client"}[id[0]]
        )
        graph.expand.return_value.matches = [SimpleNamespace(neighbor=node("W1", "Warehouse"))]

        def chunk(graph, nodes, rng, *, require_vector):
            selected = rng.choice(nodes)
            return SearchChunk(selected.id, "description", 0, selected.id, [1.0])

        with patch.object(benchmark_module, "sample_chunk", side_effect=chunk):
            samples = sample_graph(graph, 20, random.Random(42), sample_vectors=True)
        target_ids = [sample.search_chunk.node_id for sample in samples]
        self.assertEqual(len(set(target_ids)), 20)
        run = Mock(return_value=Result(1))
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            benchmark(Case("python/search-vector", run), samples, warmups=2)
        self.assertEqual([call.args[0] for call in run.call_args_list], samples)
        self.assertIn("18/18 ok", output.getvalue())

    def test_search_cases_use_the_same_sampled_property_chunk(self) -> None:
        graph = Mock()
        product = node("P1", "Product")
        warehouse = node("W1", "Warehouse")
        sample = Sample(
            product,
            warehouse,
            node("C1", "Client"),
            warehouse,
            SearchChunk("P1", "description", 1, "exact source chunk", [0.0, 1.0]),
        )
        graph.search.return_value = [
            SearchHit(product, "description", 1, "exact source chunk", 1 / 61, 1, 1)
        ]
        by_name = {case.name: case for case in cases(graph, Mock())}
        for name, text, vector in (
            ("python/search-text", "exact source chunk", None),
            ("python/search-vector", None, [0.0, 1.0]),
            ("python/search-hybrid", "exact source chunk", [0.0, 1.0]),
        ):
            with self.subTest(case=name):
                result = by_name[name].run(sample)
                self.assertEqual(result.self_match_rank, 1)
                graph.search.assert_called_with(
                    text=text,
                    vector=vector,
                    kind="node",
                    text_property="description",
                    top_k=10,
                    min_score_semantic=0.0,
                )
                graph.search.return_value = []
                with self.assertRaisesRegex(RuntimeError, "self-match missing"):
                    by_name[name].run(sample)
                graph.search.return_value = [
                    SearchHit(product, "description", 1, "exact source chunk", 1 / 61, 1, 1)
                ]
        graph._get_proto.assert_not_called()

    def test_exact_vector_case_checks_node_property_and_chunk(self) -> None:
        graph = Mock()
        product = node("P1", "Product")
        warehouse = node("W1", "Warehouse")
        sample = Sample(
            product,
            warehouse,
            node("C1", "Client"),
            warehouse,
            SearchChunk("P1", "description", 1, "second", [0.0, 1.0]),
        )
        case = select_cases(cases(graph, Mock()), ["python/search-vector-exact"])[0]
        hit = SearchHit(product, "description", 1, "second", 1 / 61, None, 1)
        graph.search.return_value = [
            SearchHit(warehouse, "description", 1, "other", 1 / 61, None, 1),
            hit,
        ]
        result = case.run(sample)
        self.assertEqual(result.self_match_rank, 2)
        graph.search.assert_called_once_with(
            text=None,
            vector=[0.0, 1.0],
            kind="node",
            text_property="description",
            top_k=10,
            min_score_semantic=0.99,
        )
        graph._get_proto.assert_not_called()
        for hits in (
            [],
            [SearchHit(warehouse, "description", 1, "other", 1 / 61, None, 1)],
            [SearchHit(product, "other", 1, "second", 1 / 61, None, 1)],
            [SearchHit(product, "description", 0, "first", 1 / 61, None, 1)],
        ):
            with self.subTest(hits=hits):
                graph.search.return_value = hits
                with self.assertRaisesRegex(RuntimeError, "self-match missing"):
                    case.run(sample)
        graph.search.return_value = [hit]
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            benchmark(case, [sample], warmups=0)
        self.assertIn("self-match rank=1..1; matched=1/1", output.getvalue())

    def test_graph_workloads_use_sampled_ids_and_filters(self) -> None:
        graph = Mock()
        graph.expand.return_value.matches = []
        graph.expand.return_value.examined_edges = 7
        graph.shortest_path.return_value = None
        sample = Sample(
            node("P1", "Product"),
            node("W2", "Warehouse"),
            node("C1", "Client"),
            node("W1", "Warehouse"),
        )
        self.assertEqual(tuple(case.name for case in cases(graph, Mock())), CASE_NAMES)
        by_name = {case.name: case for case in cases(graph, Mock())}
        self.assertEqual(by_name["python/expand-batched"].run(sample).examined_edges, 7)
        self.assertEqual(graph.expand.call_args.args[0], ["P1", "C1"])
        self.assertEqual(by_name["python/shortest-filtered"].run(sample).count, 0)
        self.assertEqual(graph.shortest_path.call_args.args, ("P1", "W1"))
        self.assertEqual(graph.shortest_path.call_args.kwargs["edge_filters"][0].value, True)

    def test_failed_calls_are_not_included_in_successful_latency(self) -> None:
        run = Mock(side_effect=[Result(1), Result(0), QueryLimitExceeded("work cap")])
        sample = Sample(
            node("P1", "Product"), node("W1", "Warehouse"), node("C1", "Client"), node("W1", "Warehouse")
        )
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            benchmark(Case("python/shortest-random", run), [sample, sample, sample], warmups=1)
        self.assertIn("1/2 ok", output.getvalue())
        self.assertIn("found paths=0/1", output.getvalue())
        self.assertIn("1 failed: QueryLimitExceeded: work cap", output.getvalue())
        self.assertEqual(run.call_count, 3)
        self.assertAlmostEqual(percentile([1.0, 3.0, 2.0], 0.95), 2.9)
