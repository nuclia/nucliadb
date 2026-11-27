# Copyright (C) 2021 Bosutech XXI S.L.
#
# SPDX-License-Identifier: AGPL-3.0-or-later
"""Read-only, single-client latency measurements for the 500k-node synthetic LPG graph.

Run after loading and search visibility have settled:

    .venv/bin/python -m nidx.examples.lpg.benchmark --graph-id GRAPH_ID --tenant TENANT_ID

Run one case at a time with --case python/get, or select multiple cases with
--case python/get python/expand. Without --case, all cases run.
Prefixes also work: --case python/ or --case cypher. Exact names take
precedence over prefixes; overlapping selections run each case only once.

The default five samples are only a rough indication; increase --iterations
for more meaningful percentiles. Search cases reuse sampled property chunks
and their stored embeddings, using distinct nodes for warmups and measurements.
Cypher matches names, which need not be unique. Its limits are
1,000 fetched records, 50 RPCs and 1,000 result rows, with a 15s per-RPC
timeout. A query LIMIT may stop early; errors are reported separately.
"""

from __future__ import annotations

import argparse
import math
import random
import statistics
import sys
import time
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass

import grpc

from nidx.examples.lpg.cypher import Cypher, QueryLimitExceeded
from nidx.examples.lpg.storage import Graph, NidxClient, PropertyFilter, Record

CASE_NAMES = (
    "python/get",
    "python/find-name",
    "python/expand",
    "python/expand-batched",
    "python/expand-filtered",
    "python/shortest-adjacent",
    "python/shortest-filtered",
    "python/shortest-random",
    "python/search-text",
    "python/search-vector",
    "python/search-vector-exact",
    "python/search-hybrid",
    "cypher/lookup",
    "cypher/neighborhood",
    "cypher/traversal",
    "cypher/join",
    "cypher/relationship-filter",
    "cypher/aggregation",
    "cypher/shortest-adjacent",
    "cypher/shortest-random",
)


@dataclass(frozen=True)
class SearchChunk:
    node_id: str
    property: str
    chunk_index: int
    text: str
    vector: list[float] | None


@dataclass(frozen=True)
class Sample:
    product: Record
    warehouse: Record
    client: Record
    adjacent_warehouse: Record
    search_chunk: SearchChunk | None = None


@dataclass(frozen=True)
class Result:
    count: int
    hops: int | None = None
    examined_edges: int | None = None
    self_match_rank: int | None = None


@dataclass(frozen=True)
class Case:
    name: str
    run: Callable[[Sample], Result]


def name(record: Record) -> str:
    value = record.properties.get("name")
    if not isinstance(value, str):
        raise ValueError(f"Sampled node {record.id} has no string name")
    return value


def sample_chunk(
    graph: Graph, nodes: Sequence[Record], rng: random.Random, *, require_vector: bool
) -> SearchChunk:
    candidates = list(nodes)
    rng.shuffle(candidates)
    for node in candidates:
        # Record omits embeddings; fetch the canonical protobuf outside query timings.
        proto = graph._get_proto("node", node.id)
        if proto is None:
            raise RuntimeError(f"Sampled node {node.id} is no longer visible")
        chunks = [
            (key, index, text, list(prop.embeddings[index].values) if prop.embeddings else None)
            for key, prop in sorted(proto.text_properties.items())
            for index, text in enumerate(prop.chunks)
            if not require_vector or prop.embeddings
        ]
        if chunks:
            key, index, text, vector = rng.choice(chunks)
            return SearchChunk(node.id, key, index, text, vector)
    required = "stored embeddings" if require_vector else "text chunks"
    raise RuntimeError(f"Sampled nodes have no {required}; load a dataset with {required}")


def sample_graph(
    graph: Graph,
    count: int,
    rng: random.Random,
    *,
    sample_search: bool = False,
    sample_vectors: bool = False,
) -> list[Sample]:
    """Sample known ID ranges of the 500k-node synthetic dataset, outside timings."""
    samples = []
    if not 1 <= count <= 49999:
        raise ValueError("Sampling requires 1..49999 nodes per kind, including warmups")
    node_ids = zip(
        rng.sample(range(1, 350000), count),
        rng.sample(range(1, 50000), count),
        rng.sample(range(1, 100000), count),
    )
    for product_id, warehouse_id, client_id in node_ids:
        ids = (f"P{product_id:08d}", f"W{warehouse_id:08d}", f"C{client_id:08d}")
        records = [graph.get("node", id) for id in ids]
        if any(record is None for record in records):
            raise RuntimeError(f"Sampled node is missing or not yet visible: {ids}")
        product, warehouse, client = records
        assert product is not None and warehouse is not None and client is not None
        for record in (product, warehouse, client):
            name(record)
        expansion = graph.expand([product.id], direction="outgoing", edge_types=["STOCKED_AT"])
        if not expansion.matches:
            raise RuntimeError(f"Product {product.id} has no visible STOCKED_AT edges")
        neighbor = expansion.matches[0].neighbor
        if "Warehouse" not in neighbor.labels:
            raise RuntimeError(f"STOCKED_AT neighbor {neighbor.id} is not a warehouse")
        name(neighbor)
        search_chunk = (
            sample_chunk(graph, (product, warehouse, client), rng, require_vector=sample_vectors)
            if sample_search or sample_vectors
            else None
        )
        samples.append(Sample(product, warehouse, client, neighbor, search_chunk))
    return samples


def cases(graph: Graph, cypher: Cypher) -> list[Case]:
    def rows(query: str, parameters: dict[str, object]) -> Result:
        return Result(len(cypher.execute(query, parameters)))

    def expand(sample: Sample, *, filtered: bool = False, batched: bool = False) -> Result:
        expansion = graph.expand(
            [sample.product.id, sample.client.id] if batched else [sample.product.id],
            direction="both",
            edge_types=["STOCKED_AT"] if filtered else (),
            edge_filters=[PropertyFilter("confirmed", "eq", value=True)] if filtered else (),
            neighbor_label="Warehouse" if filtered else None,
        )
        return Result(len(expansion.matches), examined_edges=expansion.examined_edges)

    def shortest(sample: Sample, *, adjacent: bool, filtered: bool = False) -> Result:
        target = sample.adjacent_warehouse if adjacent else sample.warehouse
        path = graph.shortest_path(
            sample.product.id,
            target.id,
            direction="both",
            edge_types=["STOCKED_AT"],
            edge_filters=[PropertyFilter("confirmed", "eq", value=True)] if filtered else (),
            max_hops=6,
            max_visited_nodes=10_000,
            max_examined_edges=100_000,
        )
        if path is None:
            return Result(0)
        return Result(1, hops=len(path.edges), examined_edges=path.examined_edges)

    def search(
        sample: Sample, *, text: bool = False, vector: bool = False, exact: bool = False
    ) -> Result:
        target = sample.search_chunk
        if target is None:
            raise RuntimeError("Search case requires a sampled property chunk")
        if vector and target.vector is None:
            raise RuntimeError("Vector search requires a sampled stored embedding")
        hits = graph.search(
            text=target.text if text else None,
            vector=target.vector if vector else None,
            kind="node",
            text_property=target.property,
            top_k=10,
            min_score_semantic=0.99 if exact else 0.0,
        )
        for rank, hit in enumerate(hits, start=1):
            if (
                hit.record.id == target.node_id
                and hit.property == target.property
                and hit.chunk_index == target.chunk_index
                and (hit.vector_rank is not None if vector else hit.text_rank is not None)
            ):
                return Result(len(hits), self_match_rank=rank)
        raise RuntimeError(
            f"Search self-match missing from {len(hits)} hits: "
            f"node={target.node_id} property={target.property!r} chunk={target.chunk_index}"
        )

    return [
        Case("python/get", lambda s: Result(int(graph.get("node", s.product.id) is not None))),
        Case(
            "python/find-name",
            lambda s: Result(len(graph.find("node", PropertyFilter("name", "eq", name(s.product))))),
        ),
        Case("python/expand", lambda s: expand(s)),
        Case("python/expand-batched", lambda s: expand(s, batched=True)),
        Case("python/expand-filtered", lambda s: expand(s, filtered=True)),
        Case("python/shortest-adjacent", lambda s: shortest(s, adjacent=True)),
        Case("python/shortest-filtered", lambda s: shortest(s, adjacent=True, filtered=True)),
        Case("python/shortest-random", lambda s: shortest(s, adjacent=False)),
        Case("python/search-text", lambda s: search(s, text=True)),
        Case("python/search-vector", lambda s: search(s, vector=True)),
        Case("python/search-vector-exact", lambda s: search(s, vector=True, exact=True)),
        Case("python/search-hybrid", lambda s: search(s, text=True, vector=True)),
        Case(
            "cypher/lookup",
            lambda s: rows("MATCH (p:Product {name: $name}) RETURN p", {"name": name(s.product)}),
        ),
        Case(
            "cypher/neighborhood",
            lambda s: rows(
                "MATCH (p:Product {name: $name})-[r]-(neighbor) RETURN p, r, neighbor LIMIT 100",
                {"name": name(s.product)},
            ),
        ),
        Case(
            "cypher/traversal",
            lambda s: rows(
                "MATCH path = (p:Product {name: $name})-[:STOCKED_AT|SUPPLIES|BUYS*1..3]->"
                "(other) RETURN path LIMIT 100",
                {"name": name(s.product)},
            ),
        ),
        Case(
            "cypher/join",
            lambda s: rows(
                "MATCH (c:Client)-[:BUYS]->(p:Product)<-[:BUYS]-(other:Client) "
                "WHERE c.name = $name RETURN c, p, other",
                {"name": name(s.client)},
            ),
        ),
        Case(
            "cypher/relationship-filter",
            lambda s: rows(
                "MATCH (p:Product)-[r:STOCKED_AT]->(w:Warehouse) "
                "WHERE r.quantity >= $minimum RETURN p, r, w",
                {"minimum": 990},
            ),
        ),
        Case(
            "cypher/aggregation",
            lambda s: rows(
                "MATCH (w:Warehouse)<-[:STOCKED_AT]-(p:Product) "
                "RETURN w.name, count(p) AS products ORDER BY products DESC LIMIT 10",
                {},
            ),
        ),
        Case(
            "cypher/shortest-adjacent",
            lambda s: rows(
                "MATCH path = shortestPath((p:Product {name: $start})-[:STOCKED_AT*..6]-"
                "(w:Warehouse {name: $end})) RETURN path",
                {"start": name(s.product), "end": name(s.adjacent_warehouse)},
            ),
        ),
        Case(
            "cypher/shortest-random",
            lambda s: rows(
                "MATCH path = shortestPath((p:Product {name: $start})-[:STOCKED_AT*..6]-"
                "(w:Warehouse {name: $end})) RETURN path",
                {"start": name(s.product), "end": name(s.warehouse)},
            ),
        ),
    ]


def percentile(values: Sequence[float], percentile_value: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile_value
    lower = math.floor(position)
    upper = math.ceil(position)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def select_cases(available: Sequence[Case], selected: Sequence[str] | None) -> list[Case]:
    if not selected:
        return list(available)
    by_name = {case.name: case for case in available}
    unknown = [prefix for prefix in selected if not any(name.startswith(prefix) for name in by_name)]
    if unknown:
        raise ValueError(f"Unknown case(s): {', '.join(unknown)}. Available: {', '.join(by_name)}")
    if len(set(selected)) != len(selected):
        raise ValueError("A case may only be selected once")
    result: list[Case] = []
    seen: set[str] = set()
    for prefix in selected:
        matches = (
            [by_name[prefix]]
            if prefix in by_name
            else [case for case in available if case.name.startswith(prefix)]
        )
        for case in matches:
            if case.name not in seen:
                result.append(case)
                seen.add(case.name)
    return result


def error_description(error: Exception) -> str:
    if isinstance(error, grpc.RpcError):
        return f"{error.code().name}: {error.details()}"
    return f"{type(error).__name__}: {error}"


def benchmark(case: Case, samples: Sequence[Sample], warmups: int) -> None:
    errors: Counter[str] = Counter()
    elapsed: list[float] = []
    results: list[Result] = []
    measured_count = len(samples) - warmups
    if warmups < 0 or measured_count < 1:
        raise ValueError("Provide at least one measured sample after warmups")
    for index, sample in enumerate(samples):
        started = time.perf_counter()
        try:
            result = case.run(sample)
        except (grpc.RpcError, QueryLimitExceeded, RuntimeError) as error:
            if index >= warmups:
                errors[error_description(error)] += 1
            else:
                print(f"  warmup failed: {error_description(error)}", file=sys.stderr, flush=True)
            continue
        duration_ms = (time.perf_counter() - started) * 1000
        if index >= warmups:
            elapsed.append(duration_ms)
            results.append(result)
    if elapsed:
        counts = [result.count for result in results]
        print(
            f"{case.name}: {len(elapsed)}/{measured_count} ok; "
            f"p50={statistics.median(elapsed):.1f} ms p95={percentile(elapsed, 0.95):.1f} ms "
            f"min={min(elapsed):.1f} ms max={max(elapsed):.1f} ms; "
            f"results={min(counts)}..{max(counts)}"
        )
        hops = [result.hops for result in results if result.hops is not None]
        examined = [result.examined_edges for result in results if result.examined_edges is not None]
        if case.name.startswith("python/shortest"):
            detail = f"; hops={min(hops)}..{max(hops)}" if hops else ""
            print(f"  found paths={len(hops)}/{len(results)}{detail}")
        if examined:
            print(f"  examined edges={min(examined)}..{max(examined)}")
        ranks = [result.self_match_rank for result in results if result.self_match_rank is not None]
        if ranks:
            print(f"  self-match rank={min(ranks)}..{max(ranks)}; matched={len(ranks)}/{measured_count}")
    else:
        print(f"{case.name}: 0/{measured_count} ok; no successful latency")
    for error, count in errors.items():
        print(f"  {count} failed: {error}")
    sys.stdout.flush()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", default="127.0.0.1:10000", help="Nidx API gRPC address")
    parser.add_argument("--graph-id", required=True, help="Existing LPG shard UUID")
    parser.add_argument("--tenant", required=True, help="Shard tenant UUID")
    parser.add_argument("--iterations", type=int, default=5, help="Samples per case (default: 5)")
    parser.add_argument("--warmups", type=int, default=1, help="Untimed calls per case (default: 1)")
    parser.add_argument("--seed", type=int, default=42, help="Sampling seed (default: 42)")
    parser.add_argument(
        "--case",
        nargs="+",
        action="extend",
        metavar="NAME_OR_PREFIX",
        help=(
            "Choose one or more exact case names or prefixes, e.g. python/ or cypher "
            f"(default: all). Available: {', '.join(CASE_NAMES)}"
        ),
    )
    parser.add_argument(
        "--vector-dimension", type=int, default=384, help="Vector dimension (default: 384)"
    )
    args = parser.parse_args()
    if args.iterations < 1 or args.warmups < 0 or not 1 <= args.vector_dimension <= 4096:
        parser.error("iterations must be positive, warmups nonnegative, and vector dimension 1..4096")
    if args.iterations + args.warmups > 49999:
        parser.error("iterations plus warmups must not exceed 49999")

    rng = random.Random(args.seed)
    client = NidxClient(args.address)
    try:
        graph = Graph(client, args.tenant, args.graph_id, vector_dimension=args.vector_dimension)
        cypher = Cypher(client.lpg_client, args.graph_id, max_records=1000, max_rpcs=50, timeout=15)
        try:
            selected = select_cases(cases(graph, cypher), args.case)
        except ValueError as error:
            parser.error(str(error))
        print("Sampling existing nodes and edges (not timed)...", file=sys.stderr, flush=True)
        samples = sample_graph(
            graph,
            args.iterations + args.warmups,
            rng,
            sample_search=any(case.name.startswith("python/search-") for case in selected),
            sample_vectors=any(
                case.name
                in ("python/search-vector", "python/search-vector-exact", "python/search-hybrid")
                for case in selected
            ),
        )
        print(
            f"graph={args.graph_id} tenant={args.tenant} address={args.address} "
            f"iterations={args.iterations} warmups={args.warmups} seed={args.seed}"
        )
        print(
            "Single client, warm cache; times include Python and RPC work; "
            "searches use sampled property chunks and stored embeddings; "
            "distinct nodes per call within each case."
        )
        for case in selected:
            print(f"Running {case.name}...", file=sys.stderr, flush=True)
            benchmark(case, samples, args.warmups)
    finally:
        client.close()


if __name__ == "__main__":
    main()
