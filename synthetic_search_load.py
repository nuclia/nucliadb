"""Run weighted internal search operations against one or more KBs, without HTTP.

Example:
    python synthetic_search_load.py --kbid UUID \
        --start-rps 4 --max-rps 20 --rps-step 4 --stage-seconds 180

    python synthetic_search_load.py --kbid UUID1 --kbid UUID2 \
        --start-rps 4 --max-rps 20

    python synthetic_search_load.py --kb-prefix ml-load-test \
        --start-rps 4 --max-rps 20

Requests are generated with Faker and validated through the endpoint request models.
Run inside a configured NucliaDB container. This includes parsing, retrieval,
hydration and any configured prediction calls, but excludes HTTP and auth costs.

###
python /synthetic_search_load.py --kbid=28d547f3-5a11-5d7f-928e-ead1615d4b5e --start-rps=2 --max-rps=20 --stage-seconds=60

python /synthetic_search_load.py --kbid=28d547f3-5a11-5d7f-928e-ead1615d4b5e --start-rps=6 --max-rps=6 --stage-seconds=600
###

"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import random
import time
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from faker import Faker

from nucliadb.common import datamanagers
from nucliadb.common.context import ApplicationContext
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.common.marklogic import documents
from nucliadb.models.responses import HTTPClientError
from nucliadb.search.api.v1.catalog import catalog
from nucliadb.search.api.v1.graph import graph_path_search
from nucliadb.search.api.v1.retrieve import retrieve_endpoint
from nucliadb.search.predict import start_predict_engine, stop_predict_engine
from nucliadb.search.search.cache import request_caches
from nucliadb.search.search.find import find
from nucliadb.search.search.metrics import Metrics
from nucliadb_models.graph.requests import GraphSearchRequest
from nucliadb_models.retrieval import Query, RawQuery, RetrievalRequest, SemanticOverrides
from nucliadb_models.search import CatalogRequest, FindOptions, FindRequest, NucliaDBClientType

ENDPOINT_PROBABILITIES = {
    # "find_keyword": 0.0,
    "find_semantic": 1,
    # "find_hybrid": 0.0,
    # "catalog": 0.2,
}
DEFAULT_REQUEST_POOL_SIZE = 32
REQUEST_MODELS = {
    # "find_keyword": FindRequest,
    "find_semantic": FindRequest,
    # "find_hybrid": FindRequest,
    # "retrieve": RetrievalRequest,
    # "catalog": CatalogRequest,
    # "graph": GraphSearchRequest,
}


async def resolve_kbids(driver: Any, args: argparse.Namespace) -> list[str]:
    if args.kbid:
        return args.kbid

    async with driver.ro_transaction(kbid=None) as txn:
        kb_map = {
            slug: kbid
            async for kbid, slug in datamanagers.kb.iter(
                txn, slug_prefix=f"{args.kb_prefix}-"
            )
        }
    if not kb_map:
        raise ValueError(f"No Knowledge Boxes found with slug prefix {args.kb_prefix!r}")
    print(
        "Resolved Knowledge Boxes: "
        + ", ".join(f"{slug} ({kb_map[slug]})" for slug in sorted(kb_map)),
        flush=True,
    )
    return [kb_map[slug] for slug in sorted(kb_map)]


def select_active_kbids(kbids: list[str], active_kbs: int | None, seed: int) -> list[str]:
    if active_kbs is None or active_kbs >= len(kbids):
        return kbids
    if active_kbs <= 0:
        raise ValueError("--active-kbs must be positive")
    selected = set(random.Random(seed).sample(kbids, active_kbs))
    return [kbid for kbid in kbids if kbid in selected]


def generate_request(operation: str, fake: Faker, rng: random.Random) -> Any:
    nb_words = rng.randint(5, 20)
    query = fake.sentence(nb_words=nb_words).rstrip(".")
    if operation in ("find_keyword", "find_semantic", "find_hybrid"):
        features = {
            "find_keyword": [FindOptions.KEYWORD.value],
            "find_semantic": [FindOptions.SEMANTIC.value],
            "find_hybrid": [FindOptions.KEYWORD.value, FindOptions.SEMANTIC.value],
        }[operation]
        request = {
            "query": query,
            "vectorset": "synthetic-vectors",
            "min_score": 0.0,
            "rephrase": False,
            "reranker": "noop",
            "features": features,
            "top_k": 20,
        }
        if FindOptions.SEMANTIC.value in features:
            request["vector"] = [rng.uniform(-1.0, 1.0) for _ in range(768)]
        return FindRequest.model_validate(request)
    if operation == "retrieve":
        random_vector = [rng.uniform(-1.0, 1.0) for _ in range(768)]
        return RetrievalRequest.model_validate({"query": {
        "keyword": {"query": query},
        "semantic": {"query": random_vector, "vectorset": "synthetic-vectors"}
                                                          }, "top_k": 10})
    if operation == "catalog":
        return CatalogRequest.model_validate({"query": query, "page_size": 10})
    if operation == "graph":
        return GraphSearchRequest.model_validate(
            {
                "query": {
                    "prop": "path",
                    "source": {"value": fake.word()},
                    "relation": {"label": fake.word()},
                    "destination": {"value": fake.word()},
                }
            }
        )
    raise ValueError(f"Unknown operation: {operation}")


def prepare_request(
    template: Any,
    rng: random.Random,
    vector_dimension: int,
    vectorset: str | None,
) -> Any:
    item = template.model_copy(deep=True)
    semantic_find = isinstance(item, FindRequest) and FindOptions.SEMANTIC in item.features
    semantic_retrieve = isinstance(item, RetrievalRequest) and (
        (isinstance(item.query, RawQuery) and item.query.semantic is not None)
        or (isinstance(item.query, Query) and item.query.override.semantic != "disabled")
    )
    if not semantic_find and not semantic_retrieve:
        return item
    vector = [rng.uniform(-1.0, 1.0) for _ in range(vector_dimension)]
    norm = math.sqrt(sum(value * value for value in vector)) or 1.0
    vector = [value / norm for value in vector]
    if isinstance(item, FindRequest):
        item.vector = vector
        if vectorset is not None:
            item.vectorset = vectorset
    elif isinstance(item.query, RawQuery):
        assert item.query.semantic is not None
        item.query.semantic.query = vector
        if vectorset is not None:
            item.query.semantic.vectorset = vectorset
    else:
        override = item.query.override.semantic
        if not isinstance(override, SemanticOverrides):
            override = SemanticOverrides()
            item.query.override.semantic = override
        override.vector = vector
        if vectorset is not None:
            override.vectorset = vectorset
    return item


def create_request_pool(
    fake: Faker,
    rng: random.Random,
    pool_size: int,
    vector_dimension: int,
    vectorset: str | None,
) -> dict[str, list[Any]]:
    return {
        operation: [
            prepare_request(
                generate_request(operation, fake, rng), rng, vector_dimension, vectorset
            )
            for _ in range(pool_size)
        ]
        for operation in ENDPOINT_PROBABILITIES
    }


async def execute_operation(kbid: str, operation: str, item: Any) -> dict[str, int | float | None] | None:
    client = {
        "x_ndb_client": NucliaDBClientType.API,
        "x_nucliadb_user": "search-load-test",
        "x_forwarded_for": "",
    }
    with request_caches():
        if operation.startswith("find_"):
            metrics = Metrics("search-load")
            _, incomplete, _ = await find(kbid, item, **client, metrics=metrics)
            if incomplete:
                raise RuntimeError("Find returned incomplete results")
            return dict(metrics.to_dict())
        if operation == "retrieve":
            result = await retrieve_endpoint(kbid, item, **client)
        elif operation == "catalog":
            result = await catalog(kbid, item)
        elif operation == "graph":
            result = await graph_path_search(kbid, item)
        else:
            raise ValueError(f"Unknown operation: {operation}")
        if isinstance(result, HTTPClientError):
            raise RuntimeError(f"{operation}: status={result.status_code} detail={result.detail}")


def measurement_summary(
    counts: Counter, samples: list[dict[str, Any]], seconds: float
) -> dict[str, Any]:
    successful = sorted(sample["seconds"] for sample in samples if sample["outcome"] == "success")
    metric_names = sorted({name for sample in samples for name in sample.get("metrics", {})})

    def percentile(values: list[float], fraction: float) -> float | None:
        if not values:
            return None
        return round(values[max(0, math.ceil(len(values) * fraction) - 1)], 6)

    metric_latencies = {}
    for name in metric_names:
        values = sorted(
            sample["metrics"][name]
            for sample in samples
            if sample.get("metrics", {}).get(name) is not None
        )
        metric_latencies[name] = {
            "mean_seconds": round(sum(values) / len(values), 6) if values else None,
            **{
                f"p{percent}_seconds": percentile(values, percent / 100)
                for percent in (50, 90, 95, 99)
            },
        }

    return {
        **{
            key: counts[key]
            for key in ("offered", "started", "success", "failed", "cancelled", "dropped")
        },
        "success_rps": round(counts["success"] / max(seconds, 1e-9), 3),
        "completed_rps": round((counts["success"] + counts["failed"]) / max(seconds, 1e-9), 3),
        "latency_mean_seconds": round(sum(successful) / len(successful), 6) if successful else None,
        "latency_p50_seconds": percentile(successful, 0.50),
        "latency_p90_seconds": percentile(successful, 0.90),
        "latency_p95_seconds": percentile(successful, 0.95),
        "latency_p99_seconds": percentile(successful, 0.99),
        "metric_latencies": metric_latencies,
    }


def format_latency(seconds: float | None) -> str:
    return "-" if seconds is None else f"{seconds * 1000:.1f} ms"


def format_metric_latencies(report: dict[str, Any]) -> str:
    return " ".join(
        f"{name} p50/p95/p99="
        + "/".join(format_latency(values.get(f"p{percent}_seconds")) for percent in (50, 95, 99))
        for name, values in sorted(report.get("metric_latencies", {}).items())
    )


def format_report_lines(report: dict[str, Any], timestamp: str) -> list[str]:
    event = report["event"]
    target_rps = report["target_rps"]
    phase = report.get("phase", "complete")
    duration = report.get("window_seconds", report.get("elapsed_seconds", 0.0))
    overall = (
        f"{timestamp} {event} ({phase}) target={target_rps:g} req/s "
        f"actual={report['success_rps']:.2f} success/s "
        f"completed={report['completed_rps']:.2f}/s "
        f"latency p50/p90/p95/p99="
        f"{format_latency(report['latency_p50_seconds'])}/"
        f"{format_latency(report['latency_p90_seconds'])}/"
        f"{format_latency(report['latency_p95_seconds'])}/"
        f"{format_latency(report['latency_p99_seconds'])} "
        f"{format_metric_latencies(report)} "
        f"ok={report['success']} failed={report['failed']} "
        f"dropped={report['dropped']} cancelled={report['cancelled']} "
        f"in-flight={report.get('in_flight', 0)} window={duration:.1f}s"
    )
    lines = [overall]
    lines.extend(
        f"  {operation:<14} {metrics['success_rps']:>6.2f} success/s "
        f"(ok={metrics['success']}, failed={metrics['failed']}, dropped={metrics['dropped']}) "
        f"p50={format_latency(metrics['latency_p50_seconds'])} "
        f"p95={format_latency(metrics['latency_p95_seconds'])} "
        f"p99={format_latency(metrics['latency_p99_seconds'])} "
        f"{format_metric_latencies(metrics)}"
        for operation, metrics in report["operations"].items()
    )
    return lines


def format_configuration_line(
    settings: dict[str, Any], probabilities: dict[str, float]
) -> str:
    explicit_kbids = settings.get("kbid") or []
    if settings.get("kb_prefix"):
        selection = f"prefix={settings['kb_prefix']} (resolving)"
    else:
        selection = f"{len(explicit_kbids)} explicit KBs"
    active_kbs = settings.get("active_kbs")
    active_label = str(active_kbs) if active_kbs is not None else "all resolved"
    operations = ", ".join(
        f"{name} {probability:.0%}" for name, probability in probabilities.items() if probability
    )
    return (
        f"Search load started: selection={selection} active-kbs={active_label} "
        f"rate={settings['start_rps']:g}-{settings['max_rps']:g} req/s "
        f"stage={settings['stage_seconds']:g}s operations={operations}"
    )


def format_final_summary(
    stages: list[dict[str, Any]], kb_chunk_counts: list[dict[str, int | str]] | None = None
) -> str:
    metric_names = sorted({name for stage in stages for name in stage.get("metric_latencies", {})})
    columns = ("Target", "Success/s", "Failure/s", "Dropped/s", "p50", "p95") + tuple(
        f"{name} p{percent}" for name in metric_names for percent in (50, 95)
    )
    rows = [
        (
            f"{stage['target_rps']:g} req/s",
            f"{stage['success'] / max(stage['elapsed_seconds'], 1e-9):.2f}",
            f"{stage['failed'] / max(stage['elapsed_seconds'], 1e-9):.2f}",
            f"{stage['dropped'] / max(stage['elapsed_seconds'], 1e-9):.2f}",
            format_latency(stage["latency_p50_seconds"]),
            format_latency(stage["latency_p95_seconds"]),
            *(
                format_latency(
                    stage.get("metric_latencies", {}).get(name, {}).get(f"p{percent}_seconds")
                )
                for name in metric_names
                for percent in (50, 95)
            ),
        )
        for stage in stages
    ]
    widths = [
        max(len(columns[index]), *(len(row[index]) for row in rows))
        if rows
        else len(columns[index])
        for index in range(len(columns))
    ]
    header = "  ".join(column.ljust(widths[index]) for index, column in enumerate(columns))
    separator = "  ".join("-" * width for width in widths)
    lines = ["Final search load summary"]
    if kb_chunk_counts is not None:
        lines.extend(("Active KB sizes by chunk count", "  KBID                                Chunks"))
        lines.extend(
            f"  {item['kbid']:<36} {item['chunks']:>12,}"
            for item in kb_chunk_counts
        )
        lines.append("")
    lines.extend((header, separator))
    lines.extend("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)) for row in rows)
    if not rows:
        lines.append("No completed stages")
    return "\n".join(lines)


async def count_kb_chunks(driver: MarkLogicDriver, kbid: str) -> int:
    collection = json.dumps(MarkLogicCollections.CHUNKS.value)
    javascript = (
        "fn.count(cts.uris('', ['document'], "
        f"cts.collectionQuery({collection})))"
    )
    async with driver.ro_transaction(kbid=kbid) as txn:
        result = await documents.evaluate(txn, javascript)
    if len(result) != 1 or type(result[0]) is not int or result[0] < 0:
        raise ValueError(f"Invalid chunk count for KB {kbid}: {result!r}")
    return result[0]


async def run_stage(
    args: argparse.Namespace,
    target_rps: float,
    rng: random.Random,
    request_pool: dict[str, list[Any]],
    emit: Callable[[dict[str, Any]], None],
    execute: Callable[[str, str, Any], Awaitable[dict[str, int | float | None] | None]] = execute_operation,
) -> dict[str, Any]:
    started = time.monotonic()
    deadline = started + args.stage_seconds
    counts: Counter = Counter()
    operation_counts = {operation: Counter() for operation in ENDPOINT_PROBABILITIES}
    samples: list[dict[str, Any]] = []
    pending: set[asyncio.Task] = set()
    previous_counts: Counter = Counter()
    previous_operation_counts = {operation: Counter() for operation in ENDPOINT_PROBABILITIES}
    previous_sample = 0
    previous_time = started
    operations = list(ENDPOINT_PROBABILITIES)
    weights = list(ENDPOINT_PROBABILITIES.values())

    def increment(operation: str, key: str) -> None:
        counts[key] += 1
        operation_counts[operation][key] += 1

    async def worker(kbid: str, operation: str, item: Any) -> None:
        request_started = time.monotonic()
        outcome = "success"
        error = None
        timings = {}
        try:
            async with asyncio.timeout(args.request_timeout):
                timings = dict(await execute(kbid, operation, item) or {})
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except Exception as exc:
            outcome = "failed"
            error = f"{type(exc).__name__}: {exc}"
            print(f"Request failed: kbid={kbid} operation={operation} error={error}", flush=True)
            #traceback.print_exc()
        finally:
            increment(operation, outcome)
            samples.append(
                {
                    "operation": operation,
                    "outcome": outcome,
                    "seconds": time.monotonic() - request_started,
                    "stage_elapsed_seconds": time.monotonic() - started,
                    "error": error,
                    "metrics": timings,
                }
            )

    def report_interval(*, final: bool = False) -> None:
        nonlocal previous_counts, previous_operation_counts, previous_sample, previous_time
        now = time.monotonic()
        seconds = now - previous_time
        window_samples = samples[previous_sample:]
        emit(
            {
                "event": "interval",
                "target_rps": target_rps,
                "phase": "load" if now < deadline else "drain",
                "stage_elapsed_seconds": now - started,
                "window_seconds": seconds,
                "final_interval": final,
                "in_flight": len(pending),
                **measurement_summary(counts - previous_counts, window_samples, seconds),
                "operations": {
                    operation: measurement_summary(
                        operation_counts[operation] - previous_operation_counts[operation],
                        [sample for sample in window_samples if sample["operation"] == operation],
                        seconds,
                    )
                    for operation in operations
                },
                "latency_samples": window_samples,
            }
        )
        previous_counts = counts.copy()
        previous_operation_counts = {
            operation: value.copy() for operation, value in operation_counts.items()
        }
        previous_sample = len(samples)
        previous_time = now

    async def reporter() -> None:
        while True:
            await asyncio.sleep(args.report_interval)
            report_interval()

    progress = asyncio.create_task(reporter())
    interrupted = True
    emit(
        {
            "event": "stage_start",
            "target_rps": target_rps,
            "stage_seconds": args.stage_seconds,
            "kbids": args.kbid,
        }
    )
    try:
        slots = math.ceil(target_rps * args.stage_seconds)
        for index in range(slots):
            due = started + index / target_rps
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            kbid = args.kbid[index % len(args.kbid)]
            operation = rng.choices(operations, weights=weights)[0]
            increment(operation, "offered")
            if time.monotonic() - due >= 1 / target_rps or len(pending) >= args.max_in_flight:
                increment(operation, "dropped")
                continue
            item = rng.choice(request_pool[operation]).model_copy(deep=True)
            increment(operation, "started")
            task = asyncio.create_task(worker(kbid, operation, item))
            pending.add(task)
            task.add_done_callback(pending.discard)
        await asyncio.sleep(max(0.0, deadline - time.monotonic()))
        if pending:
            await asyncio.gather(*list(pending))
        interrupted = False
    finally:
        progress.cancel()
        remaining = list(pending)
        for task in remaining:
            task.cancel()
        await asyncio.gather(progress, *remaining, return_exceptions=True)
        report_interval(final=True)
        elapsed = time.monotonic() - started
        result = {
            "event": "stage",
            "target_rps": target_rps,
            "kbids": args.kbid,
            "elapsed_seconds": elapsed,
            "drain_seconds": max(0.0, elapsed - args.stage_seconds),
            "interrupted": interrupted,
            **measurement_summary(counts, samples, elapsed),
            "operations": {
                operation: measurement_summary(
                    operation_counts[operation],
                    [sample for sample in samples if sample["operation"] == operation],
                    elapsed,
                )
                for operation in operations
            },
        }
        emit(result)
    return result


async def run(args: argparse.Namespace) -> None:
    rng = random.Random(args.seed)
    fake = Faker()
    fake.seed_instance(args.seed)
    request_pool = create_request_pool(
        fake,
        rng,
        args.request_pool_size,
        args.vector_dimension,
        args.vectorset,
    )
    if args.dry_run:
        print(
            json.dumps(
                {
                    "probabilities": ENDPOINT_PROBABILITIES,
                    "request_pool_size_per_endpoint": args.request_pool_size,
                    "example_requests": {
                        operation: requests[0].model_dump(mode="json", exclude_none=True)
                        for operation, requests in request_pool.items()
                    },
                },
                indent=2,
            )
        )
        return
    run_id = uuid.uuid4().hex
    started = time.monotonic()
    stages = []
    kb_chunk_counts: list[dict[str, int | str]] | None = None
    with Path(args.report_file).open("a") as output:

        def emit(report: dict[str, Any]) -> None:
            timestamp = datetime.now(timezone.utc).isoformat()
            record = {
                "run_id": run_id,
                "timestamp": timestamp,
                "run_elapsed_seconds": time.monotonic() - started,
                **report,
            }
            output.write(json.dumps(record) + "\n")
            output.flush()
            if report["event"] in ("interval", "stage"):
                for line in format_report_lines(report, timestamp):
                    print(line, flush=True)
            elif report["event"] == "configuration":
                print(format_configuration_line(report["settings"], report["probabilities"]), flush=True)
            elif report["event"] == "stage_start":
                print(
                    f"Starting stage: target={report['target_rps']:g} req/s "
                    f"duration={report['stage_seconds']:g}s KBs={len(report['kbids'])}",
                    flush=True,
                )
            elif report["event"] == "summary":
                print(
                    format_final_summary(report["stages"], report.get("kb_chunk_counts")),
                    flush=True,
                )
            else:
                print(f"{timestamp} {report['event']}", flush=True)
            if report["event"] == "stage":
                stages.append(report)

        emit(
            {
                "event": "configuration",
                "settings": vars(args),
                "probabilities": ENDPOINT_PROBABILITIES,
            }
        )
        try:
            ctx = ApplicationContext(nats_manager=False, partitioning=False, transaction=False)
            await ctx.initialize()
            resolved_kbids = await resolve_kbids(ctx.kv_driver, args)
            if args.active_kbs is not None and args.active_kbs > len(resolved_kbids):
                raise ValueError(
                    f"--active-kbs ({args.active_kbs}) exceeds the number of resolved KBs "
                    f"({len(resolved_kbids)})"
                )
            args.kbid = select_active_kbids(resolved_kbids, args.active_kbs, args.seed)
            print(
                f"Using {len(args.kbid)} active KBs: {', '.join(args.kbid)}",
                flush=True,
            )
            if args.active_kbs is not None:
                if not isinstance(ctx.kv_driver, MarkLogicDriver):
                    raise TypeError("--active-kbs chunk counts require MarkLogicDriver")
                kb_chunk_counts = []
                print("Counting chunks in selected active KBs", flush=True)
                for kbid in args.kbid:
                    chunk_count = await count_kb_chunks(ctx.kv_driver, kbid)
                    kb_chunk_counts.append({"kbid": kbid, "chunks": chunk_count})
                    print(f"  KB {kbid}: {chunk_count:,} chunks", flush=True)
            await start_predict_engine()
            rate = args.start_rps
            while True:
                await run_stage(args, rate, rng, request_pool, emit)
                if rate >= args.max_rps:
                    break
                rate = min(args.max_rps, rate + args.rps_step)
        finally:
            emit(
                {
                    "event": "summary",
                    "stages": stages,
                    "kb_chunk_counts": kb_chunk_counts,
                }
            )
            try:
                await stop_predict_engine()
            finally:
                await ctx.finalize()
    if any(stage["failed"] or stage["dropped"] for stage in stages):
        raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    kb_selection = parser.add_mutually_exclusive_group(required=True)
    kb_selection.add_argument(
        "--kbid",
        action="append",
        help="Knowledge box ID; repeat this option to distribute requests across KBs",
    )
    kb_selection.add_argument(
        "--kb-prefix",
        help="Resolve all KBs whose slug starts with this prefix plus a hyphen",
    )
    parser.add_argument(
        "--active-kbs",
        type=int,
        help="Select this many KBs from the resolved set; selection is stable for a given seed",
    )
    parser.add_argument("--start-rps", type=float, default=4)
    parser.add_argument("--max-rps", type=float, default=20)
    parser.add_argument("--rps-step", type=float, default=4)
    parser.add_argument("--stage-seconds", type=float, default=120)
    parser.add_argument("--max-in-flight", type=int, default=100)
    parser.add_argument("--request-timeout", type=float, default=30)
    parser.add_argument("--report-interval", type=float, default=10)
    parser.add_argument("--report-file", default="search-load.jsonl")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--request-pool-size",
        type=int,
        default=DEFAULT_REQUEST_POOL_SIZE,
        help="Number of fixed Faker-generated requests per endpoint, reused for all stages",
    )
    parser.add_argument(
        "--vector-dimension",
        type=int,
        default=768,
        help="Random semantic query vector dimension; must match the KB index",
    )
    parser.add_argument(
        "--vectorset", help="Override the semantic vectorset; otherwise use the request or KB default"
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if (
        any(
            not math.isfinite(value) or value <= 0
            for value in (
                args.start_rps,
                args.max_rps,
                args.rps_step,
                args.stage_seconds,
                args.request_timeout,
                args.report_interval,
            )
        )
        or args.max_in_flight < 1
        or args.request_pool_size < 1
        or args.vector_dimension < 1
    ):
        parser.error("Rates, durations, pool size, and concurrency must be positive and finite")
    if args.max_rps < args.start_rps:
        parser.error("--max-rps must be >= --start-rps")
    if args.active_kbs is not None and args.active_kbs < 1:
        parser.error("--active-kbs must be positive")
    if args.kbid and len(set(args.kbid)) != len(args.kbid):
        parser.error("--kbid values must be unique")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print(
            "Interrupted; completed latency samples and partial stages saved to " + args.report_file,
            flush=True,
        )


if __name__ == "__main__":
    main()
