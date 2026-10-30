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
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import random
import time
import traceback
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from faker import Faker

from nucliadb.common import datamanagers
from nucliadb.common.context import ApplicationContext
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
    "find_keyword": 0.0,
    "find_semantic": 0.8,
    "find_hybrid": 0.0,
    "catalog": 0.2,
}
DEFAULT_REQUEST_POOL_SIZE = 32
REQUEST_MODELS = {
    "find_keyword": FindRequest,
    "find_semantic": FindRequest,
    "find_hybrid": FindRequest,
    "retrieve": RetrievalRequest,
    "catalog": CatalogRequest,
    "graph": GraphSearchRequest,
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
            "min_score": 0.3,
            "rephrase": False,
            "reranker": "noop",
            "features": features,
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


async def execute_operation(kbid: str, operation: str, item: Any) -> None:
    client = {
        "x_ndb_client": NucliaDBClientType.API,
        "x_nucliadb_user": "search-load-test",
        "x_forwarded_for": "",
    }
    with request_caches():
        if operation.startswith("find_"):
            #breakpoint()
            _, incomplete, _ = await find(kbid, item, **client, metrics=Metrics("search-load"))
            if incomplete:
                raise RuntimeError("Find returned incomplete results")
            return
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

    def percentile(fraction: float) -> float | None:
        if not successful:
            return None
        return round(successful[max(0, math.ceil(len(successful) * fraction) - 1)], 6)

    return {
        **{
            key: counts[key]
            for key in ("offered", "started", "success", "failed", "cancelled", "dropped")
        },
        "success_rps": round(counts["success"] / max(seconds, 1e-9), 3),
        "completed_rps": round((counts["success"] + counts["failed"]) / max(seconds, 1e-9), 3),
        "latency_mean_seconds": round(sum(successful) / len(successful), 6) if successful else None,
        "latency_p50_seconds": percentile(0.50),
        "latency_p90_seconds": percentile(0.90),
        "latency_p95_seconds": percentile(0.95),
        "latency_p99_seconds": percentile(0.99),
    }


async def run_stage(
    args: argparse.Namespace,
    target_rps: float,
    rng: random.Random,
    request_pool: dict[str, list[Any]],
    emit: Callable[[dict[str, Any]], None],
    execute: Callable[[str, str, Any], Awaitable[None]] = execute_operation,
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
        try:
            async with asyncio.timeout(args.request_timeout):
                await execute(kbid, operation, item)
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except Exception as exc:
            outcome = "failed"
            error = f"{type(exc).__name__}: {exc}"
            print(f"Request failed: kbid={kbid} operation={operation} error={error}", flush=True)
            traceback.print_exc()
        finally:
            increment(operation, outcome)
            samples.append(
                {
                    "operation": operation,
                    "outcome": outcome,
                    "seconds": time.monotonic() - request_started,
                    "stage_elapsed_seconds": time.monotonic() - started,
                    "error": error,
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
                print(
                    f"{timestamp} event={report['event']} target={report['target_rps']:g} req/s "
                    f"total_success_rps={report['success_rps']:.3f} "
                    f"p50={report['latency_p50_seconds']}s p95={report['latency_p95_seconds']}s "
                    f"p99={report['latency_p99_seconds']}s failed={report['failed']} "
                    f"dropped={report['dropped']} cancelled={report['cancelled']}",
                    flush=True,
                )
                for operation, metrics in report["operations"].items():
                    print(f"  {operation}: {json.dumps(metrics)}", flush=True)
            else:
                print(json.dumps(record), flush=True)
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
            args.kbid = await resolve_kbids(ctx.kv_driver, args)
            await start_predict_engine()
            rate = args.start_rps
            while True:
                await run_stage(args, rate, rng, request_pool, emit)
                if rate >= args.max_rps:
                    break
                rate = min(args.max_rps, rate + args.rps_step)
        finally:
            emit({"event": "summary", "stages": stages})
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
