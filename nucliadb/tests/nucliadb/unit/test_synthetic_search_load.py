import asyncio
import random
import sys
from argparse import Namespace
from collections import Counter
from contextlib import asynccontextmanager
from importlib import import_module
from pathlib import Path

from faker import Faker

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))
load_test = import_module("synthetic_search_load")


def test_resolve_kbids_from_slug_prefix(monkeypatch):
    class FakeDriver:
        @asynccontextmanager
        async def ro_transaction(self, *, kbid):
            assert kbid is None
            yield object()

    async def iter_kbs(txn, *, slug_prefix):
        assert slug_prefix == "load-test-"
        yield "kb-2", "load-test-002"
        yield "kb-1", "load-test-001"

    monkeypatch.setattr(load_test.datamanagers.kb, "iter", iter_kbs)

    result = asyncio.run(
        load_test.resolve_kbids(FakeDriver(), Namespace(kbid=None, kb_prefix="load-test"))
    )

    assert result == ["kb-1", "kb-2"]


def test_resolve_kbids_without_filter_returns_all_kbs(monkeypatch):
    class FakeDriver:
        @asynccontextmanager
        async def ro_transaction(self, *, kbid):
            assert kbid is None
            yield object()

    async def iter_kbs(txn, *, slug_prefix):
        assert slug_prefix == ""
        yield "kb-2", "other-002"
        yield "kb-1", "other-001"

    monkeypatch.setattr(load_test.datamanagers.kb, "iter", iter_kbs)

    result = asyncio.run(load_test.resolve_kbids(FakeDriver(), Namespace(kbid=None, kb_prefix=None)))

    assert result == ["kb-1", "kb-2"]


def test_resolve_kbids_preserves_explicit_ids():
    result = asyncio.run(load_test.resolve_kbids(None, Namespace(kbid=["kb-a", "kb-b"], kb_prefix=None)))

    assert result == ["kb-a", "kb-b"]


def test_select_active_kbids_is_seeded_and_preserves_input_order():
    kbids = [f"kb-{index}" for index in range(10)]

    selected = load_test.select_active_kbids(kbids, active_kbs=3, seed=42)

    assert len(selected) == 3
    assert selected == load_test.select_active_kbids(kbids, active_kbs=3, seed=42)
    assert selected == [kbid for kbid in kbids if kbid in set(selected)]
    assert load_test.select_active_kbids(kbids, active_kbs=None, seed=42) == kbids


def test_generate_request_returns_valid_model_for_each_endpoint():
    fake = Faker()
    fake.seed_instance(42)
    rng = random.Random(42)

    requests = {
        operation: load_test.generate_request(operation, fake, rng)
        for operation in load_test.ENDPOINT_PROBABILITIES
    }

    assert all(
        isinstance(request, load_test.REQUEST_MODELS[operation])
        for operation, request in requests.items()
    )
    assert requests["find_keyword"].features == [load_test.FindOptions.KEYWORD]
    assert requests["find_semantic"].features == [load_test.FindOptions.SEMANTIC]
    assert requests["find_hybrid"].features == [
        load_test.FindOptions.KEYWORD,
        load_test.FindOptions.SEMANTIC,
    ]
    assert requests["find_keyword"].vector is None
    assert requests["find_semantic"].vector
    assert requests["find_hybrid"].vector
    assert sum(load_test.ENDPOINT_PROBABILITIES.values()) == 1
    assert "augment" not in requests


def test_create_request_pool_is_fixed_and_seeded():
    def create_pool():
        fake = Faker()
        fake.seed_instance(42)
        return load_test.create_request_pool(
            fake,
            random.Random(42),
            pool_size=4,
            vector_dimension=8,
            vectorset="test-vectors",
        )

    first = create_pool()
    second = create_pool()

    assert set(first) == set(load_test.ENDPOINT_PROBABILITIES)
    assert all(len(requests) == 4 for requests in first.values())
    assert {
        operation: [request.model_dump(mode="json") for request in requests]
        for operation, requests in first.items()
    } == {
        operation: [request.model_dump(mode="json") for request in requests]
        for operation, requests in second.items()
    }


def test_format_report_lines_shows_rates_and_human_readable_latencies():
    metrics = {
        "success_rps": 2.5,
        "completed_rps": 3.0,
        "success": 5,
        "failed": 1,
        "dropped": 2,
        "cancelled": 0,
        "latency_p50_seconds": 0.012,
        "latency_p90_seconds": 0.034,
        "latency_p95_seconds": 0.045,
        "latency_p99_seconds": 0.056,
        "metric_latencies": {
            "index_search": {"p50_seconds": 0.006, "p95_seconds": 0.018, "p99_seconds": 0.02},
            "results_merge": {"p50_seconds": 0.003, "p95_seconds": 0.009, "p99_seconds": 0.01},
            "custom_step": {"p50_seconds": 0.001, "p95_seconds": 0.002, "p99_seconds": 0.004},
        },
    }
    report = {
        "event": "interval",
        "target_rps": 4,
        "phase": "load",
        "window_seconds": 2.0,
        "in_flight": 3,
        **metrics,
        "operations": {
            "find_semantic": {**metrics, "success_rps": 2.0},
            "catalog": {**metrics, "success_rps": 0.5},
        },
    }

    lines = load_test.format_report_lines(report, "2026-11-02T12:00:00Z")

    assert "actual=2.50 success/s" in lines[0]
    assert "p50/p90/p95/p99=12.0 ms/34.0 ms/45.0 ms/56.0 ms" in lines[0]
    assert "index_search p50/p95/p99=6.0 ms/18.0 ms/20.0 ms" in lines[0]
    assert "results_merge p50/p95/p99=3.0 ms/9.0 ms/10.0 ms" in lines[0]
    assert "custom_step p50/p95/p99=1.0 ms/2.0 ms/4.0 ms" in lines[0]
    assert "find_semantic" in lines[1]
    assert "2.00 success/s" in lines[1]
    assert "p50=12.0 ms" in lines[1]
    assert "index_search p50/p95/p99=6.0 ms/18.0 ms/20.0 ms" in lines[1]
    assert "results_merge p50/p95/p99=3.0 ms/9.0 ms/10.0 ms" in lines[1]
    assert "custom_step p50/p95/p99=1.0 ms/2.0 ms/4.0 ms" in lines[1]
    assert "catalog" in lines[2]


def test_format_configuration_line_handles_unresolved_prefix_selection():
    line = load_test.format_configuration_line(
        {
            "kbid": None,
            "kb_prefix": "hourly-active",
            "active_kbs": 11,
            "start_rps": 4,
            "max_rps": 4,
            "stage_seconds": 120,
        },
        load_test.ENDPOINT_PROBABILITIES,
    )

    assert "prefix=hourly-active (resolving)" in line
    assert "active-kbs=11" in line
    assert "rate=4-4 req/s" in line


def test_format_final_summary_shows_rates_and_stage_percentiles():
    output = load_test.format_final_summary(
        [
            {
                "target_rps": 20,
                "elapsed_seconds": 10,
                "success": 50,
                "failed": 2,
                "dropped": 100,
                "latency_p50_seconds": 0.25,
                "latency_p95_seconds": 1.5,
                "metric_latencies": {
                    "index_search": {"p50_seconds": 0.04, "p95_seconds": 0.12},
                    "results_merge": {"p50_seconds": 0.02, "p95_seconds": 0.06},
                    "custom_step": {"p50_seconds": 0.01, "p95_seconds": 0.03},
                },
            }
        ],
        [{"kbid": "kb-one", "chunks": 12345}],
    )

    assert "Success/s" in output
    assert "Failure/s" in output
    assert "Dropped/s" in output
    assert "p50" in output and "p95" in output
    assert "20 req/s" in output
    assert "5.00" in output
    assert "0.20" in output
    assert "10.00" in output
    assert "250.0 ms" in output
    assert "1500.0 ms" in output
    assert "index_search p50" in output and "index_search p95" in output
    assert "40.0 ms" in output and "120.0 ms" in output
    assert "results_merge p50" in output and "results_merge p95" in output
    assert "20.0 ms" in output and "60.0 ms" in output
    assert "custom_step p50" in output and "custom_step p95" in output
    assert "10.0 ms" in output and "30.0 ms" in output
    assert "Active KB sizes by chunk count" in output
    assert "kb-one" in output
    assert "12,345" in output


def test_count_kb_chunks_uses_chunks_collection():
    class FakeDriver:
        @asynccontextmanager
        async def ro_transaction(self, *, kbid):
            assert kbid == "kb-one"
            yield object()

    async def evaluate(txn, javascript):
        assert 'cts.collectionQuery("chunks")' in javascript
        return [12345]

    original_evaluate = load_test.documents.evaluate
    load_test.documents.evaluate = evaluate
    try:
        result = asyncio.run(load_test.count_kb_chunks(FakeDriver(), "kb-one"))
    finally:
        load_test.documents.evaluate = original_evaluate

    assert result == 12345


def test_run_stage_distributes_requests_round_robin_across_kbs():
    seen_kbids = []
    reports = []

    async def execute(kbid, operation, item):
        seen_kbids.append(kbid)
        return {"index_search": 0.025, "results_merge": 0.01, "query_parse": 0.005}

    async def run_stage():
        args = Namespace(
            kbid=["kb-one", "kb-two"],
            stage_seconds=0.04,
            request_timeout=1,
            report_interval=60,
            max_in_flight=10,
            vector_dimension=768,
            vectorset=None,
        )
        fake = Faker()
        fake.seed_instance(42)
        request_pool = load_test.create_request_pool(
            fake, random.Random(42), pool_size=4, vector_dimension=768, vectorset=None
        )
        return await load_test.run_stage(
            args,
            target_rps=100,
            rng=random.Random(42),
            request_pool=request_pool,
            emit=reports.append,
            execute=execute,
        )

    result = asyncio.run(run_stage())

    assert result["offered"] == 4
    assert result["success"] == 4
    assert result["metric_latencies"]["index_search"]["p50_seconds"] == 0.025
    assert result["metric_latencies"]["results_merge"]["p50_seconds"] == 0.01
    assert result["metric_latencies"]["query_parse"]["p50_seconds"] == 0.005
    assert (
        result["operations"]["find_semantic"]["metric_latencies"]["results_merge"]["p95_seconds"] == 0.01
    )
    interval = next(report for report in reports if report["event"] == "interval")
    assert interval["metric_latencies"]["results_merge"]["p50_seconds"] == 0.01
    assert all(
        sample["metrics"] == {"index_search": 0.025, "results_merge": 0.01, "query_parse": 0.005}
        for sample in interval["latency_samples"]
    )
    assert seen_kbids == ["kb-one", "kb-two", "kb-one", "kb-two"]


def test_execute_operation_returns_all_metric_keys(monkeypatch):
    async def find(kbid, item, **kwargs):
        assert kbid == "kb-one"
        kwargs["metrics"].set("index_search", 0.025)
        kwargs["metrics"].set("results_merge", 0.01)
        kwargs["metrics"].set("query_parse", 0.005)
        kwargs["metrics"].set("custom_step", 0.0)
        return None, False, None

    monkeypatch.setattr(load_test, "find", find)

    result = asyncio.run(load_test.execute_operation("kb-one", "find_semantic", object()))

    assert result == {
        "index_search": 0.025,
        "results_merge": 0.01,
        "query_parse": 0.005,
        "custom_step": 0.0,
    }


def test_measurement_summary_tracks_all_percentiles_and_missing_timings():
    samples = [
        {"outcome": "success", "seconds": 1, "metrics": {"custom_step": duration}}
        for duration in (0.0, 0.01, 0.02, 0.03, None)
    ]
    samples.append({"outcome": "success", "seconds": 1})

    summary = load_test.measurement_summary(Counter(success=6), samples, 1)

    assert summary["metric_latencies"]["custom_step"] == {
        "mean_seconds": 0.015,
        "p50_seconds": 0.01,
        "p90_seconds": 0.03,
        "p95_seconds": 0.03,
        "p99_seconds": 0.03,
    }
    empty = load_test.measurement_summary(Counter(), [], 1)
    assert empty["metric_latencies"] == {}


def test_metric_reporting_handles_missing_values_and_different_stage_keys():
    empty_metrics = load_test.measurement_summary(
        Counter(success=1),
        [{"outcome": "success", "seconds": 1, "metrics": {"absent_step": None}}],
        1,
    )
    assert load_test.format_metric_latencies(empty_metrics) == "absent_step p50/p95/p99=-/-/-"
    stages = [
        {
            "target_rps": rate,
            "elapsed_seconds": 1,
            **load_test.measurement_summary(
                Counter(success=1),
                [{"outcome": "success", "seconds": 1, "metrics": metrics}],
                1,
            ),
        }
        for rate, metrics in ((1, {"first_step": 0.01}), (2, {"later_step": 0.02}))
    ]
    output = load_test.format_final_summary(stages)
    assert "first_step p50" in output
    assert "later_step p50" in output
    first_row, second_row = output.splitlines()[-2:]
    assert first_row.split()[-2:] == ["-", "-"]
    assert second_row.split().count("-") == 2
    assert second_row.count("20.0 ms") == 2
    assert load_test.format_metric_latencies({}) == ""
    assert "No completed stages" in load_test.format_final_summary([])


def test_run_stage_keeps_target_rate_on_selected_active_kbs():
    seen_kbids = []

    async def execute(kbid, operation, item):
        seen_kbids.append(kbid)

    async def run_stage():
        args = Namespace(
            kbid=load_test.select_active_kbids(
                [f"kb-{index}" for index in range(10)], active_kbs=3, seed=42
            ),
            stage_seconds=0.04,
            request_timeout=1,
            report_interval=60,
            max_in_flight=10,
            vector_dimension=768,
            vectorset=None,
        )
        fake = Faker()
        fake.seed_instance(42)
        request_pool = load_test.create_request_pool(
            fake, random.Random(42), pool_size=4, vector_dimension=768, vectorset=None
        )
        return await load_test.run_stage(
            args,
            target_rps=100,
            rng=random.Random(42),
            request_pool=request_pool,
            emit=lambda report: None,
            execute=execute,
        )

    result = asyncio.run(run_stage())
    selected_kbids = load_test.select_active_kbids(
        [f"kb-{index}" for index in range(10)], active_kbs=3, seed=42
    )

    assert result["offered"] == 4
    assert result["success"] == 4
    assert seen_kbids == [selected_kbids[index % 3] for index in range(4)]


def test_run_stage_prints_failed_request_details(capsys):
    async def execute(kbid, operation, item):
        raise RuntimeError("backend unavailable")

    async def run_stage():
        args = Namespace(
            kbid=["kb-one"],
            stage_seconds=0.01,
            request_timeout=1,
            report_interval=60,
            max_in_flight=10,
            vector_dimension=768,
            vectorset=None,
        )
        fake = Faker()
        fake.seed_instance(42)
        request_pool = load_test.create_request_pool(
            fake, random.Random(42), pool_size=1, vector_dimension=768, vectorset=None
        )
        return await load_test.run_stage(
            args,
            target_rps=100,
            rng=random.Random(42),
            request_pool=request_pool,
            emit=lambda report: None,
            execute=execute,
        )

    result = asyncio.run(run_stage())
    captured = capsys.readouterr()
    output = captured.out + captured.err

    assert result["failed"] == 1
    assert "Request failed: kbid=kb-one operation=" in output
    assert "RuntimeError: backend unavailable" in output
    assert "Traceback (most recent call last):" in output
    assert "in execute" in output
