import asyncio
import math
import sys
from argparse import Namespace
from importlib import import_module
from itertools import zip_longest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

load_test = import_module("synthetic_marklogic_ingest")
make_ramp_plan = load_test.make_ramp_plan
ramp_task_stream = load_test.ramp_task_stream


@pytest.mark.parametrize("end_rate", [2, 10])
def test_continuous_rate_ramp_paces_starts_and_stops_at_deadline(monkeypatch, end_rate):
    clock = SimpleNamespace(now=100.0)

    async def advance(seconds):
        clock.now += seconds

    monkeypatch.setattr(load_test, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(load_test, "asyncio", SimpleNamespace(Lock=asyncio.Lock, sleep=advance))

    async def exercise():
        ramp = load_test.ContinuousRateRamp(2, end_rate, 4)
        starts = []
        while await ramp.acquire():
            starts.append(clock.now - ramp.started)
        for resource_number, elapsed in enumerate(starts):
            integrated_rate = 2 * elapsed + (end_rate - 2) * elapsed**2 / 8
            assert integrated_rate == pytest.approx(resource_number)
        assert all(elapsed < 4 for elapsed in starts)
        assert len(starts) == math.ceil((2 + end_rate) * 4 / 2)
        assert not await ramp.acquire()

    asyncio.run(exercise())


def test_continuous_rate_ramp_does_not_catch_up_after_slow_writes(monkeypatch):
    clock = SimpleNamespace(now=0.0)
    sleeps = []

    async def advance(seconds):
        sleeps.append(seconds)
        clock.now += seconds

    monkeypatch.setattr(load_test, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(load_test, "asyncio", SimpleNamespace(Lock=asyncio.Lock, sleep=advance))

    async def exercise():
        ramp = load_test.ContinuousRateRamp(2, 2, 10)
        assert await ramp.acquire()
        clock.now = 5
        assert await ramp.acquire()
        assert sleeps == []
        assert await ramp.acquire()
        assert sleeps == [0.5]

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "options",
    [
        ["--rate-start", "1"],
        ["--rate-start", "1", "--rate-end", "2", "--duration-seconds", "1"],
        ["--continuous", "--kb-slug", "kb", "--rate-start", "1"],
        [
            "--continuous",
            "--kb-slug",
            "kb",
            "--rate-start",
            "0",
            "--rate-end",
            "2",
            "--duration-seconds",
            "1",
        ],
        [
            "--continuous",
            "--kb-slug",
            "kb",
            "--rate-start",
            "2",
            "--rate-end",
            "1",
            "--duration-seconds",
            "1",
        ],
        [
            "--continuous",
            "--kb-slug",
            "kb",
            "--rate-start",
            "1",
            "--rate-end",
            "inf",
            "--duration-seconds",
            "1",
        ],
        [
            "--continuous",
            "--kb-slug",
            "kb",
            "--rate-start",
            "1",
            "--rate-end",
            "2",
            "--duration-seconds",
            "nan",
        ],
        ["--max-chunks", "100"],
        ["--continuous", "--random-kbs", "2", "--kb-prefix", "load", "--max-chunks", "100"],
        ["--continuous", "--kb-slug", "kb", "--max-chunks", "0"],
    ],
)
def test_continuous_rate_ramp_rejects_invalid_cli_options(monkeypatch, options):
    monkeypatch.setattr(sys, "argv", ["synthetic_marklogic_ingest.py", *options])
    with pytest.raises(SystemExit) as exc_info:
        load_test.main()
    assert exc_info.value.code == 2


@pytest.mark.parametrize("existing_kbid", [None, "existing-kb"])
def test_resolve_ingest_kb_by_slug_reuses_or_creates(monkeypatch, existing_kbid):
    class FakeDriver:
        def ro_transaction(self, kbid):
            assert kbid is None
            return AsyncMock()

    lookup = AsyncMock(return_value=existing_kbid)
    ensure = AsyncMock(return_value="new-kb")
    monkeypatch.setattr(load_test.datamanagers.kb, "get_kbid", lookup)
    monkeypatch.setattr(load_test, "ensure_synthetic_kb", ensure)
    driver = FakeDriver()
    args = Namespace(kb_slug="target", vectorset_id="vectors", vector_dimension=4)

    kbid = asyncio.run(load_test.resolve_ingest_kb(driver, {"slug": "target"}, args))

    assert kbid == (existing_kbid or "new-kb")
    assert lookup.await_args.kwargs == {"slug": "target"}
    if existing_kbid:
        ensure.assert_not_awaited()
    else:
        ensure.assert_awaited_once_with(
            driver, slug="target", vectorset_id="vectors", vector_dimension=4
        )


@pytest.mark.parametrize("auto_resume", [False, True])
@pytest.mark.parametrize("timed", [False, True])
@pytest.mark.parametrize("multiple_kbs", [False, True])
def test_continuous_ingest_ignores_scale_and_cleans_up(
    monkeypatch, auto_resume, timed, multiple_kbs, capsys
):
    class FakeDriver:
        def ro_transaction(self, kbid):
            assert kbid == "kb-id"
            return AsyncMock()

    class FakeGenerator:
        def __init__(self, seed, paragraph_sentences):
            pass

        def make(self, kbid, resource_index):
            return {
                "index": resource_index,
                "paragraphs": 0,
                "text_bytes": 0,
                "groups": [],
                "labels": [],
            }

    written = []
    written_kbids = []
    selected_plan = [
        {"slug": f"load-{index}", "kbid": f"kb-{index}", "resources": math.inf} for index in range(3)
    ]
    identities = []
    teardown = AsyncMock()
    checkpoint = AsyncMock(side_effect=AssertionError("continuous mode must not checkpoint"))

    async def slug_exists(txn, *, kbid, slug):
        raise AssertionError("continuous mode must not scan existing slugs")

    async def exercise():
        enough_writes = asyncio.Event()

        async def write_resource(driver, kbid, generated, vectorsets, seed, *, check_existing):
            assert kbid in ([item["kbid"] for item in selected_plan] if multiple_kbs else ["kb-id"])
            assert vectorsets == [(f"vectors-{kbid}", 4)]
            assert check_existing is False
            assert generated["slug"] == f"synth-{generated['rid']}"
            identities.append(generated["rid"])
            written.append(generated["index"])
            written_kbids.append(kbid)
            if len(written) == 6:
                enough_writes.set()
            await asyncio.sleep(0.02 if timed else 0)
            return True, 0, 0, 0

        monkeypatch.setattr(load_test, "write_resource", write_resource)
        args = Namespace(
            continuous=True,
            kb_slug=None if multiple_kbs else "target",
            scale=0,
            seed=42,
            sigma=0.4,
            random_kbs=3 if multiple_kbs else None,
            kb_prefix="load",
            ramp=False,
            active_kbs=1,
            start_resource_number=1,
            dry_run=False,
            concurrency=2,
            delete=False,
            yes=False,
            vectorset_id="vectors",
            vector_dimension=4,
            write_timeout=1,
            report_interval=60,
            log_every=100,
            paragraph_sentences=4,
            auto_resume=auto_resume,
            rate_start=100 if timed else None,
            rate_end=200 if timed else None,
            duration_seconds=0.12 if timed else None,
        )
        runner = asyncio.create_task(load_test.run(args))
        try:
            await asyncio.wait_for(enough_writes.wait(), timeout=2)
            assert not runner.done()
            if timed:
                await asyncio.wait_for(asyncio.shield(runner), timeout=2)
        finally:
            if not runner.done():
                runner.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await runner
        assert not [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]

    monkeypatch.setattr(load_test, "MarkLogicDriver", FakeDriver)
    monkeypatch.setattr(load_test, "setup_driver", AsyncMock(return_value=FakeDriver()))
    monkeypatch.setattr(load_test, "teardown_driver", teardown)

    async def resolve(driver, item, args):
        return item.get("kbid", "kb-id")

    async def vectorsets(driver, kbid):
        return [(f"vectors-{kbid}", 4)]

    selection = AsyncMock(return_value=selected_plan)
    monkeypatch.setattr(load_test, "select_random_kbs", selection)
    monkeypatch.setattr(load_test, "resolve_ingest_kb", resolve)
    monkeypatch.setattr(load_test, "get_kb_vectorsets", vectorsets)
    monkeypatch.setattr(load_test, "SyntheticResourceFactory", FakeGenerator)
    monkeypatch.setattr(load_test.datamanagers.resources, "slug_exists", slug_exists)
    monkeypatch.setattr(load_test, "load_completed_kb_prefix", checkpoint)
    monkeypatch.setattr(load_test, "save_completed_kb_prefix", checkpoint)

    asyncio.run(exercise())

    assert sorted(written) == list(range(len(written)))
    assert len(set(identities)) == len(written)
    assert len(written) >= 6
    if multiple_kbs:
        assert written_kbids == [f"kb-{index % 3}" for index in range(len(written))]
        selection.assert_awaited_once()
    else:
        selection.assert_not_awaited()
    teardown.assert_awaited_once()
    checkpoint.assert_not_awaited()
    if timed:
        output = capsys.readouterr().out
        assert '"duration_seconds": 0.12' in output
        assert '"planned_resources": null' in output
        assert "Infinity" not in output
        assert "target=200.00 res/s" in output


def test_count_kb_chunks_uses_selected_database():
    class FakeDriver:
        class FakeClient:
            async def eval(self, javascript, *, params):
                assert 'cts.collectionQuery("chunks")' in javascript
                assert params == {"database": "database-kb-id"}
                return [123]

        client = FakeClient()

        def kb_database(self, kbid):
            assert kbid == "kb-id"
            return "database-kb-id"

    assert asyncio.run(load_test.count_kb_chunks(FakeDriver(), "kb-id")) == 123


@pytest.mark.parametrize("existing_chunks, expected_writes", [(2, 2), (6, 0)])
def test_continuous_chunk_limit_includes_existing_and_drains(
    monkeypatch, existing_chunks, expected_writes
):
    class FakeDriver:
        def __init__(self):
            self.client = SimpleNamespace(eval=AsyncMock(return_value=[existing_chunks]))

        def ro_transaction(self, kbid):
            assert kbid is None
            return AsyncMock()

        def kb_database(self, kbid):
            return f"database-{kbid}"

    class FakeGenerator:
        def __init__(self, seed, paragraph_sentences):
            pass

        def make(self, kbid, resource_index):
            return {
                "rid": f"resource-{resource_index}",
                "slug": f"resource-{resource_index}",
                "title": "title",
                "summary": "summary",
                "fields": {},
                "paragraphs": 0,
                "text_bytes": 0,
                "groups": [],
                "labels": [],
            }

    async def exercise():
        driver = FakeDriver()
        written = []

        async def write_resource(driver, kbid, generated, vectorsets, seed, *, check_existing):
            written.append(generated["rid"])
            await asyncio.sleep(0.01)
            return True, 0, 2, 2

        monkeypatch.setattr(load_test, "MarkLogicDriver", FakeDriver)
        monkeypatch.setattr(load_test, "setup_driver", AsyncMock(return_value=driver))
        monkeypatch.setattr(load_test, "teardown_driver", AsyncMock())
        monkeypatch.setattr(load_test, "SyntheticResourceFactory", FakeGenerator)
        monkeypatch.setattr(load_test, "write_resource", write_resource)
        monkeypatch.setattr(
            load_test,
            "resolve_ingest_kb",
            AsyncMock(return_value="kb-id"),
        )
        monkeypatch.setattr(
            load_test,
            "get_kb_vectorsets",
            AsyncMock(return_value=[("vectors", 4)]),
        )
        args = Namespace(
            continuous=True,
            kb_slug="target",
            kbid=None,
            random_kbs=None,
            scale=0,
            seed=42,
            sigma=0.4,
            kb_prefix="load",
            ramp=False,
            active_kbs=1,
            start_resource_number=1,
            dry_run=False,
            concurrency=4,
            delete=False,
            yes=False,
            vectorset_id="vectors",
            vector_dimension=4,
            write_timeout=1,
            report_interval=60,
            log_every=100,
            paragraph_sentences=4,
            auto_resume=False,
            rate_start=None,
            rate_end=None,
            duration_seconds=None,
            max_chunks=6,
        )
        await asyncio.wait_for(load_test.run(args), timeout=3)
        assert len(written) == expected_writes
        assert driver.client.eval.await_count == 1

    asyncio.run(exercise())


@pytest.mark.parametrize("amount", [2, 4])
def test_select_random_kbs_matches_prefix_without_replacement(monkeypatch, amount):
    class FakeDriver:
        def ro_transaction(self, kbid):
            assert kbid is None
            return AsyncMock()

    async def iter_kbs(txn, *, slug_prefix):
        assert slug_prefix == "load-"
        for index in range(3):
            yield f"kb-{index}", f"load-{index}"

    monkeypatch.setattr(load_test.datamanagers.kb, "iter", iter_kbs)
    if amount > 3:
        with pytest.raises(ValueError, match="only 3 match"):
            asyncio.run(load_test.select_random_kbs(FakeDriver(), "load-", amount))
    else:
        plan = asyncio.run(load_test.select_random_kbs(FakeDriver(), "load-", amount))
        assert len(plan) == amount
        assert len({item["kbid"] for item in plan}) == amount
        assert all(item["slug"].startswith("load-") for item in plan)
        assert all(item["resources"] == math.inf for item in plan)


def test_random_kbs_dry_run_only_selects_existing_kbs(monkeypatch):
    class FakeDriver:
        pass

    setup = AsyncMock(return_value=FakeDriver())
    teardown = AsyncMock()
    selection = AsyncMock(return_value=[{"kbid": "kb", "slug": "load-1"}])
    resolve = AsyncMock(side_effect=AssertionError("dry run must not prepare KBs"))
    monkeypatch.setattr(load_test, "MarkLogicDriver", FakeDriver)
    monkeypatch.setattr(load_test, "setup_driver", setup)
    monkeypatch.setattr(load_test, "teardown_driver", teardown)
    monkeypatch.setattr(load_test, "select_random_kbs", selection)
    monkeypatch.setattr(load_test, "resolve_ingest_kb", resolve)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "synthetic_marklogic_ingest.py",
            "--continuous",
            "--random-kbs",
            "1",
            "--kb-prefix",
            "load-",
            "--dry-run",
        ],
    )

    load_test.main()

    selection.assert_awaited_once_with(setup.return_value, "load-", 1)
    teardown.assert_awaited_once()
    resolve.assert_not_awaited()


def test_make_ramp_plan_balances_resources_and_names_kbs():
    plan = make_ramp_plan(11, 4, "load")

    assert [item["resources"] for item in plan] == [3, 3, 3, 2]
    assert [item["slug"] for item in plan] == [
        "load-000",
        "load-001",
        "load-002",
        "load-003",
    ]
    assert sum(item["resources"] for item in plan) == 11


def test_ramp_task_stream_interleaves_kbs_and_honors_resource_ranges():
    streams = [
        ramp_task_stream("load-000", "kb-000", [], 2, 4),
        ramp_task_stream("load-001", "kb-001", [], 0, 3),
        ramp_task_stream("load-002", "kb-002", [], 1, 2),
    ]

    tasks = [task for row in zip_longest(*streams) for task in row if task is not None]

    assert [(task[0], task[3]) for task in tasks] == [
        ("load-000", 2),
        ("load-001", 0),
        ("load-002", 1),
        ("load-000", 3),
        ("load-001", 1),
        ("load-001", 2),
    ]


def test_find_resume_offset_uses_binary_slug_search(monkeypatch):
    class FakeDriver:
        def ro_transaction(self, kbid):
            class TransactionContext:
                async def __aenter__(self):
                    return object()

                async def __aexit__(self, *exc_info):
                    return False

            return TransactionContext()

    lookups = []

    async def slug_exists(txn, *, kbid, slug):
        lookups.append(slug)
        resource_index = int(slug.rsplit("-", 1)[1])
        return resource_index < 13

    monkeypatch.setattr(load_test.datamanagers.resources, "slug_exists", slug_exists)

    offset = asyncio.run(load_test.find_resume_offset(FakeDriver(), "kb", 42, 100))

    assert offset == 13
    assert len(lookups) <= 7


def test_ramp_skips_setup_for_kbs_before_start_offset(monkeypatch):
    ensured_slugs = []

    class FakeDriver:
        def kb_database(self, kbid):
            return f"database-{kbid}"

    async def ensure_kb(driver, *, slug, vectorset_id, vector_dimension):
        ensured_slugs.append(slug)
        return slug

    async def get_vectorsets(driver, kbid):
        return []

    async def run_stage(driver, tasks, generator, args, concurrency, emit):
        return {"event": "stage", "failed": 0, "exhausted": True}

    monkeypatch.setattr(load_test, "ensure_synthetic_kb", ensure_kb)
    monkeypatch.setattr(load_test, "get_kb_vectorsets", get_vectorsets)
    monkeypatch.setattr(load_test, "run_ramp_stage", run_stage)

    args = Namespace(
        report_file=None,
        seed=42,
        paragraph_sentences=4,
        concurrency=1,
        max_concurrency=1,
        concurrency_step=1,
        active_kbs=3,
        stage_seconds=1,
        vectorset_id="vectors",
        vector_dimension=4,
        auto_resume=False,
    )
    plan = [{"slug": f"load-{index:03d}", "resources": 2} for index in range(3)]

    asyncio.run(load_test.run_ramp(FakeDriver(), args, plan, start_offset=2))

    assert ensured_slugs == ["load-001", "load-002"]


def test_ramp_stage_reports_per_kb_metrics(monkeypatch):
    async def fake_write_resource(driver, kbid, generated, vectorsets, seed):
        return True, 1, 2, 0

    async def run_stage():
        args = Namespace(
            stage_seconds=1,
            write_timeout=2,
            report_interval=60,
            seed=42,
            active_kbs=2,
        )
        tasks = iter(
            [("load-000", "kb-000", [], index) for index in range(3)]
            + [("load-001", "kb-001", [], index) for index in range(3)]
        )
        reports = []
        result = await load_test.run_ramp_stage(
            None,
            tasks,
            Namespace(make=lambda kbid, index: {"kbid": kbid, "index": index}),
            args,
            2,
            reports.append,
        )
        return result, reports

    monkeypatch.setattr(load_test, "write_resource", fake_write_resource)
    result, reports = asyncio.run(run_stage())

    assert result["created"] == 6
    assert set(result["knowledge_boxes"]) == {"load-000", "load-001"}
    for metrics in result["knowledge_boxes"].values():
        assert metrics["created"] == 3
        assert metrics["documents"] == 21
        assert metrics["write_latency_p50_seconds"] is not None
    interval = next(report for report in reports if report["event"] == "interval")
    assert interval["created"] == 6
    assert all(metrics["created"] == 3 for metrics in interval["knowledge_boxes"].values())


def test_normal_ingest_keeps_workers_alive_after_generation_failure(monkeypatch, capsys):
    class FakeDriver:
        pass

    class FakeGenerator:
        def __init__(self, seed, paragraph_sentences):
            pass

        def make(self, kbid, resource_index):
            if resource_index == 0:
                raise ValueError("synthetic generation failure")
            return {
                "fields": {},
                "paragraphs": 0,
                "text_bytes": 0,
                "groups": [],
                "labels": [],
            }

    written = []

    async def ensure_kb(driver, *, slug, vectorset_id, vector_dimension):
        return "kb-id"

    async def get_vectorsets(driver, kbid):
        return []

    async def write_resource(driver, kbid, generated, vectorsets, seed):
        await asyncio.sleep(0.01)
        written.append(kbid)
        return True, 0, 0, 0

    async def setup_fake_driver():
        return FakeDriver()

    async def noop(*args, **kwargs):
        pass

    async def load_checkpoint(driver, fingerprint):
        return 0

    async def save_checkpoint(driver, fingerprint, completed_kbs):
        raise AssertionError("a KB with a failed resource must not be checkpointed")

    monkeypatch.setattr(load_test, "MarkLogicDriver", FakeDriver)
    monkeypatch.setattr(load_test, "setup_driver", setup_fake_driver)
    monkeypatch.setattr(load_test, "teardown_driver", noop)
    monkeypatch.setattr(
        load_test, "plan_distribution", lambda *args: [{"slug": "load-000", "resources": 6}]
    )
    monkeypatch.setattr(load_test, "ensure_synthetic_kb", ensure_kb)
    monkeypatch.setattr(load_test, "get_kb_vectorsets", get_vectorsets)
    monkeypatch.setattr(load_test, "SyntheticResourceFactory", FakeGenerator)
    monkeypatch.setattr(load_test, "write_resource", write_resource)
    monkeypatch.setattr(load_test, "load_completed_kb_prefix", load_checkpoint)
    monkeypatch.setattr(load_test, "save_completed_kb_prefix", save_checkpoint)

    args = Namespace(
        scale=0.1,
        seed=42,
        sigma=0.4,
        kb_prefix="load",
        ramp=False,
        active_kbs=1,
        start_resource_number=1,
        dry_run=False,
        concurrency=1,
        delete=False,
        yes=False,
        vectorset_id="vectors",
        vector_dimension=4,
        write_timeout=1,
        report_interval=0.001,
        log_every=100,
        paragraph_sentences=4,
        auto_resume=False,
    )

    with pytest.raises(SystemExit) as exc_info:
        asyncio.run(asyncio.wait_for(load_test.run(args), timeout=2))

    assert exc_info.value.code == 1
    assert written == ["kb-id"] * 5
    output = capsys.readouterr().out
    assert "resource_index=0" in output
    assert '"created": 5' in output
    assert '"failed": 1' in output
    assert output.count("Ingest elapsed=") > 1
    assert "in-flight=1" in output
    assert "res/s chunks=" in output
    assert "documents=" in output
    assert '"documents": 15' in output
    progress_lines = [line for line in output.splitlines() if line.startswith("Ingest elapsed=")]
    assert any("write_p50=- write_p95=-" in line for line in progress_lines)
    committed_lines = [line for line in progress_lines if "write_p50=-" not in line]
    assert committed_lines
    for line in committed_lines:
        metrics = dict(part.split("=", 1) for part in line.split() if "=" in part)
        assert float(metrics["write_p50"].removesuffix("s")) > 0
        assert float(metrics["write_p95"].removesuffix("s")) >= float(
            metrics["write_p50"].removesuffix("s")
        )
