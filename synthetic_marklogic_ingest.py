"""Populate MarkLogic with synthetic searchable data and create its KnowledgeBoxes.

Unlike synthetic_ingest.py, this writes directly through the MarkLogic maindb driver.
It does not call the writer API, processing engine, or object storage. Text field values,
processed-field documents, chunks, and synthetic vectors are written to MarkLogic.

KnowledgeBoxes use slugs ``<kb-prefix>-NNN``. The default prefix is ``ml-sizing-kb``
so these databases can be identified for cleanup. Existing KBs are reused and existing
resources are skipped. No blob-storage buckets are created.

Example from an in-cluster NucliaDB container:
    python synthetic_marklogic_ingest.py --scale 0.15 --kb-prefix ml-sizing-kb

Ramp concurrent resource transactions across 15 KB databases:
    python synthetic_marklogic_ingest.py --ramp --kb-prefix ml-load-test \
    --active-kbs 15 \
        --concurrency 4 --max-concurrency 64 --concurrency-step 4 \
        --stage-seconds 180 --report-file ml-load.jsonl

Use a fresh KB prefix or seed to avoid measuring skipped resources. Ramp mode
prepares all configured KBs before timing, distributes resources evenly, and
interleaves writes across the databases. It drains each stage and measures the
direct driver workload, not end-to-end processing or raw server capacity. Watch
client CPU as well as cluster CPU, disk, merges, and indexing; synthetic
text/vector generation and the client connection pool can limit load.

MarkLogic connection uses the normal MARKLOGIC_SERVER_* and DRIVER settings.
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
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from itertools import zip_longest
from pathlib import Path
from typing import Any

from faker import Faker

from nucliadb.common import datamanagers
from nucliadb.common.datamanagers.fields import (
    ChunkContent,
    ChunkEmbeddingContent,
    FieldContent,
    FieldProcessedContent,
)
from nucliadb.common.datamanagers.resources import ResourceCreateContent
from nucliadb.common.ids import ParagraphId
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.common.maindb.utils import setup_driver, teardown_driver
from nucliadb.common.marklogic import documents, vector_views
from nucliadb.common.marklogic.uris import MarkLogicURIs
from nucliadb.migrator.utils import get_latest_version
from nucliadb_protos import knowledgebox_pb2, resources_pb2, utils_pb2, writer_pb2

PROD_KBS = 356
PROD_RESOURCES = 2_200_000
PROD_FIELDS = 3_200_000
PROD_TEXT_BLOCKS = 65_500_000
BIG_KB_RATIO = 0.015
BIG_KB_RESOURCE_SHARE = 0.70
EMPTY_KB_RATIO = 0.12
FIELDS_PER_RESOURCE_DIST = {1: 0.65, 2: 0.25, 3: 0.10}
PARAGRAPHS_PER_FIELD = PROD_TEXT_BLOCKS / PROD_FIELDS
LABELS = {
    "topic": ["finance", "legal", "health", "technology", "science", "sports", "politics", "education"],
    "department": ["sales", "marketing", "engineering", "support", "hr", "operations"],
    "priority": ["low", "medium", "high"],
}
SECURITY_GROUPS = [
    "group-admins",
    "group-managers",
    "group-employees",
    "group-contractors",
    "group-partners",
    "group-customers",
    "group-auditors",
    "group-interns",
]
PUBLIC_RESOURCE_RATIO = 0.2
DEFAULT_PARAGRAPH_SENTENCES = 6


def plan_distribution(scale: float, seed: int, sigma: float, prefix: str) -> list[dict[str, Any]]:
    if not 0 <= scale <= 1:
        raise ValueError("--scale must be between 0 and 1")
    rng = random.Random(seed)
    total_kbs = min(PROD_KBS, max(1, round(PROD_KBS * scale))) if scale > 0 else 0
    total_resources = round(PROD_RESOURCES * scale)
    big_count = min(total_kbs, max(1, round(total_kbs * BIG_KB_RATIO))) if total_kbs else 0
    empty_count = min(total_kbs - big_count, round(total_kbs * EMPTY_KB_RATIO))
    small_count = total_kbs - empty_count - big_count
    big_total = round(total_resources * (BIG_KB_RESOURCE_SHARE if small_count else 1.0))
    small_total = total_resources - big_total

    def split(total: int, count: int) -> list[int]:
        if count == 0:
            return []
        weights = [max(0.05, rng.gauss(1, sigma)) for _ in range(count)]
        weight_sum = sum(weights)
        values = [math.floor(total * weight / weight_sum) for weight in weights]
        values[weights.index(max(weights))] += total - sum(values)
        return values

    plan = (
        [{"tier": "big", "resources": count} for count in split(big_total, big_count)]
        + [{"tier": "small", "resources": count} for count in split(small_total, small_count)]
        + [{"tier": "empty", "resources": 0} for _ in range(empty_count)]
    )
    rng.shuffle(plan)
    for index, item in enumerate(plan):
        item["slug"] = f"{prefix}-{index:03d}"
    return plan


def kb_resource_ranges(
    plan: list[dict[str, Any]], start_offset: int
) -> list[tuple[dict[str, Any], int, int]]:
    """Return every KB's item and local [start, stop) resource-index range to run."""
    ranges = []
    resource_offset = 0
    for item in plan:
        resource_count = item["resources"]
        local_start = min(resource_count, max(0, start_offset - resource_offset))
        ranges.append((item, local_start, resource_count))
        resource_offset += resource_count
    return ranges


def make_ramp_plan(total_resources: int, active_kbs: int, prefix: str) -> list[dict[str, Any]]:
    resources_per_kb, remainder = divmod(total_resources, active_kbs)
    return [
        {
            "tier": "ramp",
            "resources": resources_per_kb + (index < remainder),
            "slug": f"{prefix}-{index:03d}",
        }
        for index in range(active_kbs)
    ]


def ramp_task_stream(
    slug: str,
    kbid: str,
    vectorsets: list[tuple[str, int]],
    start: int,
    stop: int,
) -> Iterator[tuple[str, str, list[tuple[str, int]], int]]:
    return ((slug, kbid, vectorsets, index) for index in range(start, stop))


def build_basic_and_security(
    *,
    slug: str,
    title: str,
    summary: str,
    groups: list[str],
    labels: list[tuple[str, str]],
    created: datetime,
) -> tuple[resources_pb2.Basic, resources_pb2.Security, list[str]]:
    basic = resources_pb2.Basic(slug=slug, title=title, summary=summary)
    basic.created.FromDatetime(created)
    basic.modified.FromDatetime(created)
    basic.metadata.status = writer_pb2.Metadata.Status.PROCESSED
    facets = []
    for labelset, label in labels:
        classification = basic.usermetadata.classifications.add()
        classification.labelset = labelset
        classification.label = label
        facets.append(f"/l/{labelset}/{label}")
    return basic, resources_pb2.Security(access_groups=groups), sorted(facets)


class SyntheticResourceFactory:
    def __init__(self, seed: int, paragraph_sentences: int):
        self.seed = seed
        self.paragraph_sentences = paragraph_sentences
        self.paragraph_sigma = 1.0
        self.paragraph_mu = math.log(PARAGRAPHS_PER_FIELD) - self.paragraph_sigma**2 / 2

    def _paragraph_count(self, rng: random.Random) -> int:
        return max(1, min(1000, round(rng.lognormvariate(self.paragraph_mu, self.paragraph_sigma))))

    def make(self, kbid: str, resource_index: int) -> dict[str, Any]:
        rng = random.Random(f"{self.seed}:{resource_index}")
        fake = Faker()
        fake.seed_instance(f"{self.seed}:{resource_index}")
        field_count = rng.choices(
            list(FIELDS_PER_RESOURCE_DIST), weights=list(FIELDS_PER_RESOURCE_DIST.values())
        )[0]
        fields: dict[str, dict[str, Any]] = {}
        total_paragraphs = 0
        total_bytes = 0
        for field_index in range(field_count):
            field_id = f"text-{field_index}"
            paragraph_count = self._paragraph_count(rng)
            paragraphs = []
            for _ in range(paragraph_count):
                sentences = max(1, self.paragraph_sentences + rng.randint(-2, 2))
                paragraphs.append(
                    fake.paragraph(nb_sentences=sentences, variable_nb_sentences=False)
                )
            body = "\n\n".join(paragraphs)
            fields[field_id] = {"body": body, "paragraphs": paragraphs}
            total_paragraphs += paragraph_count
            total_bytes += len(body.encode("utf-8"))

        labels: list[tuple[str, str]] = []
        for labelset, choices in LABELS.items():
            labels.extend(
                (labelset, value)
                for value in rng.sample(choices, 1 if labelset != "topic" else rng.randint(1, 2))
            )
        groups = (
            []
            if rng.random() < PUBLIC_RESOURCE_RATIO
            else rng.sample(SECURITY_GROUPS, rng.randint(1, 3))
        )
        title = fake.sentence(nb_words=rng.randint(4, 10)).rstrip(".")
        summary = fake.paragraph(nb_sentences=2)
        created = fake.date_time_this_decade(tzinfo=timezone.utc)
        rid = uuid.uuid5(uuid.NAMESPACE_URL, f"nucliadb-sizing:{self.seed}:{kbid}:{resource_index}").hex
        return {
            "rid": rid,
            "slug": f"synth-{self.seed}-{resource_index:09d}",
            "title": title,
            "summary": summary,
            "created": created,
            "groups": groups,
            "labels": labels,
            "fields": fields,
            "paragraphs": total_paragraphs,
            "text_bytes": total_bytes,
        }


def make_chunks_and_embeddings(
    *,
    rid: str,
    field_type: str,
    field_id: str,
    body: str,
    paragraphs: list[str],
    facets: list[str],
    groups: list[str],
    vectorsets: list[tuple[str, int]],
    seed: int,
) -> tuple[FieldProcessedContent, list[ChunkContent], list[ChunkEmbeddingContent]]:
    field = FieldProcessedContent(
        resource_id=rid,
        field_id=field_id,
        field_type=field_type,
        extracted_text=body,
        facets=[],
        resource_facets=facets,
        security=documents.to_json(resources_pb2.Security(access_groups=groups)),
    )
    chunks: list[ChunkContent] = []
    embeddings: list[ChunkEmbeddingContent] = []
    offset = 0
    for paragraph_index, paragraph in enumerate(paragraphs):
        start = body.find(paragraph, offset)
        if start < 0:
            raise ValueError(f"Paragraph offset could not be determined: {rid}/{field_id}")
        end = start + len(paragraph)
        paragraph_id = ParagraphId.from_string(f"{rid}/{field_type}/{field_id}/{start}-{end}")
        chunk_id = str(paragraph_id)
        position = {"start": start, "end": end, "index": paragraph_index}
        chunks.append(
            ChunkContent(
                resource_id=rid,
                field_id=field_id,
                field_type=field_type,
                chunk_id=chunk_id,
                text=paragraph,
                facets=[],
                data={"position": position},
            )
        )
        for vectorset_id, dimension in vectorsets:
            vector_rng = random.Random(f"{seed}:{rid}:{field_id}:{paragraph_index}:{vectorset_id}")
            vector = [vector_rng.uniform(-1.0, 1.0) for _ in range(dimension)]
            norm = math.sqrt(sum(value * value for value in vector)) or 1.0
            vector_id = f"{rid}/{field_type}/{field_id}/{paragraph_index}/{start}-{end}"
            embeddings.append(
                ChunkEmbeddingContent(
                    resource_id=rid,
                    field_id=field_id,
                    field_type=field_type,
                    chunk_id=vector_id,
                    model_id=vectorset_id,
                    chunk_embedding_index=0,
                    vector=[value / norm for value in vector],
                    data={"position": position},
                )
            )
        offset = end + 2
    return field, chunks, embeddings


async def write_resource(
    driver: MarkLogicDriver,
    kbid: str,
    generated: dict[str, Any],
    vectorsets: list[tuple[str, int]],
    seed: int,
) -> tuple[bool, int, int, int]:
    rid = generated["rid"]
    async with driver.rw_transaction(kbid=kbid) as txn:
        if await datamanagers.resources.exists(txn, kbid=kbid, rid=rid):
            return False, 0, 0, 0
        basic, security, facets = build_basic_and_security(
            slug=generated["slug"],
            title=generated["title"],
            summary=generated["summary"],
            groups=generated["groups"],
            labels=generated["labels"],
            created=generated["created"],
        )
        content = ResourceCreateContent(
            slug=generated["slug"],
            basic=documents.to_json(basic),
            origin=None,
            security=documents.to_json(security),
            title=generated["title"],
            processing_status="PROCESSED",
            created_at=generated["created"].isoformat(),
            modified_at=generated["created"].isoformat(),
            facets=facets,
        )
        await datamanagers.resources.create_resource(txn, kbid=kbid, rid=rid, data=content)
        field_count = chunk_count = embedding_count = 0
        for field_id, field_data in generated["fields"].items():
            field_proto = resources_pb2.FieldText(
                body=field_data["body"], format=resources_pb2.FieldText.PLAIN
            )
            field_value = documents.to_json(field_proto)
            status = writer_pb2.FieldStatus(status=writer_pb2.FieldStatus.Status.PROCESSED)
            await datamanagers.fields.create_document(
                txn,
                FieldContent(
                    resource_id=rid,
                    field_id=field_id,
                    field_type="t",
                    value=field_value,
                    md5=None,
                    status=documents.to_json(status),
                ),
            )
            processed_field, chunks, embeddings = await asyncio.to_thread(
                make_chunks_and_embeddings,
                rid=rid,
                field_type="t",
                field_id=field_id,
                body=field_data["body"],
                paragraphs=field_data["paragraphs"],
                facets=facets,
                groups=generated["groups"],
                vectorsets=vectorsets,
                seed=seed,
            )
            await datamanagers.fields.create_processed_documents(
                txn, field=processed_field, chunks=chunks, embeddings=embeddings
            )
            field_count += 1
            chunk_count += len(chunks)
            embedding_count += len(embeddings)
        for field_id in ("title", "summary"):
            body = generated[field_id]
            paragraphs = body.split("\n\n")
            processed_field, chunks, embeddings = await asyncio.to_thread(
                make_chunks_and_embeddings,
                rid=rid,
                field_type="a",
                field_id=field_id,
                body=body,
                paragraphs=paragraphs,
                facets=facets,
                groups=generated["groups"],
                vectorsets=vectorsets,
                seed=seed,
            )
            await datamanagers.fields.create_processed_documents(
                txn, field=processed_field, chunks=chunks, embeddings=embeddings
            )
            chunk_count += len(chunks)
            embedding_count += len(embeddings)
        await txn.commit()
    return True, field_count, chunk_count, embedding_count


async def get_kb_vectorsets(driver: MarkLogicDriver, kbid: str) -> list[tuple[str, int]]:
    async with driver.ro_transaction(kbid=kbid) as txn:
        configs = [config async for _, config in datamanagers.vectorsets.iter(txn, kbid=kbid)]
    vectorsets = []
    for config in configs:
        dimension = config.vectorset_index_config.vector_dimension
        if dimension <= 0:
            raise ValueError(f"KB {kbid}: vectorset {config.vectorset_id} has no valid dimension")
        await vector_views.ensure_view(driver, driver.kb_database(kbid), config.vectorset_id, dimension)
        vectorsets.append((config.vectorset_id, dimension))
    if not vectorsets:
        raise ValueError(f"KB {kbid} has no configured vectorsets")
    return vectorsets


async def find_resume_offset(
    driver: MarkLogicDriver, kbid: str, seed: int, resource_count: int
) -> int:
    """Find the first missing resource in the contiguous synthetic-slug prefix."""
    low = 0
    high = resource_count
    probes = 0
    started = time.monotonic()
    print(
        f"Auto-resume KB={kbid}: searching indexes [0, {resource_count})",
        flush=True,
    )
    async with driver.ro_transaction(kbid=kbid) as txn:
        while low < high:
            midpoint = (low + high) // 2
            slug = f"synth-{seed}-{midpoint:09d}"
            probe_started = time.monotonic()
            exists = await datamanagers.resources.slug_exists(txn, kbid=kbid, slug=slug)
            probes += 1
            print(
                f"Auto-resume KB={kbid}: index={midpoint} exists={exists} "
                f"probe={time.monotonic() - probe_started:.2f}s "
                f"elapsed={time.monotonic() - started:.1f}s",
                flush=True,
            )
            if exists:
                low = midpoint + 1
            else:
                high = midpoint
    print(
        f"Auto-resume KB={kbid}: first-missing={low} probes={probes} "
        f"elapsed={time.monotonic() - started:.1f}s",
        flush=True,
    )
    return low


async def ensure_synthetic_kb(
    driver: MarkLogicDriver,
    *,
    slug: str,
    vectorset_id: str,
    vector_dimension: int,
) -> str:
    started = time.monotonic()
    def log_phase(message: str) -> None:
        print(
            f"ensure_synthetic_kb slug={slug} elapsed={time.monotonic() - started:.3f}s {message}",
            flush=True,
        )

    phase_started = time.monotonic()
    log_phase("looking up KB slug")
    async with driver.ro_transaction(kbid=None) as txn:
        kbid = await datamanagers.kb.get_kbid(txn, slug=slug)
    log_phase(f"KB lookup complete in {time.monotonic() - phase_started:.3f}s kbid={kbid}")
    created = kbid is None
    if created:
        kbid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"synthetic-marklogic:{slug}"))
        phase_started = time.monotonic()
        log_phase(f"registering new KB kbid={kbid}")
        async with driver.rw_transaction(kbid=None) as txn:
            await datamanagers.kb.set_slug(txn, slug=slug, kbid=kbid)
            log_phase("KB slug registered; committing")
            await txn.commit()
        log_phase(f"KB registration complete in {time.monotonic() - phase_started:.3f}s")

    phase_started = time.monotonic()
    log_phase(f"ensuring KB database kbid={kbid}")
    database = await driver.ensure_kb_database(kbid)
    log_phase(
        f"KB database ready in {time.monotonic() - phase_started:.3f}s database={database}"
    )
    phase_started = time.monotonic()
    log_phase("opening KB config transaction")
    async with driver.rw_transaction(kbid=kbid) as txn:
        log_phase("reading vectorset configs")
        configs = [config async for _, config in datamanagers.vectorsets.iter(txn, kbid=kbid)]
        log_phase(f"read {len(configs)} vectorset configs")
        if not configs:
            phase_started = time.monotonic()
            log_phase("initializing vectorset storage")
            await datamanagers.vectorsets.initialize(txn, kbid=kbid)
            log_phase(f"vectorset storage initialized in {time.monotonic() - phase_started:.3f}s")
            phase_started = time.monotonic()
            log_phase(f"writing default vectorset config id={vectorset_id}")
            await datamanagers.vectorsets.set(
                txn,
                kbid=kbid,
                config=knowledgebox_pb2.VectorSetConfig(
                    vectorset_id=vectorset_id,
                    vectorset_index_config=knowledgebox_pb2.VectorIndexConfig(
                        similarity=utils_pb2.VectorSimilarity.COSINE,
                        vector_type=knowledgebox_pb2.VectorType.DENSE_F32,
                        vector_dimension=vector_dimension,
                    ),
                    storage_key_kind=knowledgebox_pb2.VectorSetConfig.StorageKeyKind.VECTORSET_PREFIX,
                ),
            )
            log_phase(f"default vectorset config written in {time.monotonic() - phase_started:.3f}s")
        config = knowledgebox_pb2.KnowledgeBoxConfig(
            title=slug,
            slug=slug,
            migration_version=get_latest_version(),
            enforce_security=True,
        )
        phase_started = time.monotonic()
        log_phase("reading KB metadata")
        kb_data = await documents.read(txn, MarkLogicURIs.kb_data()) or {"kbid": kbid}
        log_phase(f"KB metadata read in {time.monotonic() - phase_started:.3f}s")
        kb_data["config"] = documents.to_json(config)
        phase_started = time.monotonic()
        log_phase("writing KB metadata")
        await documents.write(
            txn,
            uri=MarkLogicURIs.kb_data(),
            collection=MarkLogicCollections.KNOWLEDGEBOXES,
            content=kb_data,
        )
        log_phase(f"KB metadata written in {time.monotonic() - phase_started:.3f}s")
        phase_started = time.monotonic()
        log_phase("committing KB config transaction")
        await txn.commit()
        log_phase(f"KB config transaction committed in {time.monotonic() - phase_started:.3f}s")

    phase_started = time.monotonic()
    log_phase("re-reading vectorset configs")
    async with driver.ro_transaction(kbid=kbid) as txn:
        configs = [config async for _, config in datamanagers.vectorsets.iter(txn, kbid=kbid)]
    log_phase(f"re-read {len(configs)} vectorset configs in {time.monotonic() - phase_started:.3f}s")
    for config in configs:
        dimension = config.vectorset_index_config.vector_dimension
        if dimension <= 0:
            raise ValueError(f"KB {kbid}: vectorset {config.vectorset_id} has no valid dimension")
        phase_started = time.monotonic()
        log_phase(f"ensuring vector view id={config.vectorset_id} dimension={dimension}")
        await vector_views.ensure_view(driver, database, config.vectorset_id, dimension)
        log_phase(
            f"vector view ready id={config.vectorset_id} in {time.monotonic() - phase_started:.3f}s"
        )
    if created:
        print(
            f"Created synthetic KB {slug} ({kbid}), database={database}, "
            f"elapsed={time.monotonic() - started:.3f}s"
        )
    log_phase(f"setup complete kbid={kbid} database={database}")
    return kbid


async def delete_synthetic_kbs(
    driver: MarkLogicDriver, prefix: str, *, confirm: bool, concurrency: int
) -> int:
    async with driver.ro_transaction(kbid=None) as txn:
        matches = [
            (kbid, slug)
            async for kbid, slug in datamanagers.kb.iter(txn, slug_prefix=f"{prefix}-")
            if slug[len(prefix) + 1 :].isascii() and slug[len(prefix) + 1 :].isdigit()
        ]
    if not confirm:
        for kbid, slug in matches:
            print(f"Would delete {slug} ({kbid}) and its MarkLogic database")
    else:
        semaphore = asyncio.Semaphore(concurrency)

        async def delete_one(kbid: str, slug: str) -> None:
            async with semaphore:
                async with driver.rw_transaction(kbid=None) as txn:
                    await datamanagers.kb.delete(txn, kbid=kbid)
                    await txn.commit()
                print(f"Deleted {slug} ({kbid})")

        await asyncio.gather(*(delete_one(kbid, slug) for kbid, slug in matches))
    print(f"Matched={len(matches)} deleted={len(matches) if confirm else 0}")
    if not confirm:
        print("Preview only. Pass --yes with --delete to remove these KBs.")
    return len(matches)


def committed_document_count(fields: int, chunks: int, embeddings: int) -> int:
    return 3 + 2 * fields + chunks + embeddings


def ramp_report(
    stats: Counter,
    elapsed: float,
    latencies: list[float],
) -> dict[str, Any]:
    ordered = sorted(latencies)

    def percentile(fraction: float) -> float | None:
        return round(ordered[max(0, math.ceil(len(ordered) * fraction) - 1)], 3) if ordered else None

    return {
        "elapsed_seconds": round(elapsed, 3),
        "created": stats["created"],
        "skipped": stats["skipped"],
        "failed": stats["failed"],
        "documents": stats["documents"],
        "documents_per_second": round(stats["documents"] / max(elapsed, 1e-9), 2),
        "resources_per_second": round(stats["created"] / max(elapsed, 1e-9), 2),
        "write_latency_p50_seconds": percentile(0.50),
        "write_latency_p95_seconds": percentile(0.95),
        "write_latency_p99_seconds": percentile(0.99),
        "generation_seconds": round(stats["generation_seconds"], 3),
    }


async def run_ramp_stage(
    driver: MarkLogicDriver,
    tasks: Iterator[tuple[str, str, list[tuple[str, int]], int]],
    generator: SyntheticResourceFactory,
    args: argparse.Namespace,
    concurrency: int,
    emit: Callable[[dict[str, Any]], None],
) -> dict[str, Any]:
    stats: Counter = Counter()
    latencies: list[float] = []
    kb_stats: dict[str, Counter] = {}
    kb_latencies: dict[str, list[float]] = {}
    started = time.monotonic()
    deadline = started + args.stage_seconds
    exhausted = False

    async def worker() -> None:
        nonlocal exhausted
        while time.monotonic() < deadline:
            task = next(tasks, None)
            if task is None:
                exhausted = True
                return
            slug, kbid, vectorsets, resource_index = task
            local_stats = kb_stats.setdefault(slug, Counter())
            local_latencies = kb_latencies.setdefault(slug, [])
            generation_started = time.monotonic()
            try:
                generated = await asyncio.to_thread(generator.make, kbid, resource_index)
                generation_elapsed = time.monotonic() - generation_started
                stats["generation_seconds"] += generation_elapsed
                local_stats["generation_seconds"] += generation_elapsed
                write_started = time.monotonic()
                if stats["created"] + stats["skipped"] + stats["failed"] == 0:
                    print(
                        f"Starting first resource write kb={slug} "
                        f"resource_index={resource_index}",
                        flush=True,
                    )
                async with asyncio.timeout(args.write_timeout):
                    created, fields, chunks, embeddings = await write_resource(
                        driver, kbid, generated, vectorsets, args.seed
                    )
                if created:
                    latency = time.monotonic() - write_started
                    document_count = committed_document_count(fields, chunks, embeddings)
                    stats["created"] += 1
                    stats["documents"] += document_count
                    local_stats["created"] += 1
                    local_stats["documents"] += document_count
                    latencies.append(latency)
                    local_latencies.append(latency)
                    if stats["created"] == 1:
                        print(
                            f"First resource committed kb={slug} "
                            f"resource_index={resource_index} write_seconds={latency:.2f}",
                            flush=True,
                        )
                else:
                    stats["skipped"] += 1
                    local_stats["skipped"] += 1
            except Exception as exc:
                stats["failed"] += 1
                local_stats["failed"] += 1
                print(
                    f"Ramp write failed kb={slug} kbid={kbid} resource_index={resource_index}: {exc!r}",
                    flush=True,
                )

    previous: Counter = Counter()
    previous_kb: dict[str, Counter] = {}
    previous_time = started
    previous_latency_count = 0
    previous_kb_latency_counts: dict[str, int] = {}

    def report_interval(*, final: bool = False) -> None:
        nonlocal previous, previous_kb, previous_time, previous_latency_count
        nonlocal previous_kb_latency_counts
        now = time.monotonic()
        window = Counter({key: value - previous[key] for key, value in stats.items()})
        knowledge_boxes = {}
        for slug, local_stats in kb_stats.items():
            local_previous = previous_kb.get(slug, Counter())
            local_window = Counter(
                {key: value - local_previous[key] for key, value in local_stats.items()}
            )
            local_samples = kb_latencies[slug][previous_kb_latency_counts.get(slug, 0) :]
            knowledge_boxes[slug] = ramp_report(local_window, now - previous_time, local_samples)
        emit(
            {
                "event": "interval",
                "concurrency": concurrency,
                "active_kbs": args.active_kbs,
                "phase": "load" if now < deadline else "drain",
                "stage_elapsed_seconds": round(now - started, 3),
                "stage_documents": stats["documents"],
                "knowledge_boxes": knowledge_boxes,
                "final_interval": final,
                "write_latencies_seconds": latencies[previous_latency_count:],
                **ramp_report(window, now - previous_time, latencies[previous_latency_count:]),
            }
        )
        previous = stats.copy()
        previous_kb = {slug: local_stats.copy() for slug, local_stats in kb_stats.items()}
        previous_time = now
        previous_latency_count = len(latencies)
        previous_kb_latency_counts = {
            slug: len(local_samples) for slug, local_samples in kb_latencies.items()
        }

    async def reporter() -> None:
        while True:
            await asyncio.sleep(args.report_interval)
            report_interval()

    progress = asyncio.create_task(reporter())
    workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
    interrupted = True
    try:
        await asyncio.gather(*workers)
        interrupted = False
    finally:
        progress.cancel()
        for task in workers:
            task.cancel()
        await asyncio.gather(progress, *workers, return_exceptions=True)
        report_interval(final=True)
        elapsed = time.monotonic() - started
        result = {
            "event": "stage",
            "concurrency": concurrency,
            "active_kbs": args.active_kbs,
            "requested_seconds": args.stage_seconds,
            "drain_seconds": round(max(0.0, elapsed - args.stage_seconds), 3),
            "exhausted": exhausted,
            "interrupted": interrupted,
            "knowledge_boxes": {
                slug: ramp_report(local_stats, elapsed, kb_latencies[slug])
                for slug, local_stats in kb_stats.items()
            },
            **ramp_report(stats, elapsed, latencies),
        }
        emit(result)
    return result


async def run_ramp(
    driver: MarkLogicDriver,
    args: argparse.Namespace,
    plan: list[dict[str, Any]],
    start_offset: int,
) -> None:
    kb_configs = []
    ranges = kb_resource_ranges(plan, start_offset)
    for item, local_start, resource_count in ranges:
        if local_start >= resource_count:
            print(f"Skipping KB setup before start resource: {item['slug']}", flush=True)
            continue
        kbid = await ensure_synthetic_kb(
            driver,
            slug=item["slug"],
            vectorset_id=args.vectorset_id,
            vector_dimension=args.vector_dimension,
        )
        vectorsets = await get_kb_vectorsets(driver, kbid)
        resume_offset = (
            await find_resume_offset(driver, kbid, args.seed, resource_count)
            if args.auto_resume
            else 0
        )
        effective_start = max(local_start, resume_offset)
        print(
            f"KB {item['slug']} start={effective_start} (manual={local_start}, "
            f"detected={resume_offset})",
            flush=True,
        )
        if effective_start < resource_count:
            kb_configs.append((item["slug"], kbid, vectorsets, effective_start, resource_count))
    report_file = Path(args.report_file).open("a") if args.report_file else None
    run_id = uuid.uuid4().hex
    run_started = time.monotonic()

    def emit(report: dict[str, Any]) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        run_elapsed = time.monotonic() - run_started
        line = json.dumps(
            {
                "run_id": run_id,
                "timestamp": timestamp,
                "run_elapsed_seconds": round(run_elapsed, 3),
                **report,
            }
        )
        if report_file is not None:
            report_file.write(line + "\n")
            report_file.flush()
        if report["event"] in ("interval", "stage"):
            print(
                f"{timestamp} event={report['event']} elapsed={run_elapsed:.1f}s "
                f"concurrency={report['concurrency']} phase={report.get('phase', 'complete')} "
                f"window={report['elapsed_seconds']:.3f}s "
                f"documents={report['documents']} "
                f"rate={report['documents_per_second']:.2f} docs/s "
                f"p50={report['write_latency_p50_seconds']}s "
                f"p95={report['write_latency_p95_seconds']}s "
                f"p99={report['write_latency_p99_seconds']}s "
                f"created={report['created']} skipped={report['skipped']} failed={report['failed']}",
                flush=True,
            )
        else:
            print(line, flush=True)

    try:
        emit(
            {
                "event": "configuration",
                "settings": vars(args),
                "document_count_formula": "3 + 2 * text_fields + chunks + embeddings",
                "knowledge_boxes": [
                    {"slug": slug, "kbid": kbid, "database": driver.kb_database(kbid)}
                    for slug, kbid, _, _, _ in kb_configs
                ],
                "workload": "round-robin across active KBs; one transaction per resource",
            }
        )
        streams = []
        for slug, kbid, vectorsets, local_start, resource_count in kb_configs:
            streams.append(ramp_task_stream(slug, kbid, vectorsets, local_start, resource_count))
        tasks = (task for row in zip_longest(*streams) for task in row if task is not None)
        generator = SyntheticResourceFactory(args.seed, args.paragraph_sentences)
        stages = []
        concurrency = args.concurrency
        while True:
            result = await run_ramp_stage(driver, tasks, generator, args, concurrency, emit)
            stages.append(result)
            if result["exhausted"] or concurrency >= args.max_concurrency:
                break
            concurrency = min(args.max_concurrency, concurrency + args.concurrency_step)
        emit({"event": "summary", "stages": stages})
        if any(stage["failed"] for stage in stages):
            raise SystemExit(1)
    finally:
        if report_file is not None:
            report_file.close()


async def run(args):
    plan = (
        make_ramp_plan(round(PROD_RESOURCES * args.scale), args.active_kbs, args.kb_prefix)
        if args.ramp
        else plan_distribution(args.scale, args.seed, args.sigma, args.kb_prefix)
    )
    planned_resources = sum(item["resources"] for item in plan)
    start_resource_offset = args.start_resource_number - 1
    if start_resource_offset > planned_resources:
        raise ValueError(
            f"--start-resource-number must be at most {planned_resources + 1} for this plan"
        )
    resources_to_run = planned_resources - start_resource_offset
    print(
        json.dumps(
            {
                "scale": args.scale,
                "knowledge_boxes": len(plan),
                "planned_resources": planned_resources,
                "start_resource_number": args.start_resource_number,
                "resources_to_run": resources_to_run,
                "planned_fields": round(planned_resources * PROD_FIELDS / PROD_RESOURCES),
                "planned_paragraphs": round(planned_resources * PROD_TEXT_BLOCKS / PROD_RESOURCES),
                "plan": plan,
            },
            indent=2,
        )
    )
    if args.dry_run:
        return
    driver = await setup_driver()
    if not isinstance(driver, MarkLogicDriver):
        raise TypeError("Synthetic direct ingestion requires MarkLogicDriver")
    if args.delete:
        try:
            await delete_synthetic_kbs(
                driver, args.kb_prefix, confirm=args.yes, concurrency=args.concurrency
            )
        finally:
            await teardown_driver()
        return
    stats: Counter = Counter()
    generator = SyntheticResourceFactory(args.seed, args.paragraph_sentences)
    started = time.monotonic()
    try:
        if args.ramp:
            await run_ramp(driver, args, plan, start_resource_offset)
            return
        semaphore = asyncio.Semaphore(args.concurrency)
        queue: asyncio.Queue[tuple[str, str, list[tuple[str, int]], int] | None] = asyncio.Queue(
            maxsize=args.concurrency * 2
        )

        def print_worker_progress(
            worker_id: int,
            kbid: str,
            slug: str,
            worker_stats: Counter,
            worker_started: float,
            force: bool = False,
        ) -> None:
            processed = worker_stats["created"] + worker_stats["skipped"] + worker_stats["failed"]
            reported = worker_stats["reported"]
            if (
                not processed
                or processed == reported
                or (not force and processed - reported < args.log_every)
            ):
                return
            elapsed = max(time.monotonic() - worker_started, 1e-9)
            total_elapsed = max(time.monotonic() - started, 1e-9)
            global_processed = stats["created"] + stats["skipped"] + stats["failed"]
            print(
                f"worker={worker_id}/{args.concurrency} kb={slug} kbid={kbid} "
                f"resources={processed} rate={processed / elapsed:.2f} res/s "
                f"created={worker_stats['created']} skipped={worker_stats['skipped']} "
                f"failed={worker_stats['failed']} | total={global_processed}/{resources_to_run} "
                f"rate={global_processed / total_elapsed:.2f} res/s"
            )
            worker_stats["reported"] = processed

        async def ingest_worker(worker_id: int):
            worker_started = time.monotonic()
            worker_stats: Counter = Counter()
            last_slug = "-"
            last_kbid = "-"
            first_resource = True
            while True:
                task = await queue.get()
                if task is None:
                    print_worker_progress(
                        worker_id, last_kbid, last_slug, worker_stats, worker_started, force=True
                    )
                    return
                kbid, slug, vectorsets, resource_index = task
                last_slug = slug
                last_kbid = kbid
                async with semaphore:
                    try:
                        generated = await asyncio.to_thread(
                            generator.make, kbid, resource_index
                        )
                        if first_resource:
                            print(
                                f"Worker {worker_id} starting first resource write "
                                f"kb={slug} resource_index={resource_index}",
                                flush=True,
                            )
                        async with asyncio.timeout(args.write_timeout):
                            created, fields, chunks, embeddings = await write_resource(
                                driver,
                                kbid,
                                generated,
                                vectorsets,
                                args.seed,
                            )
                    except Exception as exc:
                        print(
                            f"Failed direct MarkLogic resource processing kbid={kbid} "
                            f"resource_index={resource_index}: {exc!r}",
                            flush=True,
                        )
                        stats["failed"] += 1
                        worker_stats["failed"] += 1
                        print_worker_progress(worker_id, kbid, slug, worker_stats, worker_started)
                        continue
                    stats["skipped" if not created else "created"] += 1
                    worker_stats["skipped" if not created else "created"] += 1
                    if first_resource:
                        outcome = "committed" if created else "already exists; skipped"
                        print(
                            f"Worker {worker_id} first resource {outcome} "
                            f"kb={slug} resource_index={resource_index}",
                            flush=True,
                        )
                        first_resource = False
                    if created:
                        stats["fields"] += fields
                        stats["chunks"] += chunks
                        stats["embeddings"] += embeddings
                        stats["text_paragraphs"] += generated["paragraphs"]
                        stats["text_bytes"] += generated["text_bytes"]
                        stats["public_resources"] += not generated["groups"]
                        for labelset, label in generated["labels"]:
                            stats[f"label:{labelset}/{label}"] += 1
                        for group in generated["groups"]:
                            stats[f"group:{group}"] += 1
                    print_worker_progress(worker_id, kbid, slug, worker_stats, worker_started)

        workers = [
            asyncio.create_task(ingest_worker(worker_id)) for worker_id in range(1, args.concurrency + 1)
        ]
        try:
            for item, local_start, resource_count in kb_resource_ranges(plan, start_resource_offset):
                slug = item["slug"]
                if local_start >= resource_count:
                    print(f"Skipping KB before start resource: {slug}", flush=True)
                    continue
                kbid = await ensure_synthetic_kb(
                    driver,
                    slug=slug,
                    vectorset_id=args.vectorset_id,
                    vector_dimension=args.vector_dimension,
                )
                if args.auto_resume:
                    detected_start = await find_resume_offset(
                        driver, kbid, args.seed, resource_count
                    )
                    local_start = max(local_start, detected_start)
                    print(
                        f"KB {slug} start={local_start} (detected={detected_start})",
                        flush=True,
                    )
                if local_start >= resource_count:
                    continue
                vectorsets = await get_kb_vectorsets(driver, kbid)
                for resource_index in range(local_start, resource_count):
                    await queue.put((kbid, slug, vectorsets, resource_index))
            for _ in workers:
                await queue.put(None)
            await asyncio.gather(*workers)
        except BaseException:
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise
        report = {
            "scale": args.scale,
            "seed": args.seed,
            "knowledge_boxes": len(plan),
            "planned_resources": planned_resources,
            "start_resource_number": args.start_resource_number,
            "resources_to_run": resources_to_run,
            "elapsed_seconds": round(time.monotonic() - started, 1),
            "stats": dict(stats),
        }
        print(json.dumps(report, indent=2))
        if stats["failed"]:
            raise SystemExit(1)
    finally:
        await teardown_driver()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale", type=float, default=0.15)
    parser.add_argument(
        "--start-resource-number",
        type=int,
        default=1,
        help="1-based resource position in the deterministic plan; earlier resources are skipped",
    )
    parser.add_argument(
        "--auto-resume",
        action="store_true",
        help="Use indexed synthetic slugs to find the first missing resource in each KB",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--sigma", type=float, default=0.4)
    parser.add_argument("--kb-prefix", default="ml-sizing-kb")
    parser.add_argument("--vectorset-id", default="synthetic-vectors")
    parser.add_argument("--vector-dimension", type=int, default=768)
    parser.add_argument("--paragraph-sentences", type=int, default=DEFAULT_PARAGRAPH_SENTENCES)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument(
        "--ramp", action="store_true", help="Ramp concurrent writes across synthetic KB databases"
    )
    parser.add_argument(
        "--active-kbs",
        type=int,
        default=1,
        help="Number of KB databases to write across in ramp mode",
    )
    parser.add_argument("--max-concurrency", type=int, default=64)
    parser.add_argument("--concurrency-step", type=int, default=8)
    parser.add_argument("--stage-seconds", type=float, default=120)
    parser.add_argument("--report-interval", type=float, default=10)
    parser.add_argument("--write-timeout", type=float, default=120)
    parser.add_argument("--report-file", help="Append ramp configuration, intervals and stages as JSONL")
    parser.add_argument("--log-every", type=int, default=1000)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--delete", action="store_true", help="Preview deletion of matching synthetic KBs"
    )
    parser.add_argument("--yes", action="store_true", help="Confirm deletion when using --delete")
    args = parser.parse_args()
    if args.concurrency <= 0 or args.log_every <= 0 or args.active_kbs <= 0:
        parser.error("--concurrency, --log-every and --active-kbs must be positive")
    if args.ramp and (args.concurrency_step <= 0 or args.max_concurrency < args.concurrency):
        parser.error("--concurrency-step must be positive and --max-concurrency >= --concurrency")
    if any(
        not math.isfinite(value) or value <= 0
        for value in (args.stage_seconds, args.report_interval, args.write_timeout)
    ):
        parser.error(
            "--stage-seconds, --report-interval and --write-timeout must be finite and positive"
        )
    if args.ramp and args.delete:
        parser.error("--ramp cannot be combined with --delete")
    if args.vector_dimension <= 0:
        parser.error("--vector-dimension must be positive")
    if not 0 <= args.scale <= 1:
        parser.error("--scale must be between 0 and 1")
    if args.start_resource_number < 1:
        parser.error("--start-resource-number must be at least 1")
    if args.yes and not args.delete:
        parser.error("--yes can only be used with --delete")
    if args.delete and args.dry_run:
        parser.error("--delete performs its own preview; do not combine it with --dry-run")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
