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

Write continuously to one KB, creating it if needed (stop with Ctrl+C):
    python synthetic_marklogic_ingest.py --continuous --kb-slug ml-load-test \
        --concurrency 8

    Stop after the KB reaches 100000 total chunks (including chunks already present):
        python synthetic_marklogic_ingest.py --continuous --kb-slug ml-load-test \
            --max-chunks 100000 --concurrency 8

    Continuous mode uses random resource IDs and slugs without duplicate checks.
    It ignores --scale and --auto-resume.

    Ramp the target resource rate from 5 to 100 resources/s over 300 seconds, then stop:
        python synthetic_marklogic_ingest.py --continuous --kb-slug ml-load-test \
            --rate-start 5 --rate-end 100 --duration-seconds 300 --concurrency 32

    The duration excludes KB setup. Workers are paced before resource generation;
    --concurrency limits in-flight work. Slow writes can limit the achieved rate.
    At the deadline no new work starts, and already-started resources are drained.

    Ramp across 10 randomly selected existing KBs with matching slug prefixes:
        python synthetic_marklogic_ingest.py --continuous --random-kbs 10 \
            --kb-prefix ml-sizing-kb --rate-start 5 --rate-end 100 \
            --duration-seconds 300 --concurrency 32

    Rates and concurrency are shared across the selected KBs, not applied per KB.

Use a fresh KB prefix or seed to avoid measuring skipped resources. Ramp mode
prepares all configured KBs before timing, distributes resources evenly, and
interleaves writes across the databases. It drains each stage and measures the
direct driver workload, not end-to-end processing or raw server capacity. Watch
client CPU as well as cluster CPU, disk, merges, and indexing; synthetic
text/vector generation and the client connection pool can limit load.

MarkLogic connection uses the normal MARKLOGIC_SERVER_* and DRIVER settings.


###

python /synthetic_marklogic_ingest.py --scale 0.2 --kb-prefix ml-sizing-kb --concurrency=20  --auto-resume

###


"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import random
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from itertools import count, cycle, zip_longest
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
SYNTHETIC_CHECKPOINT_COLLECTION = "synthetic_ingest_checkpoints"


class ContinuousRateRamp:
    def __init__(self, start_rate: float, end_rate: float, duration: float):
        self.start_rate = start_rate
        self.end_rate = end_rate
        self.duration = duration
        self.started = time.monotonic()
        self.deadline = self.started + duration
        self.next_start = self.started
        self.lock = asyncio.Lock()

    def target_rate(self) -> float:
        fraction = min(1.0, max(0.0, (time.monotonic() - self.started) / self.duration))
        return self.start_rate + (self.end_rate - self.start_rate) * fraction

    async def acquire(self) -> bool:
        async with self.lock:
            now = time.monotonic()
            scheduled = max(now, self.next_start)
            if scheduled >= self.deadline:
                return False
            if scheduled > now:
                await asyncio.sleep(scheduled - now)
            if time.monotonic() >= self.deadline:
                return False
            rate = self.target_rate()
            slope = (self.end_rate - self.start_rate) / self.duration
            delay = 2 / (rate + math.sqrt(rate * rate + 2 * slope))
            self.next_start = time.monotonic() + delay
            return True


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
    *,
    check_existing: bool = True,
) -> tuple[bool, int, int, int]:
    rid = generated["rid"]
    async with driver.rw_transaction(kbid=kbid) as txn:
        if check_existing and await datamanagers.resources.exists(txn, kbid=kbid, rid=rid):
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


def checkpoint_fingerprint(plan: list[dict[str, Any]], seed: int) -> str:
    plan_key = json.dumps(
        {
            "seed": seed,
            "knowledge_boxes": [
                {"slug": item["slug"], "resources": item["resources"]} for item in plan
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(plan_key.encode()).hexdigest()


def checkpoint_uri(fingerprint: str) -> str:
    return f"/synthetic_marklogic_ingest/checkpoints/{fingerprint}.json"


async def load_completed_kb_prefix(driver: MarkLogicDriver, fingerprint: str) -> int:
    async with driver.ro_transaction(kbid=None) as txn:
        checkpoint = await documents.read(txn, checkpoint_uri(fingerprint))
    return int(checkpoint.get("completed_kbs", 0)) if checkpoint else 0


async def save_completed_kb_prefix(
    driver: MarkLogicDriver, fingerprint: str, completed_kbs: int
) -> None:
    async with driver.rw_transaction(kbid=None) as txn:
        await documents.write(
            txn,
            uri=checkpoint_uri(fingerprint),
            collection=SYNTHETIC_CHECKPOINT_COLLECTION,
            content={"fingerprint": fingerprint, "completed_kbs": completed_kbs},
        )
        await txn.commit()


async def resolve_ingest_kb(
    driver: MarkLogicDriver, item: dict[str, Any], args: argparse.Namespace
) -> str:
    if "kbid" in item:
        async with driver.ro_transaction(kbid=None) as txn:
            if not await datamanagers.kb.exists(txn, kbid=item["kbid"]):
                raise ValueError(f"KnowledgeBox {item['kbid']} does not exist")
        return item["kbid"]
    if getattr(args, "kb_slug", None):
        async with driver.ro_transaction(kbid=None) as txn:
            kbid = await datamanagers.kb.get_kbid(txn, slug=item["slug"])
        if kbid is not None:
            return kbid
    return await ensure_synthetic_kb(
        driver,
        slug=item["slug"],
        vectorset_id=args.vectorset_id,
        vector_dimension=args.vector_dimension,
    )


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


async def select_random_kbs(
    driver: MarkLogicDriver, prefix: str, amount: int
) -> list[dict[str, Any]]:
    async with driver.ro_transaction(kbid=None) as txn:
        matches = [
            (kbid, slug)
            async for kbid, slug in datamanagers.kb.iter(txn, slug_prefix=prefix)
        ]
    if len(matches) < amount:
        raise ValueError(
            f"Requested {amount} KBs but only {len(matches)} match slug prefix {prefix!r}"
        )
    return [
        {"tier": "existing", "kbid": kbid, "slug": slug, "resources": math.inf}
        for kbid, slug in random.sample(matches, amount)
    ]


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


def generated_chunk_count(generated: dict[str, Any]) -> int:
    text_chunks = sum(len(field_data["paragraphs"]) for field_data in generated["fields"].values())
    attribute_chunks = sum(
        len(generated[field_id].split("\n\n")) for field_id in ("title", "summary")
    )
    return text_chunks + attribute_chunks


async def count_kb_chunks(driver: MarkLogicDriver, kbid: str) -> int:
    javascript = (
        "fn.count(cts.uris('', ['document'], "
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.CHUNKS.value)})))"
    )
    result = await driver.client.eval(javascript, params={"database": driver.kb_database(kbid)})
    return int(result[0]) if result else 0


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
        kbid = await resolve_ingest_kb(driver, item, args)
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
    continuous = getattr(args, "continuous", False)
    random_kbs = getattr(args, "random_kbs", None)
    plan = (
        make_ramp_plan(round(PROD_RESOURCES * args.scale), args.active_kbs, args.kb_prefix)
        if args.ramp
        else plan_distribution(args.scale, args.seed, args.sigma, args.kb_prefix)
    )
    if getattr(args, "kbid", None):
        plan = [
            {
                "tier": "single",
                "slug": args.kbid,
                "kbid": args.kbid,
                "resources": round(PROD_RESOURCES * args.scale),
            }
        ]
    if getattr(args, "kb_slug", None):
        plan = [
            {
                "tier": "single",
                "slug": args.kb_slug,
                "resources": round(PROD_RESOURCES * args.scale),
            }
        ]
    if continuous and not random_kbs:
        plan[0]["resources"] = math.inf
    if random_kbs:
        plan = []
    planned_resources = sum(item["resources"] for item in plan)
    if random_kbs:
        planned_resources = math.inf
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
                "planned_resources": None if continuous else planned_resources,
                "start_resource_number": args.start_resource_number,
                "resources_to_run": None if continuous else resources_to_run,
                "planned_fields": None if continuous else round(planned_resources * PROD_FIELDS / PROD_RESOURCES),
                "planned_paragraphs": None if continuous else round(planned_resources * PROD_TEXT_BLOCKS / PROD_RESOURCES),
                "continuous": continuous,
                "rate_ramp": (
                    {
                        "start_resources_per_second": args.rate_start,
                        "end_resources_per_second": args.rate_end,
                        "duration_seconds": args.duration_seconds,
                    }
                    if continuous and getattr(args, "rate_start", None) is not None else None
                ),
                "plan": [{**item, "resources": None} for item in plan] if continuous else plan,
            },
            indent=2,
        )
    )
    if args.dry_run and not random_kbs:
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
        if random_kbs:
            plan = await select_random_kbs(driver, args.kb_prefix, random_kbs)
            print(
                json.dumps(
                    {"selected_knowledge_boxes": [
                        {"slug": item["slug"], "kbid": item["kbid"]} for item in plan
                    ]},
                    indent=2,
                ),
                flush=True,
            )
            if args.dry_run:
                return
        if args.ramp:
            await run_ramp(driver, args, plan, start_resource_offset)
            return
        semaphore = asyncio.Semaphore(args.concurrency)
        queue: asyncio.Queue[tuple[str, str, list[tuple[str, int]], int] | None] = asyncio.Queue(
            maxsize=args.concurrency * 2
        )
        previous_stats: Counter = Counter()
        previous_time = time.monotonic()
        interval_latencies: list[float] = []
        rate_ramp: ContinuousRateRamp | None = None
        chunk_limit_reached = asyncio.Event()
        chunk_budget_lock = asyncio.Lock()
        reserved_chunks = 0
        committed_chunks = 0
        max_chunks = getattr(args, "max_chunks", None)
        def report_progress() -> None:
            nonlocal previous_stats, previous_time
            now = time.monotonic()
            seconds = max(now - previous_time, 1e-9)
            completed = stats["created"] + stats["skipped"] + stats["failed"]
            latency_metrics = ramp_report(stats, seconds, interval_latencies)
            p50 = latency_metrics["write_latency_p50_seconds"]
            p95 = latency_metrics["write_latency_p95_seconds"]
            p50_label = "-" if p50 is None else f"{p50:.3f}s"
            p95_label = "-" if p95 is None else f"{p95:.3f}s"
            target_label = (
                f"target={rate_ramp.target_rate():.2f} res/s " if rate_ramp is not None else ""
            )
            print(
                f"Ingest elapsed={now - started:.1f}s window={seconds:.1f}s "
                f"{target_label}"
                f"rate={(stats['created'] - previous_stats['created']) / seconds:.2f} res/s "
                f"chunks={(stats['chunks'] - previous_stats['chunks']) / seconds:.2f}/s "
                f"documents={(stats['documents'] - previous_stats['documents']) / seconds:.2f}/s "
                f"write_p50={p50_label} write_p95={p95_label} "
                f"created={stats['created']} skipped={stats['skipped']} failed={stats['failed']} "
                f"queued={queue.qsize()} in-flight={stats['attempted'] - completed}",
                flush=True,
            )
            previous_stats = stats.copy()
            previous_time = now
            interval_latencies.clear()

        async def reporter() -> None:
            while True:
                await asyncio.sleep(args.report_interval)
                report_progress()

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
                f"failed={worker_stats['failed']} | total={global_processed}/"
                f"{'unbounded' if continuous else resources_to_run} "
                f"rate={global_processed / total_elapsed:.2f} res/s",
                flush=True,
            )
            worker_stats["reported"] = processed

        checkpoint_id = checkpoint_fingerprint(plan, args.seed) if not continuous else ""
        completed_kb_prefix = (
            await load_completed_kb_prefix(driver, checkpoint_id) if not continuous else 0
        )
        if args.auto_resume and not continuous:
            print(
                f"Auto-resume checkpoint: {completed_kb_prefix} completed KBs",
                flush=True,
            )

        async def ingest_worker(worker_id: int):
            nonlocal reserved_chunks, committed_chunks
            worker_started = time.monotonic()
            worker_stats: Counter = Counter()
            last_slug = "-"
            last_kbid = "-"
            first_resource = True
            while True:
                task = await queue.get()
                if task is None:
                    queue.task_done()
                    print_worker_progress(
                        worker_id, last_kbid, last_slug, worker_stats, worker_started, force=True
                    )
                    return
                kbid, slug, vectorsets, resource_index = task
                if chunk_limit_reached.is_set():
                    queue.task_done()
                    continue
                if rate_ramp is not None and not await rate_ramp.acquire():
                    queue.task_done()
                    continue
                stats["attempted"] += 1
                last_slug = slug
                last_kbid = kbid
                async with semaphore:
                    resource_chunk_reservation = 0
                    try:
                        generated = await asyncio.to_thread(
                            generator.make, kbid, resource_index
                        )
                        if continuous:
                            generated["rid"] = uuid.uuid4().hex
                            generated["slug"] = f"synth-{generated['rid']}"
                        if max_chunks is not None:
                            resource_chunk_reservation = generated_chunk_count(generated)
                            async with chunk_budget_lock:
                                if (
                                    chunk_limit_reached.is_set()
                                    or committed_chunks
                                    + reserved_chunks
                                    + resource_chunk_reservation
                                    > max_chunks
                                ):
                                    chunk_limit_reached.set()
                                    admitted = False
                                else:
                                    reserved_chunks += resource_chunk_reservation
                                    admitted = True
                            if not admitted:
                                queue.task_done()
                                continue
                        if first_resource:
                            print(
                                f"Worker {worker_id} starting first resource write "
                                f"kb={slug} resource_index={resource_index}",
                                flush=True,
                            )
                        write_started = time.monotonic()
                        async with asyncio.timeout(args.write_timeout):
                            created, fields, chunks, embeddings = await write_resource(
                                driver,
                                kbid,
                                generated,
                                vectorsets,
                                args.seed,
                                **({"check_existing": False} if continuous else {}),
                            )
                        write_elapsed = time.monotonic() - write_started
                    except Exception as exc:
                        if resource_chunk_reservation:
                            async with chunk_budget_lock:
                                reserved_chunks -= resource_chunk_reservation
                        print(
                            f"Failed direct MarkLogic resource processing kbid={kbid} "
                            f"resource_index={resource_index}: {exc!r}",
                            flush=True,
                        )
                        stats["failed"] += 1
                        worker_stats["failed"] += 1
                        print_worker_progress(worker_id, kbid, slug, worker_stats, worker_started)
                        queue.task_done()
                        continue
                    if resource_chunk_reservation:
                        async with chunk_budget_lock:
                            reserved_chunks -= resource_chunk_reservation
                            if created:
                                committed_chunks += chunks
                                if committed_chunks >= max_chunks:
                                    chunk_limit_reached.set()
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
                        interval_latencies.append(write_elapsed)
                        stats["documents"] += committed_document_count(fields, chunks, embeddings)
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
                queue.task_done()

        workers = [
            asyncio.create_task(ingest_worker(worker_id)) for worker_id in range(1, args.concurrency + 1)
        ]
        progress = asyncio.create_task(reporter())
        try:
            all_active_ranges = [
                resource_range
                for resource_range in kb_resource_ranges(plan, 0)
                if resource_range[1] < resource_range[2]
            ]
            kb_positions = {
                item["slug"]: position
                for position, (item, _, _) in enumerate(all_active_ranges)
            }
            active_ranges = [
                resource_range
                for resource_range in kb_resource_ranges(plan, start_resource_offset)
                if resource_range[1] < resource_range[2]
            ]
            checkpointable = start_resource_offset == 0 and not continuous
            continuous_configs = []
            if continuous:
                for item, _, _ in active_ranges:
                    kbid = await resolve_ingest_kb(driver, item, args)
                    vectorsets = await get_kb_vectorsets(driver, kbid)
                    continuous_configs.append((kbid, item["slug"], vectorsets))
            if max_chunks is not None:
                if len(continuous_configs) != 1:
                    raise ValueError("--max-chunks requires exactly one continuous-mode KB")
                committed_chunks = await count_kb_chunks(driver, continuous_configs[0][0])
                print(
                    f"Chunk limit KB={continuous_configs[0][1]} existing={committed_chunks} "
                    f"target={max_chunks}",
                    flush=True,
                )
                if committed_chunks >= max_chunks:
                    chunk_limit_reached.set()
            targets = cycle(continuous_configs)
            for item, local_start, resource_count in (active_ranges[:1] if continuous else active_ranges):
                slug = item["slug"]
                kb_position = kb_positions[slug]
                if args.auto_resume and kb_position < completed_kb_prefix:
                    print(f"Skipping checkpointed KB: {slug}", flush=True)
                    continue
                kbid = continuous_configs[0][0] if continuous else await resolve_ingest_kb(driver, item, args)
                if args.auto_resume and not continuous:
                    detected_start = await find_resume_offset(
                        driver, kbid, args.seed, resource_count
                    )
                    local_start = max(local_start, detected_start)
                    print(
                        f"KB {slug} start={local_start} (detected={detected_start})",
                        flush=True,
                    )
                failed_before_kb = stats["failed"]
                if continuous:
                    vectorsets = continuous_configs[0][2]
                elif local_start >= resource_count:
                    vectorsets = []
                else:
                    vectorsets = await get_kb_vectorsets(driver, kbid)
                if continuous and getattr(args, "rate_start", None) is not None:
                    rate_ramp = ContinuousRateRamp(
                        args.rate_start, args.rate_end, args.duration_seconds
                    )
                    print(
                        f"Rate ramp KBs={len(continuous_configs)}: {args.rate_start} -> {args.rate_end} res/s "
                        f"over {args.duration_seconds}s; concurrency={args.concurrency}",
                        flush=True,
                    )
                resource_indexes = count(local_start) if continuous else range(local_start, resource_count)
                for resource_index in resource_indexes:
                    if chunk_limit_reached.is_set():
                        break
                    if continuous:
                        kbid, slug, vectorsets = next(targets)
                    if rate_ramp is None:
                        await queue.put((kbid, slug, vectorsets, resource_index))
                    else:
                        if time.monotonic() >= rate_ramp.deadline:
                            break
                        try:
                            async with asyncio.timeout_at(rate_ramp.deadline):
                                await queue.put((kbid, slug, vectorsets, resource_index))
                        except TimeoutError:
                            break
                await queue.join()
                if (
                    checkpointable
                    and kb_position == completed_kb_prefix
                    and stats["failed"] == failed_before_kb
                ):
                    completed_kb_prefix += 1
                    await save_completed_kb_prefix(driver, checkpoint_id, completed_kb_prefix)
                    print(
                        f"Checkpointed completed KB {slug}: "
                        f"{completed_kb_prefix}/{len(active_ranges)}",
                        flush=True,
                    )
                elif stats["failed"] > failed_before_kb:
                    checkpointable = False
            for _ in workers:
                await queue.put(None)
            await asyncio.gather(*workers)
        except BaseException:
            for worker in workers:
                worker.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise
        finally:
            progress.cancel()
            await asyncio.gather(progress, return_exceptions=True)
            report_progress()
        report = {
            "scale": args.scale,
            "seed": args.seed,
            "knowledge_boxes": len(plan),
            "planned_resources": None if continuous else planned_resources,
            "start_resource_number": args.start_resource_number,
            "resources_to_run": None if continuous else resources_to_run,
            "rate_ramp": (
                {
                    "start_resources_per_second": rate_ramp.start_rate,
                    "end_resources_per_second": rate_ramp.end_rate,
                    "duration_seconds": rate_ramp.duration,
                    "load_elapsed_seconds": round(time.monotonic() - rate_ramp.started, 3),
                    "drain_seconds": round(max(0.0, time.monotonic() - rate_ramp.deadline), 3),
                }
                if rate_ramp is not None else None
            ),
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
        help=(
            "Skip checkpointed KBs and use indexed slugs to find the first missing resource; "
            "ramp mode uses per-KB slug search"
        ),
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--sigma", type=float, default=0.4)
    parser.add_argument("--kb-prefix", default="ml-sizing-kb")
    parser.add_argument(
        "--random-kbs", type=int,
        help="Randomly select this many existing KBs whose slugs start with --kb-prefix; requires --continuous",
    )
    parser.add_argument(
        "--kb-slug", help="Write to one KB by slug, creating it if it does not exist"
    )
    parser.add_argument(
        "--continuous", action="store_true", help="Write indefinitely to --kb-slug or --kbid until interrupted; ignores --scale"
    )
    parser.add_argument("--rate-start", type=float, help="Initial target resources/s in continuous mode")
    parser.add_argument("--rate-end", type=float, help="Final target resources/s in continuous mode")
    parser.add_argument(
        "--duration-seconds", type=float,
        help="Ramp duration after KB setup; stop starting resources at the deadline, then drain",
    )
    parser.add_argument(
        "--max-chunks",
        type=int,
        help="Stop continuous single-KB ingestion at this total chunk count",
    )
    parser.add_argument(
        "--kbid",
        help="Ingest the full --scale resource budget into one existing KB using its vectorsets",
    )
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
    if args.random_kbs is not None:
        if args.random_kbs <= 0:
            parser.error("--random-kbs must be positive")
        if not args.continuous:
            parser.error("--random-kbs requires --continuous")
        if args.kbid or args.kb_slug:
            parser.error("--random-kbs cannot be combined with --kbid or --kb-slug")
        if not args.kb_prefix:
            parser.error("--random-kbs requires a nonempty --kb-prefix")
    rate_options = (args.rate_start, args.rate_end, args.duration_seconds)
    if any(value is not None for value in rate_options):
        if not args.continuous or any(value is None for value in rate_options):
            parser.error("--rate-start, --rate-end and --duration-seconds require each other and --continuous")
        if any(not math.isfinite(value) or value <= 0 for value in rate_options):
            parser.error("--rate-start, --rate-end and --duration-seconds must be finite and positive")
        if args.rate_end < args.rate_start:
            parser.error("--rate-end must be at least --rate-start")
    if args.kb_slug and args.kbid:
        parser.error("--kb-slug cannot be combined with --kbid")
    if args.kb_slug and args.delete:
        parser.error("--kb-slug cannot be combined with --delete")
    if args.kb_slug and args.ramp and args.active_kbs != 1:
        parser.error("--kb-slug requires --active-kbs 1 in ramp mode")
    if args.continuous and not (args.kb_slug or args.kbid or args.random_kbs):
        parser.error("--continuous requires --kb-slug, --kbid or --random-kbs")
    if args.continuous and (args.ramp or args.delete):
        parser.error("--continuous cannot be combined with --ramp or --delete")
    if args.max_chunks is not None:
        if args.max_chunks <= 0:
            parser.error("--max-chunks must be positive")
        if not args.continuous or args.random_kbs or not (args.kb_slug or args.kbid):
            parser.error("--max-chunks requires --continuous with --kb-slug or --kbid")
    if args.kbid and args.delete:
        parser.error("--kbid cannot be combined with --delete")
    if args.kbid and args.ramp and args.active_kbs != 1:
        parser.error("--kbid requires --active-kbs 1 in ramp mode")
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
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("Ingestion interrupted", flush=True)


if __name__ == "__main__":
    main()
