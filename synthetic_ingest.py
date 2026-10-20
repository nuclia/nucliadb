"""
Synthetic ingestion to compare production NucliaDB vs the MarkLogic-backed branch.

Data shape is modeled on a large PARAG production environment:
  - 356 KBs, 2.2M resources, 3.2M fields, 65.5M text blocks (paragraphs)
  - 70% of resources live in ~1.5% of KBs, 12% of KBs are empty

Only text fields are created (synthetic Faker text), one paragraph per text block.

Content generation is deterministic given (--seed, resource index), so running the same
command against both deployments pushes identical data.

Subcommands:
  plan    Print how many resources each KB should get for a given scale.
  ingest  Push synthetic resources into one existing KB using nucliadb_sdk.
  run     Create all KBs from the plan (account API) and fill them.

Target cluster, account and KB settings are hardcoded below; only the Nuclia bearer
token is a parameter (--token or NUCLIA_TOKEN). Tokens are short-lived (~1h).

Examples:
  python synthetic_ingest.py plan --scale 0.1
  python synthetic_ingest.py ingest --token ... --kbid <KBID> --resources 1000
  NUCLIA_TOKEN=... python synthetic_ingest.py run --scale 0.1 --report run.jsonl
"""

import argparse
import asyncio
import datetime
import json
import math
import os
import random
import sys
import time
from collections import Counter
from pathlib import Path

import httpx
from faker import Faker

from nucliadb_models.metadata import InputOrigin
from nucliadb_models.text import TextField, TextFormat
from nucliadb_models.writer import CreateResourcePayload
from nucliadb_sdk import NucliaDBAsync
from nucliadb_sdk.v2.exceptions import ConflictError, RateLimitError

# Dev cluster where the MarkLogic implementation is deployed
NUCLIA_API_URL = "https://gcp-dev-1.dp.gcp-global-dev-1.nuclia.io/api"
ACCOUNT_ID = "57104109-8afe-48b2-81aa-be6204eef0f0"
ZONE = "gcp-dev-1"
SEMANTIC_MODEL = "multilingual-2024-05-06"
# The account API rejects requests that don't look like they come from the admin dashboard
BROWSER_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/156.0.0.0 Safari/537.36"
    ),
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://admin.gcp-global-dev-1.nuclia.io",
    "referer": "https://admin.gcp-global-dev-1.nuclia.io/",
    "sec-ch-ua": '"Not:A-Brand";v="8", "Chromium";v="156", "Google Chrome";v="156"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
}

# Production reference numbers
PROD_KBS = 356
PROD_RESOURCES = 2_200_000
PROD_FIELDS = 3_200_000
PROD_TEXT_BLOCKS = 65_500_000
BIG_KB_RATIO = 0.015
BIG_KB_RESOURCE_SHARE = 0.70
EMPTY_KB_RATIO = 0.12

FIELDS_PER_RESOURCE = PROD_FIELDS / PROD_RESOURCES  # ~1.45
PARAGRAPHS_PER_RESOURCE = PROD_TEXT_BLOCKS / PROD_RESOURCES  # ~29.8
PARAGRAPHS_PER_FIELD = PROD_TEXT_BLOCKS / PROD_FIELDS  # ~20.5

# P(1)=0.65, P(2)=0.25, P(3)=0.10 -> mean 1.45 fields per resource
FIELDS_PER_RESOURCE_DIST = {1: 0.65, 2: 0.25, 3: 0.10}


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


def plan_distribution(scale: float, seed: int, sigma: float, prefix: str = "sizing-kb") -> list[dict]:
    rng = random.Random(seed)
    # Same number of KBs as production; only the load (resources) is scaled
    total_kbs = PROD_KBS
    total_resources = round(PROD_RESOURCES * scale)
    n_empty = round(total_kbs * EMPTY_KB_RATIO)
    n_big = max(1, round(total_kbs * BIG_KB_RATIO))
    n_small = max(0, total_kbs - n_empty - n_big)

    big_share = BIG_KB_RESOURCE_SHARE if n_small > 0 else 1.0
    big_total = round(total_resources * big_share)
    small_total = total_resources - big_total

    def gaussian_split(total: int, n: int) -> list[int]:
        if n == 0:
            return []
        # Weights ~ N(1, sigma), i.e. sizes ~ N(mean, sigma * mean) within the tier
        weights = [max(0.05, rng.gauss(1.0, sigma)) for _ in range(n)]
        s = sum(weights)
        counts = [math.floor(total * w / s) for w in weights]
        counts[weights.index(max(weights))] += total - sum(counts)
        return counts

    plan = []
    for count in gaussian_split(big_total, n_big):
        plan.append({"tier": "big", "resources": count})
    for count in gaussian_split(small_total, n_small):
        plan.append({"tier": "small", "resources": count})
    for _ in range(n_empty):
        plan.append({"tier": "empty", "resources": 0})
    rng.shuffle(plan)
    for i, item in enumerate(plan):
        item["kb"] = f"{prefix}-{i:03d}"
    return plan


def cmd_plan(args):
    plan = plan_distribution(args.scale, args.seed, args.sigma, args.kb_prefix)
    total = sum(p["resources"] for p in plan)
    print(json.dumps(plan, indent=2))
    print(
        f"\nKBs={len(plan)} resources={total} "
        f"~fields={round(total * FIELDS_PER_RESOURCE)} "
        f"~text_blocks={round(total * PARAGRAPHS_PER_RESOURCE)}",
        file=sys.stderr,
    )


# ---------------------------------------------------------------------------
# Resource generation
# ---------------------------------------------------------------------------


class ResourceGenerator:
    def __init__(self, seed: int, paragraphs_per_field: float, paragraph_sentences: int):
        self.seed = seed
        self.paragraph_sentences = paragraph_sentences
        self.fake = Faker()
        # Lognormal with sigma=1 gives a long tail; mu chosen so the mean equals the target
        self.sigma = 1.0
        self.mu = math.log(paragraphs_per_field) - self.sigma**2 / 2

    def _n_paragraphs(self, rng: random.Random, cap: int = 1000) -> int:
        return max(1, min(cap, round(rng.lognormvariate(self.mu, self.sigma))))

    def _paragraph(self, rng: random.Random) -> str:
        n = max(1, self.paragraph_sentences + rng.randint(-2, 2))
        return self.fake.paragraph(nb_sentences=n, variable_nb_sentences=False)

    def generate(self, index: int) -> tuple[CreateResourcePayload, dict]:
        rng = random.Random(f"{self.seed}:{index}")
        self.fake.seed_instance(f"{self.seed}:{index}")
        stats: dict = Counter()

        n_fields = rng.choices(
            list(FIELDS_PER_RESOURCE_DIST), weights=list(FIELDS_PER_RESOURCE_DIST.values())
        )[0]
        texts: dict[str, TextField] = {}
        for k in range(n_fields):
            n = self._n_paragraphs(rng)
            body = "\n\n".join(self._paragraph(rng) for _ in range(n))
            texts[f"text-{k}"] = TextField(body=body, format=TextFormat.PLAIN)
            stats["fields_text"] += 1
            stats["text_paragraphs"] += n
            stats["text_bytes"] += len(body.encode())

        payload = CreateResourcePayload(
            title=self.fake.sentence(nb_words=rng.randint(4, 10)).rstrip("."),
            summary=self.fake.paragraph(nb_sentences=2),
            slug=f"synth-{self.seed}-{index:09d}",
            texts=texts,
            origin=InputOrigin(
                url=self.fake.url(),
                tags=[self.fake.word() for _ in range(rng.randint(0, 4))],
                created=self.fake.date_time_this_decade(tzinfo=datetime.timezone.utc),
            ),
        )
        return payload, stats


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


def auth_headers(args) -> dict[str, str]:
    return {**BROWSER_HEADERS, "Authorization": f"Bearer {args.token}"}


def build_client(args) -> NucliaDBAsync:
    return NucliaDBAsync(url=NUCLIA_API_URL, headers=auth_headers(args), timeout=args.timeout)


async def push_resource(
    ndb: NucliaDBAsync, kbid: str, payload: CreateResourcePayload, args
) -> str:
    for attempt in range(args.retries + 1):
        try:
            await ndb.create_resource(kbid=kbid, content=payload)
            return "created"
        except ConflictError:
            # Slug already exists: resource pushed in a previous run
            return "skipped"
        except RateLimitError as exc:
            await asyncio.sleep(exc.try_after or 2**attempt)
        except (httpx.TransportError, httpx.TimeoutException):
            if attempt == args.retries:
                raise
            await asyncio.sleep(2**attempt)
    raise RuntimeError("Exhausted retries")


async def ingest_kb(
    ndb: NucliaDBAsync | None,
    gen: ResourceGenerator,
    kbid: str,
    n_resources: int,
    start_index: int,
    args,
) -> dict:
    totals: Counter = Counter()
    queue: asyncio.Queue[int | None] = asyncio.Queue(maxsize=args.concurrency * 2)
    started = time.monotonic()

    async def worker():
        while True:
            index = await queue.get()
            if index is None:
                return
            payload, stats = gen.generate(index)
            try:
                result = (
                    "created"
                    if ndb is None
                    else await push_resource(ndb, kbid, payload, args)
                )
            except Exception as exc:
                totals["failed"] += 1
                print(f"[{kbid}][{index}] FAILED: {exc!r}", file=sys.stderr)
                continue
            totals[result] += 1
            if result == "created":
                totals.update(stats)
            done = totals["created"] + totals["skipped"] + totals["failed"]
            if done % args.log_every == 0:
                rate = done / (time.monotonic() - started)
                print(
                    f"[{kbid}] {done}/{n_resources} created={totals['created']} "
                    f"skipped={totals['skipped']} failed={totals['failed']} ({rate:.1f} res/s)"
                )

    workers = [asyncio.create_task(worker()) for _ in range(args.concurrency)]
    for index in range(start_index, start_index + n_resources):
        await queue.put(index)
    for _ in workers:
        await queue.put(None)
    await asyncio.gather(*workers)

    elapsed = time.monotonic() - started
    created = totals["created"] or 1
    return {
        "kbid": kbid,
        "seed": args.seed,
        "start_index": start_index,
        "requested": n_resources,
        "elapsed_s": round(elapsed, 1),
        "resources_per_s": round(totals["created"] / elapsed, 2) if elapsed else None,
        "avg_fields_per_resource": round(totals["fields_text"] / created, 3),
        "totals": dict(totals),
    }


def make_generator(args) -> ResourceGenerator:
    return ResourceGenerator(
        seed=args.seed,
        paragraphs_per_field=args.paragraphs_per_field,
        paragraph_sentences=args.paragraph_sentences,
    )


async def cmd_ingest(args):
    ndb = None if args.dry_run else build_client(args)
    report = await ingest_kb(
        ndb, make_generator(args), args.kbid, args.resources, args.start_index, args
    )
    print(json.dumps(report, indent=2))
    if args.report:
        Path(args.report).write_text(json.dumps(report, indent=2))


# ---------------------------------------------------------------------------
# run: create KBs following the plan and fill them
# ---------------------------------------------------------------------------


class AccountAPI:
    def __init__(self, args):
        self.args = args
        self.kbs_url = f"{NUCLIA_API_URL}/v1/account/{ACCOUNT_ID}/kbs"
        self.client = httpx.AsyncClient(timeout=args.timeout)

    def _headers(self) -> dict[str, str]:
        return {"x-ndb-client": "admin", **auth_headers(self.args)}

    async def list_kbs(self) -> dict[str, str]:
        resp = await self.client.get(self.kbs_url, headers=self._headers())
        resp.raise_for_status()
        return {kb["slug"]: kb["id"] for kb in resp.json()}

    async def create_kb(self, slug: str) -> str | None:
        resp = await self.client.post(
            self.kbs_url,
            headers=self._headers(),
            json={
                "title": slug,
                "description": "",
                "zone": ZONE,
                "enforce_security": True,
                "slug": slug,
                "learning_configuration": {
                    "anonymization_model": "disabled",
                    "semantic_models": [SEMANTIC_MODEL],
                },
            },
        )
        resp.raise_for_status()
        body = resp.json() if resp.content else {}
        return body.get("id") or body.get("uuid") if isinstance(body, dict) else None


async def cmd_run(args):
    plan = plan_distribution(args.scale, args.seed, args.sigma, args.kb_prefix)
    account = AccountAPI(args)
    existing = await account.list_kbs()
    gen = make_generator(args)
    ndb = build_client(args)
    report_file = Path(args.report) if args.report else None

    for i, item in enumerate(plan, 1):
        slug = item["kb"]
        kbid = existing.get(slug)
        if kbid is None:
            kbid = await account.create_kb(slug)
            if kbid is None:
                existing = await account.list_kbs()
                kbid = existing[slug]
            print(f"[{i}/{len(plan)}] created KB {slug} ({kbid})")
        else:
            print(f"[{i}/{len(plan)}] KB {slug} already exists ({kbid})")

        report = await ingest_kb(ndb, gen, kbid, item["resources"], 0, args)
        report.update(slug=slug, tier=item["tier"])
        print(json.dumps(report))
        if report_file:
            with report_file.open("a") as f:
                f.write(json.dumps(report) + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    plan_args = argparse.ArgumentParser(add_help=False)
    plan_args.add_argument("--scale", type=float, default=0.1, help="Fraction of production load (default 0.1)")
    plan_args.add_argument("--seed", type=int, default=42, help="Same seed => same data on both deployments")
    plan_args.add_argument(
        "--sigma",
        type=float,
        default=0.4,
        help="Relative std dev of KB sizes within each tier (gaussian, default 0.4)",
    )
    plan_args.add_argument("--kb-prefix", default="sizing-kb", help="KB slug prefix")

    push_args = argparse.ArgumentParser(add_help=False)
    push_args.add_argument(
        "--token",
        default=os.environ.get("NUCLIA_TOKEN"),
        required="NUCLIA_TOKEN" not in os.environ,
        help="Nuclia bearer token (or NUCLIA_TOKEN env var)",
    )
    push_args.add_argument(
        "--paragraphs-per-field",
        type=float,
        default=PARAGRAPHS_PER_FIELD,
        help=f"Mean paragraphs per text field (default {PARAGRAPHS_PER_FIELD:.1f})",
    )
    push_args.add_argument("--paragraph-sentences", type=int, default=6, help="Mean sentences per paragraph")
    push_args.add_argument("--concurrency", type=int, default=8)
    push_args.add_argument("--retries", type=int, default=5)
    push_args.add_argument("--timeout", type=float, default=120)
    push_args.add_argument("--log-every", type=int, default=100)

    sub.add_parser("plan", parents=[plan_args], help="Per-KB resource distribution for a given scale")

    p = sub.add_parser("ingest", parents=[push_args], help="Push synthetic resources into an existing KB")
    p.add_argument("--kbid", required=True)
    p.add_argument("--resources", type=int, required=True)
    p.add_argument("--start-index", type=int, default=0, help="First resource index (for resuming/sharding)")
    p.add_argument("--seed", type=int, default=42, help="Same seed => same data on both deployments")
    p.add_argument("--report", default=None, help="Write the final JSON report to this path")
    p.add_argument("--dry-run", action="store_true", help="Generate payloads without pushing")

    p = sub.add_parser("run", parents=[plan_args, push_args], help="Create KBs from the plan and fill them")
    p.add_argument("--report", default=None, help="Append one JSON line per KB to this path")

    args = parser.parse_args()
    if args.command == "plan":
        cmd_plan(args)
    elif args.command == "ingest":
        asyncio.run(cmd_ingest(args))
    elif args.command == "run":
        asyncio.run(cmd_run(args))


if __name__ == "__main__":
    main()
