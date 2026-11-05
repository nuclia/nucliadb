"""Enqueue or wait for MarkLogic reindexes of KB databases using MARKLOGIC_SERVER_* settings.

Enqueue all KBs:   python reindex_marklogic_databases.py enqueue
Enqueue one KB:    python reindex_marklogic_databases.py enqueue --kbid <kbid>
Wait for all KBs:  python reindex_marklogic_databases.py wait
Wait for one KB:   python reindex_marklogic_databases.py wait --kbid <kbid> --interval 30

Without --kbid, every database starting with the KB database prefix is targeted.
"""

import argparse
import asyncio
import sys
import time

from nucliadb.common.marklogic.admin_client import AdminClient
from nucliadb.common.marklogic.settings import settings

KB_DATABASE_PREFIX = "nucliadb-kb-"


def admin_client() -> AdminClient:
    if not settings.username or not settings.password:
        raise ValueError("Set MARKLOGIC_SERVER_USERNAME and MARKLOGIC_SERVER_PASSWORD")
    url = f"{settings.uri.rstrip('/')}:{settings.admin_port}"
    return AdminClient(url, settings.username, settings.password.get_secret_value())


async def target_databases(client: AdminClient, kbids: list[str] | None) -> list[str]:
    existing = set(await client.list_databases())
    if not kbids:
        return sorted(name for name in existing if name.startswith(KB_DATABASE_PREFIX))
    databases = [f"{KB_DATABASE_PREFIX}{kbid}" for kbid in kbids]
    missing = [name for name in databases if name not in existing]
    if missing:
        raise ValueError(f"Databases not found: {', '.join(missing)}")
    return databases


async def enqueue(kbids: list[str] | None) -> int:
    failed = 0
    async with admin_client() as client:
        databases = await target_databases(client, kbids)
        print(f"Enqueuing reindex on {len(databases)} databases")
        for database in databases:
            try:
                await client.reindex_database(database)
            except Exception as exc:
                failed += 1
                print(f"Failed to reindex {database}: {exc}", file=sys.stderr)
            else:
                print(f"Reindex enqueued on {database}")
    print(f"Enqueued={len(databases) - failed} failed={failed}")
    return 1 if failed else 0


async def wait(kbids: list[str] | None, interval: float, timeout: float | None) -> int:
    start = time.monotonic()
    async with admin_client() as client:
        pending = await target_databases(client, kbids)
        total = len(pending)
        print(f"Waiting for reindex on {total} databases")
        while pending:
            still_pending = []
            for database in pending:
                try:
                    reindexing = await client.get_database_reindex_count(database)
                except Exception as exc:
                    print(f"Failed to read status of {database}: {exc}", file=sys.stderr)
                    still_pending.append(database)
                    continue
                if reindexing:
                    still_pending.append(database)
                else:
                    print(f"Reindex finished on {database}")
            pending = still_pending
            elapsed = time.monotonic() - start
            print(f"[{elapsed:.0f}s] finished={total - len(pending)}/{total}")
            if not pending:
                break
            if timeout is not None and elapsed >= timeout:
                print(f"Timed out; still reindexing: {', '.join(pending)}", file=sys.stderr)
                return 1
            await asyncio.sleep(interval)
    print("All reindexes finished")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("mode", choices=["enqueue", "wait"])
    parser.add_argument(
        "--kbid", action="append", help="KB to target (repeatable). Defaults to all KB databases"
    )
    parser.add_argument("--interval", type=float, default=10.0, help="Wait poll interval (s)")
    parser.add_argument("--timeout", type=float, default=None, help="Max wait time (s)")
    args = parser.parse_args()
    if args.mode == "enqueue":
        raise SystemExit(asyncio.run(enqueue(args.kbid)))
    raise SystemExit(asyncio.run(wait(args.kbid, args.interval, args.timeout)))


if __name__ == "__main__":
    main()
