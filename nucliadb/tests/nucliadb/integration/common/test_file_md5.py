# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

import os
import time
import uuid

import pytest

from nucliadb.common import file_md5
from nucliadb.common.maindb.pg import PGDriver
from nucliadb_utils.utilities import Utility
from tests.ndbfixtures.utils import global_utility


@pytest.fixture()
async def driver(pg_maindb_driver: PGDriver):
    with global_utility(Utility.MAINDB_DRIVER, pg_maindb_driver):
        yield pg_maindb_driver


def kbid() -> str:
    return str(uuid.uuid4())


def rid() -> str:
    return str(uuid.uuid4())


async def test_set_and_exists(driver: PGDriver):
    kb = kbid()
    r1 = rid()

    assert await file_md5.exists(kbid=kb, md5="aaa") is False

    async with driver.rw_transaction() as txn:
        await file_md5.set(txn, kbid=kb, md5="aaa", rid=r1, field_id="file1")
        await txn.commit()

    assert await file_md5.exists(kbid=kb, md5="aaa") is True
    assert await file_md5.exists(kbid=kbid(), md5="aaa") is False


async def test_set_is_idempotent(driver: PGDriver):
    kb = kbid()
    r1 = rid()

    async with driver.rw_transaction() as txn:
        await file_md5.set(txn, kbid=kb, md5="aaa", rid=r1, field_id="file1")
        await txn.commit()

    async with driver.rw_transaction() as txn:
        await file_md5.set(txn, kbid=kb, md5="aaa", rid=r1, field_id="file1")
        await txn.commit()

    assert await file_md5.exists(kbid=kb, md5="aaa") is True


async def test_same_md5_different_fields(driver: PGDriver):
    kb = kbid()
    r1 = rid()

    async with driver.rw_transaction() as txn:
        await file_md5.set(txn, kbid=kb, md5="aaa", rid=r1, field_id="file1")
        await file_md5.set(txn, kbid=kb, md5="aaa", rid=r1, field_id="file2")
        await txn.commit()

    assert await file_md5.exists(kbid=kb, md5="aaa") is True

    async with driver.rw_transaction() as txn:
        await file_md5.delete(txn, kbid=kb, rid=r1, field_id="file1")
        await txn.commit()

    assert await file_md5.exists(kbid=kb, md5="aaa") is True


async def test_delete_by_field(driver: PGDriver):
    kb = kbid()
    r1 = rid()

    async with driver.rw_transaction() as txn:
        await file_md5.set(txn, kbid=kb, md5="aaa", rid=r1, field_id="file1")
        await file_md5.set(txn, kbid=kb, md5="bbb", rid=r1, field_id="file2")
        await txn.commit()

    async with driver.rw_transaction() as txn:
        await file_md5.delete(txn, kbid=kb, rid=r1, field_id="file1")
        await txn.commit()

    assert await file_md5.exists(kbid=kb, md5="aaa") is False
    assert await file_md5.exists(kbid=kb, md5="bbb") is True


async def test_delete_by_resource(driver: PGDriver):
    kb = kbid()
    r1 = rid()
    r2 = rid()

    async with driver.rw_transaction() as txn:
        await file_md5.set(txn, kbid=kb, md5="aaa", rid=r1, field_id="file1")
        await file_md5.set(txn, kbid=kb, md5="bbb", rid=r1, field_id="file2")
        await file_md5.set(txn, kbid=kb, md5="ccc", rid=r2, field_id="file1")
        await txn.commit()

    async with driver.rw_transaction() as txn:
        await file_md5.delete(txn, kbid=kb, rid=r1)
        await txn.commit()

    assert await file_md5.exists(kbid=kb, md5="aaa") is False
    assert await file_md5.exists(kbid=kb, md5="bbb") is False
    assert await file_md5.exists(kbid=kb, md5="ccc") is True


async def test_delete_by_kb(driver: PGDriver):
    kb = kbid()
    other_kb = kbid()
    r1 = rid()
    r2 = rid()

    async with driver.rw_transaction() as txn:
        await file_md5.set(txn, kbid=kb, md5="aaa", rid=r1, field_id="file1")
        await file_md5.set(txn, kbid=kb, md5="bbb", rid=r2, field_id="file1")
        await file_md5.set(txn, kbid=other_kb, md5="ccc", rid=r1, field_id="file1")
        await txn.commit()

    async with driver.rw_transaction() as txn:
        await file_md5.delete(txn, kbid=kb)
        await txn.commit()

    assert await file_md5.exists(kbid=kb, md5="aaa") is False
    assert await file_md5.exists(kbid=kb, md5="bbb") is False
    assert await file_md5.exists(kbid=other_kb, md5="ccc") is True


@pytest.mark.skipif(
    not os.environ.get("LOCAL_TEST"),
    reason="Performance tests are skipped by default. Set LOCAL_TEST=1 to enable.",
)
async def test_performance(driver: PGDriver):
    """Insert 2M rows across 300 KBs (5 large KBs hold ~70% of rows), then benchmark operations."""
    total_rows = 2_000_000
    num_large_kbs = 5
    num_small_kbs = 295
    large_kb_rows = int(total_rows * 0.70)
    small_kb_rows = total_rows - large_kb_rows

    rows_per_large_kb = large_kb_rows // num_large_kbs
    rows_per_small_kb = small_kb_rows // num_small_kbs

    large_kbs = [kbid() for _ in range(num_large_kbs)]
    small_kbs = [kbid() for _ in range(num_small_kbs)]

    batch_size = 10_000

    # Bulk insert rows
    pg_driver: PGDriver = driver
    async with pg_driver._get_connection() as conn:
        async with conn.cursor() as cur:
            row_idx = 0
            for kb, count in [(kb, rows_per_large_kb) for kb in large_kbs] + [
                (kb, rows_per_small_kb) for kb in small_kbs
            ]:
                for batch_start in range(0, count, batch_size):
                    batch_end = min(batch_start + batch_size, count)
                    values = ",".join(
                        f"('{kb}', 'md5_{row_idx + i}', '{uuid.uuid4()}', 'f/field_{i % 10}')"
                        for i in range(batch_end - batch_start)
                    )
                    await cur.execute(f"INSERT INTO file_md5 (kbid, md5, rid, field_id) VALUES {values}")
                    row_idx += batch_end - batch_start

    target_kb = large_kbs[0]

    # Benchmark exists (hit)
    start = time.monotonic()
    result = await file_md5.exists(kbid=target_kb, md5="md5_0")
    exists_hit_ms = (time.monotonic() - start) * 1000
    assert result is True

    # Benchmark exists (miss)
    start = time.monotonic()
    result = await file_md5.exists(kbid=target_kb, md5="nonexistent_md5")
    exists_miss_ms = (time.monotonic() - start) * 1000
    assert result is False

    # Benchmark exists on a small KB
    small_target = small_kbs[0]
    start = time.monotonic()
    result = await file_md5.exists(kbid=small_target, md5="nonexistent_md5")
    exists_small_kb_ms = (time.monotonic() - start) * 1000
    assert result is False

    # Benchmark set (insert new)
    new_rid = rid()
    async with driver.rw_transaction() as txn:
        start = time.monotonic()
        await file_md5.set(txn, kbid=target_kb, md5="new_md5", rid=new_rid, field_id="f/new")
        set_new_ms = (time.monotonic() - start) * 1000
        await txn.commit()

    # Benchmark set (upsert existing)
    async with driver.rw_transaction() as txn:
        start = time.monotonic()
        await file_md5.set(txn, kbid=target_kb, md5="new_md5", rid=new_rid, field_id="f/new")
        set_upsert_ms = (time.monotonic() - start) * 1000
        await txn.commit()

    # Benchmark delete by field
    async with driver.rw_transaction() as txn:
        start = time.monotonic()
        await file_md5.delete(txn, kbid=target_kb, rid=new_rid, field_id="f/new")
        delete_field_ms = (time.monotonic() - start) * 1000
        await txn.commit()

    # Insert a resource with multiple fields for resource-level delete benchmark
    resource_rid = rid()
    num_fields = 50
    async with driver.rw_transaction() as txn:
        for i in range(num_fields):
            await file_md5.set(
                txn, kbid=target_kb, md5=f"res_md5_{i}", rid=resource_rid, field_id=f"field_{i}"
            )
        await txn.commit()

    # Benchmark delete by resource
    async with driver.rw_transaction() as txn:
        start = time.monotonic()
        await file_md5.delete(txn, kbid=target_kb, rid=resource_rid)
        delete_resource_ms = (time.monotonic() - start) * 1000
        await txn.commit()

    assert await file_md5.exists(kbid=target_kb, md5="res_md5_0") is False

    # Benchmark delete by KB (large KB, ~280k rows)
    async with driver.rw_transaction() as txn:
        start = time.monotonic()
        await file_md5.delete(txn, kbid=target_kb)
        delete_large_kb_ms = (time.monotonic() - start) * 1000
        await txn.commit()

    assert await file_md5.exists(kbid=target_kb, md5="md5_0") is False

    # Benchmark delete by KB (small KB)
    async with driver.rw_transaction() as txn:
        start = time.monotonic()
        await file_md5.delete(txn, kbid=small_target)
        delete_small_kb_ms = (time.monotonic() - start) * 1000
        await txn.commit()

    print(
        f"\n--- file_md5 performance with {total_rows:,} rows across 300 KBs ---\n"
        f"  exists (hit, large KB):     {exists_hit_ms:>8.2f} ms\n"
        f"  exists (miss, large KB):    {exists_miss_ms:>8.2f} ms\n"
        f"  exists (miss, small KB):    {exists_small_kb_ms:>8.2f} ms\n"
        f"  set (new):                  {set_new_ms:>8.2f} ms\n"
        f"  set (upsert):               {set_upsert_ms:>8.2f} ms\n"
        f"  delete (field):             {delete_field_ms:>8.2f} ms\n"
        f"  delete (resource, {num_fields} fields): {delete_resource_ms:>8.2f} ms\n"
        f"  delete (large KB, ~{rows_per_large_kb:,}):  {delete_large_kb_ms:>8.2f} ms\n"
        f"  delete (small KB, ~{rows_per_small_kb:,}):   {delete_small_kb_ms:>8.2f} ms\n"
    )

    # Sanity thresholds — single-row operations should be well under 100ms
    assert exists_hit_ms < 100, f"exists (hit) too slow: {exists_hit_ms:.2f}ms"
    assert exists_miss_ms < 100, f"exists (miss) too slow: {exists_miss_ms:.2f}ms"
    assert exists_small_kb_ms < 100, f"exists (small KB) too slow: {exists_small_kb_ms:.2f}ms"
    assert set_new_ms < 100, f"set (new) too slow: {set_new_ms:.2f}ms"
    assert set_upsert_ms < 100, f"set (upsert) too slow: {set_upsert_ms:.2f}ms"
    assert delete_field_ms < 100, f"delete (field) too slow: {delete_field_ms:.2f}ms"
    assert delete_resource_ms < 100, f"delete (resource) too slow: {delete_resource_ms:.2f}ms"
