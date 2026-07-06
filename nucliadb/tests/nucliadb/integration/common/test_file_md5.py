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


import pytest

from nucliadb.common import file_md5
from nucliadb.common.datamanagers import fields_v2, kb_v2, resources_v2
from nucliadb.common.maindb.pg import PGDriver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.resource import Resource
from nucliadb_utils.utilities import Utility
from tests.ndbfixtures.utils import global_utility


@pytest.fixture()
async def driver(pg_maindb_driver: PGDriver):
    with global_utility(Utility.MAINDB_DRIVER, pg_maindb_driver):
        yield pg_maindb_driver


@pytest.fixture(scope="function")
async def kbid(driver: PGDriver) -> str:
    kb_id = KnowledgeBox.new_unique_kbid()
    async with driver.rw_transaction() as txn:
        await kb_v2.set_kbid_for_slug(txn, kbid=kb_id, slug=f"slug-{kb_id}")
        await txn.commit()
    return kb_id


@pytest.fixture(scope="function")
async def rid(driver: PGDriver, kbid: str) -> str:
    r_id = Resource.new_unique_rid()
    async with driver.rw_transaction() as txn:
        await resources_v2.set_slug(txn, kbid=kbid, rid=r_id, slug=f"slug-{r_id}")
        await txn.commit()
    return r_id


async def test_set_and_exists(driver: PGDriver, kbid: str, rid: str):
    assert await file_md5.exists(kbid=kbid, md5="aaa") is False

    async with driver.rw_transaction() as txn:
        await file_md5.set(txn, kbid=kbid, md5="aaa", rid=rid, field_id="file1")
        await txn.commit()

    assert await file_md5.exists(kbid=kbid, md5="aaa") is True

    non_existing_kbid = KnowledgeBox.new_unique_kbid()
    assert await file_md5.exists(kbid=non_existing_kbid, md5="aaa") is False


async def test_set_is_idempotent(driver: PGDriver, kbid: str, rid: str):
    async with driver.rw_transaction() as txn:
        await file_md5.set(txn, kbid=kbid, md5="aaa", rid=rid, field_id="file1")
        await txn.commit()

    async with driver.rw_transaction() as txn:
        await file_md5.set(txn, kbid=kbid, md5="aaa", rid=rid, field_id="file1")
        await txn.commit()

    assert await file_md5.exists(kbid=kbid, md5="aaa") is True


async def test_same_md5_different_fields(driver: PGDriver, kbid: str, rid: str):
    async with driver.rw_transaction() as txn:
        await file_md5.set(txn, kbid=kbid, md5="aaa", rid=rid, field_id="file1")
        await file_md5.set(txn, kbid=kbid, md5="aaa", rid=rid, field_id="file2")
        await txn.commit()

    assert await file_md5.exists(kbid=kbid, md5="aaa") is True

    async with driver.rw_transaction() as txn:
        await file_md5.delete(txn, kbid=kbid, rid=rid, field_id="file1")
        await txn.commit()

    assert await file_md5.exists(kbid=kbid, md5="aaa") is True


async def test_delete_by_field(driver: PGDriver, kbid: str, rid: str):
    async with driver.rw_transaction() as txn:
        await file_md5.set(txn, kbid=kbid, md5="aaa", rid=rid, field_id="file1")
        await file_md5.set(txn, kbid=kbid, md5="bbb", rid=rid, field_id="file2")
        await txn.commit()

    async with driver.rw_transaction() as txn:
        await file_md5.delete(txn, kbid=kbid, rid=rid, field_id="file1")
        await fields_v2.delete(txn, kbid=kbid, rid=rid, field_id="file1", field_type="f")
        await txn.commit()

    assert await file_md5.exists(kbid=kbid, md5="aaa") is False
    assert await file_md5.exists(kbid=kbid, md5="bbb") is True


async def test_delete_by_resource(driver: PGDriver, kbid: str, rid: str):
    r2 = Resource.new_unique_rid()
    async with driver.rw_transaction() as txn:
        await resources_v2.set_slug(txn, kbid=kbid, rid=r2, slug=f"slug-{r2}")
        await txn.commit()

    async with driver.rw_transaction() as txn:
        await file_md5.set(txn, kbid=kbid, md5="aaa", rid=rid, field_id="file1")
        await file_md5.set(txn, kbid=kbid, md5="bbb", rid=rid, field_id="file2")
        await file_md5.set(txn, kbid=kbid, md5="ccc", rid=r2, field_id="file1")
        await txn.commit()

    async with driver.rw_transaction() as txn:
        await file_md5.delete(txn, kbid=kbid, rid=rid)
        await resources_v2.delete(txn, kbid=kbid, rid=rid)
        await txn.commit()

    assert await file_md5.exists(kbid=kbid, md5="aaa") is False
    assert await file_md5.exists(kbid=kbid, md5="bbb") is False
    assert await file_md5.exists(kbid=kbid, md5="ccc") is True


async def test_delete_by_kb(driver: PGDriver, kbid: str, rid: str):
    other_kb = KnowledgeBox.new_unique_kbid()
    r2 = Resource.new_unique_rid()
    async with driver.rw_transaction() as txn:
        await kb_v2.set_kbid_for_slug(txn, kbid=other_kb, slug=f"slug-{other_kb}")
        await resources_v2.set_slug(txn, kbid=kbid, rid=r2, slug=f"slug-{r2}")
        await resources_v2.set_slug(txn, kbid=other_kb, rid=rid, slug=f"slug-{rid}")
        await txn.commit()

    async with driver.rw_transaction() as txn:
        await file_md5.set(txn, kbid=kbid, md5="aaa", rid=rid, field_id="file1")
        await file_md5.set(txn, kbid=kbid, md5="bbb", rid=r2, field_id="file1")
        await file_md5.set(txn, kbid=other_kb, md5="ccc", rid=rid, field_id="file1")
        await txn.commit()

    async with driver.rw_transaction() as txn:
        await file_md5.delete(txn, kbid=kbid)
        await kb_v2.delete(txn, kbid=kbid)
        await txn.commit()

    assert await file_md5.exists(kbid=kbid, md5="aaa") is False
    assert await file_md5.exists(kbid=kbid, md5="bbb") is False
    assert await file_md5.exists(kbid=other_kb, md5="ccc") is True
