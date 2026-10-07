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
import asyncio
import unittest
import unittest.mock
import uuid

import pytest
from httpx import AsyncClient

from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import (
    KB_TO_DELETE_BASE,
    KB_TO_DELETE_STORAGE_BASE,
)
from nucliadb.purge import (
    _count_resources_storage_to_purge,
    _purge_resources_storage_batch,
    purge_deleted_resource_storage,
    purge_kbs,
    purge_kbs_storage,
)
from nucliadb_utils.storages.storage import Storage


# TODO(Marklogic): Tweak tests to make sure that purge works well
@pytest.mark.deploy_modes("standalone")
@pytest.skip("TODO: Marklogic")
async def test_purge_deletes_everything_from_maindb(
    maindb_driver: Driver,
    storage: Storage,
    nucliadb_writer_manager: AsyncClient,
    nucliadb_reader_manager: AsyncClient,
    nucliadb_writer: AsyncClient,
):
    """Create a KB and some resource and then purge it. Validate that purge
    removes every key from maindb

    """
    kb_slug = str(uuid.uuid4())
    resp = await nucliadb_writer_manager.post("/kbs", json={"slug": kb_slug})
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")

    resp = await nucliadb_reader_manager.get("/kbs")
    body = resp.json()
    assert len(body["kbs"]) == 1
    assert body["kbs"][0]["uuid"] == kbid

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "My title",
            "slug": "myresource",
            "texts": {"text1": {"body": "My text"}},
        },
    )
    assert resp.status_code == 201

    # Maindb now contain keys for the new kb and resource
    keys_after_create = await list_all_keys(maindb_driver)
    assert len(keys_after_create) > 0

    resp = await nucliadb_writer_manager.delete(f"/kb/{kbid}")
    assert resp.status_code == 200

    resp = await nucliadb_reader_manager.get("/kbs")
    body = resp.json()
    assert len(body["kbs"]) == 0

    keys_after_delete = await list_all_keys(maindb_driver)
    # A marker key has been added to delete the KB asynchronously
    assert any([key.startswith(KB_TO_DELETE_BASE) for key in keys_after_delete])

    await purge_kbs(maindb_driver)
    keys_after_purge_kb = await list_all_keys(maindb_driver)
    # A marker key has been added to delete storage when bucket is empty (that
    # can take a while so it will happen asynchronously too)
    assert any([key.startswith(KB_TO_DELETE_STORAGE_BASE) for key in keys_after_purge_kb])

    with unittest.mock.patch.object(storage, "schedule_delete_kb") as mock_schedule_delete_kb:
        await purge_kbs_storage(maindb_driver, storage)

        # After deletion and purge, no keys should be in maindb
        keys_after_purge_storage = await list_all_keys(maindb_driver)
        if len(keys_after_purge_storage) > 0:
            # The only key left should be the storage deletion marker, and the storage deletion should have been scheduled
            assert len(keys_after_purge_storage) == 1
            assert keys_after_purge_storage[0].startswith(KB_TO_DELETE_STORAGE_BASE)
            assert kbid in keys_after_purge_storage[0]
            assert mock_schedule_delete_kb.call_count == 1


async def list_all_keys(driver: Driver) -> list[str]:
    async with driver.ro_transaction() as txn:
        keys = [key async for key in txn.keys(match="")]
    return keys


@pytest.mark.deploy_modes("standalone")
async def test_purge_resources_deleted_storage(
    maindb_driver: Driver,
    storage: Storage,
    nucliadb_writer_manager: AsyncClient,
    nucliadb_writer: AsyncClient,
):
    # Create a KB
    kb_slug = str(uuid.uuid4())
    resp = await nucliadb_writer_manager.post("/kbs", json={"slug": kb_slug})
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")

    # Create some resources
    resources = []
    for i in range(10):
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json={
                "title": f"Resource {i}",
                "slug": f"resource-{i}",
                "texts": {"text1": {"body": "My text"}},
            },
        )
        assert resp.status_code == 201
        resources.append(resp.json().get("uuid"))

    # Delete the resource
    # Test the case where resources are scheduled to be deleted
    with unittest.mock.patch("nucliadb.ingest.orm.knowledgebox.is_onprem_nucliadb", return_value=False):
        # Delete the resources
        for rid in resources:
            resp = await nucliadb_writer.delete(f"/kb/{kbid}/resource/{rid}")
            assert resp.status_code == 204

    to_purge = await _count_resources_storage_to_purge(maindb_driver)
    assert to_purge == 10
    purged = await _purge_resources_storage_batch(maindb_driver, storage, batch_size=5)
    assert purged == 5
    purged = await _purge_resources_storage_batch(maindb_driver, storage, batch_size=10)
    assert purged == 5

    # Check task cancellation
    task = asyncio.create_task(purge_deleted_resource_storage(maindb_driver, storage))
    await asyncio.sleep(0.1)
    task.cancel()
    await task
