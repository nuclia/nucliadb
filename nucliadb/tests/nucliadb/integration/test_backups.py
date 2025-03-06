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
import uuid
from datetime import datetime

import pytest
from httpx import AsyncClient

from nucliadb.backups.const import StorageKeys
from nucliadb.backups.create import backup_kb, get_metadata, set_metadata
from nucliadb.backups.models import BackupMetadata
from nucliadb.backups.restore import get_last_restored_resource_key, restore_kb, set_last_restored_resource_key
from nucliadb.backups.settings import BackupSettings
from nucliadb.backups.settings import settings as backups_settings
from nucliadb.backups.utils import exists_backup
from nucliadb.common.context import ApplicationContext


async def create_kb(
    nucliadb_writer_manager: AsyncClient,
):
    slug = uuid.uuid4().hex
    resp = await nucliadb_writer_manager.post("/kbs", json={"slug": slug})
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")
    return kbid


@pytest.fixture(scope="function")
async def src_kb(
    nucliadb_writer: AsyncClient,
    nucliadb_writer_manager: AsyncClient,
):
    kbid = await create_kb(nucliadb_writer_manager)

    # Create 10 simple resources with a text field
    for i in range(10):
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json={
                "title": "Test",
                "thumbnail": "foobar",
                "icon": "application/pdf",
                "slug": f"test-{i}",
                "texts": {
                    "text": {
                        "body": f"This is a test {i}",
                    }
                },
            },
        )
        assert resp.status_code == 201

    # Create an entity group with a few entities
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/entitiesgroups",
        json={
            "group": "foo",
            "entities": {
                "bar": {"value": "BAZ", "represents": ["lorem", "ipsum"]},
            },
            "title": "Foo title",
            "color": "red",
        },
    )
    assert resp.status_code == 200

    # Create a labelset with a few labels
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/labelset/foo",
        json={
            "title": "Foo title",
            "color": "red",
            "multiple": True,
            "kind": ["RESOURCES"],
            "labels": [{"title": "Foo title", "text": "Foo text"}],
        },
    )
    assert resp.status_code == 200
    yield kbid


@pytest.fixture(scope="function")
async def dst_kb(
    nucliadb_writer_manager: AsyncClient,
):
    kbid = await create_kb(nucliadb_writer_manager)
    yield kbid


@pytest.fixture(scope="function")
def settings():
    # We lower the concurrency in tests to avoid exhausting psql connections
    backups_settings.backup_resources_concurrency = 2
    backups_settings.restore_resources_concurrency = 2
    yield backups_settings


@pytest.fixture(scope="function")
async def context(nucliadb_reader: AsyncClient, settings: BackupSettings):
    context = ApplicationContext()
    await context.initialize()
    await context.blob_storage.create_bucket(backups_settings.backups_bucket)
    yield context
    await context.finalize()


@pytest.mark.deploy_modes("standalone")
async def test_backup(
    nucliadb_reader: AsyncClient,
    src_kb: str,
    dst_kb: str,
    settings: BackupSettings,
    context: ApplicationContext,
):
    backup_id = str(uuid.uuid4())

    assert await exists_backup(context.blob_storage, backup_id) is False

    await backup_kb(context, src_kb, backup_id)

    assert await exists_backup(context.blob_storage, backup_id) is True

    # Make sure that the backup metadata is cleaned up
    assert await get_metadata(context, src_kb, backup_id) is None

    await restore_kb(context, dst_kb, backup_id)

    # Make sure that the restore metadata is cleaned up
    assert await get_last_restored_resource_key(context, dst_kb, backup_id) is None

    # Check that the resources were restored
    resp = await nucliadb_reader.get(f"/kb/{dst_kb}/resources")
    assert resp.status_code == 200
    resources = resp.json()["resources"]
    assert len(resources) == 10

    # Check that the entities were restored
    resp = await nucliadb_reader.get(f"/kb/{dst_kb}/entitiesgroups")
    assert resp.status_code == 200
    assert len(resp.json()["groups"]) == 1

    # Check that the labelset was restored
    resp = await nucliadb_reader.get(f"/kb/{dst_kb}/labelset/foo")
    assert resp.status_code == 200
    assert len(resp.json()["labels"]) == 1


@pytest.mark.deploy_modes("standalone")
async def test_backup_resumed(
    nucliadb_reader: AsyncClient,
    src_kb: str,
    dst_kb: str,
    settings: BackupSettings,
    context: ApplicationContext,
):
    backup_id = str(uuid.uuid4())

    # Read all rids
    resp = await nucliadb_reader.get(f"/kb/{src_kb}/resources")
    assert resp.status_code == 200
    rids = sorted([r["id"] for r in resp.json()["resources"]])

    # Set the metadata as if the backup was interrupted right after exporting the first resource
    metadata = BackupMetadata(
        kbid=src_kb, requested_at=datetime.now(), total_resources=len(rids), missing_resources=rids[1:]
    )
    await set_metadata(context, src_kb, backup_id, metadata)

    await backup_kb(context, src_kb, backup_id)

    await restore_kb(context, dst_kb, backup_id)

    # Check that the resources were restored
    resp = await nucliadb_reader.get(f"/kb/{dst_kb}/resources")
    assert resp.status_code == 200
    resources = resp.json()["resources"]
    assert len(resources) == 9
    assert sorted([r["id"] for r in resources]) == rids[1:]


@pytest.mark.deploy_modes("standalone")
async def test_restore_resumed(
    nucliadb_reader: AsyncClient,
    src_kb: str,
    dst_kb: str,
    settings: BackupSettings,
    context: ApplicationContext,
):
    backup_id = str(uuid.uuid4())

    # Read all rids
    resp = await nucliadb_reader.get(f"/kb/{src_kb}/resources")
    assert resp.status_code == 200
    rids = sorted([r["id"] for r in resp.json()["resources"]])

    await backup_kb(context, src_kb, backup_id)

    # Set the last restored resource id as if the restore was interrupted right after restoring the first resource
    last_restored_key = StorageKeys.RESOURCE.format(
        kbid=src_kb, backup_id=backup_id, resource_id=rids[0]
    )
    await set_last_restored_resource_key(context, dst_kb, backup_id, last_restored_key)

    await restore_kb(context, dst_kb, backup_id)

    # Check that the correct resources were restored
    resp = await nucliadb_reader.get(f"/kb/{dst_kb}/resources")
    assert resp.status_code == 200
    resources = resp.json()["resources"]
    assert len(resources) == 9
    assert sorted([r["id"] for r in resources]) == rids[1:]
