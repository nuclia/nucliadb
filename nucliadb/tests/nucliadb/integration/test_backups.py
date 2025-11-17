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
import base64
import uuid
from datetime import datetime

import pytest
from httpx import AsyncClient

from nucliadb.backups.const import StorageKeys
from nucliadb.backups.create import backup_kb_task, get_metadata, set_metadata
from nucliadb.backups.delete import delete_backup_task
from nucliadb.backups.models import (
    BackupMetadata,
    CreateBackupRequest,
    DeleteBackupRequest,
    RestoreBackupRequest,
)
from nucliadb.backups.restore import (
    get_last_restored,
    restore_kb_task,
    set_last_restored,
)
from nucliadb.backups.settings import BackupSettings
from nucliadb.backups.settings import settings as backups_settings
from nucliadb.backups.utils import exists_backup
from nucliadb.common.context import ApplicationContext
from nucliadb.export_import.utils import BM_FIELDS
from nucliadb_protos.writer_pb2 import BrokerMessage

N_RESOURCES = 10


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

    # Create a search configuration
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/search_configurations/myconfig",
        json={"kind": "find", "config": {"features": ["keyword"]}},
    )
    assert resp.status_code == 201

    # Create some synonyms
    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/custom-synonyms",
        json={
            "synonyms": {
                "foo": ["bar", "baz"],
            }
        },
    )
    assert resp.status_code == 204

    # Create some simple resources with a text field
    for i in range(N_RESOURCES):
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
        rid = resp.json()["uuid"]

        # Add some binary files to backup
        content = b"Test for /upload endpoint"
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resource/{rid}/file/file/upload",
            headers={
                "X-Filename": base64.b64encode(b"testfile").decode("utf-8"),
                "Content-Type": "text/plain",
            },
            content=base64.b64encode(content),
        )
        assert resp.status_code == 201

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

    await backup_kb_task(context, CreateBackupRequest(kb_id=src_kb, backup_id=backup_id))

    assert await exists_backup(context.blob_storage, backup_id) is True

    # Make sure that the backup metadata is cleaned up
    assert await get_metadata(context, src_kb, backup_id) is None

    await restore_kb_task(context, RestoreBackupRequest(kb_id=dst_kb, backup_id=backup_id))

    # Make sure that the restore metadata is cleaned up
    assert await get_last_restored(context, dst_kb, backup_id) is None

    # Check that the resources were restored
    await check_kb(nucliadb_reader, src_kb)
    await check_kb(nucliadb_reader, dst_kb)

    # Check that the labelset was restored
    resp = await nucliadb_reader.get(f"/kb/{dst_kb}/labelset/foo")
    assert resp.status_code == 200
    assert len(resp.json()["labels"]) == 1

    # Delete the backup
    await delete_backup_task(context, DeleteBackupRequest(backup_id=backup_id))

    assert await exists_backup(context.blob_storage, backup_id) is False


async def check_kb(nucliadb_reader: AsyncClient, kbid: str):
    await check_resources(nucliadb_reader, kbid)
    await check_synonyms(nucliadb_reader, kbid)
    await check_search_configuration(nucliadb_reader, kbid)
    await check_labelset(nucliadb_reader, kbid)


async def check_synonyms(nucliadb_reader: AsyncClient, kbid: str):
    resp = await nucliadb_reader.get(f"/kb/{kbid}/custom-synonyms")
    assert resp.status_code == 200
    synonyms = resp.json()["synonyms"]
    assert synonyms == {"foo": ["bar", "baz"]}


async def check_search_configuration(nucliadb_reader: AsyncClient, kbid: str):
    resp = await nucliadb_reader.get(f"/kb/{kbid}/search_configurations/myconfig")
    assert resp.status_code == 200
    config = resp.json()
    assert config == {"kind": "find", "config": {"features": ["keyword"]}}


async def check_labelset(nucliadb_reader: AsyncClient, kbid: str):
    resp = await nucliadb_reader.get(f"/kb/{kbid}/labelset/foo")
    assert resp.status_code == 200
    labelset = resp.json()
    assert labelset["title"] == "Foo title"
    assert labelset["color"] == "red"
    assert labelset["multiple"] is True
    assert labelset["kind"] == ["RESOURCES"]
    labels = labelset["labels"]
    assert len(labels) == 1
    label = labels[0]
    assert label["title"] == "Foo title"
    assert label["text"] == "Foo text"


async def check_resources(nucliadb_reader: AsyncClient, kbid: str):
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resources")
    assert resp.status_code == 200
    resources = resp.json()["resources"]
    assert len(resources) == N_RESOURCES
    for resource in resources:
        rid = resource["id"]
        resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid}/file/file")
        assert resp.status_code == 200
        body = resp.json()
        field = body["value"]["file"]
        assert field["content_type"] == "text/plain"
        assert field["filename"] == "testfile"
        assert field["size"] == 36
        assert kbid in field["uri"]

        # Try downloading the file
        resp = await nucliadb_reader.get(field["uri"])
        assert resp.status_code == 200
        assert base64.b64decode(resp.content) == b"Test for /upload endpoint"


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
        kb_id=src_kb, requested_at=datetime.now(), total_resources=len(rids), missing_resources=rids[1:]
    )
    await set_metadata(context, src_kb, backup_id, metadata)

    await backup_kb_task(context, CreateBackupRequest(kb_id=src_kb, backup_id=backup_id))

    await restore_kb_task(context, RestoreBackupRequest(kb_id=dst_kb, backup_id=backup_id))

    # Check that the resources were restored
    resp = await nucliadb_reader.get(f"/kb/{dst_kb}/resources")
    assert resp.status_code == 200
    resources = resp.json()["resources"]
    assert len(resources) == N_RESOURCES - 1
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

    await backup_kb_task(context, CreateBackupRequest(kb_id=src_kb, backup_id=backup_id))

    # Set the last restored resource id as if the restore was interrupted right after restoring the first resource
    last_restored_key = StorageKeys.RESOURCE.format(
        kbid=src_kb, backup_id=backup_id, resource_id=rids[0]
    )
    await set_last_restored(context, dst_kb, backup_id, last_restored_key)

    await restore_kb_task(context, RestoreBackupRequest(kb_id=dst_kb, backup_id=backup_id))

    # Check that the correct resources were restored
    resp = await nucliadb_reader.get(f"/kb/{dst_kb}/resources")
    assert resp.status_code == 200
    resources = resp.json()["resources"]
    assert len(resources) == 9
    assert sorted([r["id"] for r in resources]) == rids[1:]


def test_all_broker_message_fields_are_backed_up():
    """
    Hi developer! If this test fails is because you added a new field in the BrokerMessage proto and it is not
    handled in the backup logic. If the field relates to some new data that needs to be preserved in the backup, please
    make sure to extend the backup logic to handle it. If the field is not relevant for the backup, please add it to the
    ignored fields in the BM_FIELDS dictionary in nucliadb/export_import/utils.py.
    """
    bm = BrokerMessage()
    for field_name in bm.DESCRIPTOR.fields_by_name:
        assert (
            field_name in BM_FIELDS["writer"]
            or field_name in BM_FIELDS["processor"]
            or field_name in BM_FIELDS["ignored"]
            or field_name in BM_FIELDS["common"]
            or field_name in BM_FIELDS["deprecated"]
        ), f"Field {field_name} is not being taken into account in the backup!"
