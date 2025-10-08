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
from typing import AsyncIterator
from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.models.internal.processing import ProcessingInfo
from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RESOURCES_PREFIX
from nucliadb.writer.utilities import get_processing
from nucliadb_models.resource import QueueType
from tests.writer.utils import load_file_as_FileB64_payload


@pytest.fixture(scope="function")
def processing_mock(mocker):
    processing = get_processing()
    mocker.patch.object(
        processing,
        "send_to_process",
        AsyncMock(return_value=ProcessingInfo(seqid=0, account_seq=0, queue=QueueType.SHARED)),
    )
    yield processing


@pytest.fixture(scope="function")
async def file_field(
    nucliadb_writer: AsyncClient, knowledgebox: str
) -> AsyncIterator[tuple[str, str, str]]:
    kbid = knowledgebox
    field_id = "myfile"

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": "resource",
            "title": "My resource",
            "files": {
                field_id: {
                    "language": "en",
                    "password": "xxxxxx",
                    "file": load_file_as_FileB64_payload("assets/text001.txt", "text/plain"),
                }
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    assert (await datamanagers.atomic.resources.resource_exists(kbid=kbid, rid=rid)) is True

    yield kbid, rid, field_id

    resp = await nucliadb_writer.delete(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}",
    )
    assert resp.status_code == 204


@pytest.mark.deploy_modes("component")
async def test_reprocess_nonexistent_file_field(
    nucliadb_writer: AsyncClient, knowledgebox: str, resource: str
):
    kbid = knowledgebox
    rid = resource
    field_id = "nonexistent-field"

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/file/{field_id}/reprocess",
    )
    assert resp.status_code == 404


@pytest.mark.deploy_modes("component")
async def test_reprocess_file_field_with_password(
    nucliadb_writer: AsyncClient, file_field: tuple[str, str, str], processing_mock
):
    kbid, rid, field_id = file_field
    password = "secret-password"

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/file/{field_id}/reprocess",
        headers={
            "X-FILE-PASSWORD": password,
        },
    )
    assert resp.status_code == 202

    assert processing_mock.send_to_process.await_count == 1


@pytest.mark.deploy_modes("component")
async def test_reprocess_file_field_without_password(
    nucliadb_writer: AsyncClient, file_field: tuple[str, str, str], processing_mock
):
    kbid, rid, field_id = file_field

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/file/{field_id}/reprocess",
    )
    assert resp.status_code == 202

    assert processing_mock.send_to_process.await_count == 1
