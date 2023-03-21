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
from typing import AsyncIterator, Tuple
from unittest.mock import AsyncMock

import pytest
from nucliadb_protos.writer_pb2 import ResourceFieldId

from nucliadb.ingest.processing import ProcessingInfo
from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RESOURCES_PREFIX
from nucliadb.writer.tests.utils import load_file_as_FileB64_payload
from nucliadb.writer.utilities import get_processing
from nucliadb_models.resource import NucliaDBRoles, QueueType
from nucliadb_utils.utilities import get_ingest


@pytest.fixture(scope="function")
def processing_mock(mocker):
    processing = get_processing()
    mocker.patch.object(
        processing,
        "send_to_process",
        AsyncMock(
            return_value=ProcessingInfo(seqid=0, account_seq=0, queue=QueueType.SHARED)
        ),
    )
    yield processing


@pytest.fixture(scope="function")
@pytest.mark.asyncio
async def file_field(
    writer_api, knowledgebox_writer: str
) -> AsyncIterator[Tuple[str, str, str]]:
    kbid = knowledgebox_writer
    field_id = "myfile"

    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
            headers={"X-SYNCHRONOUS": "True"},
            json={
                "slug": "resource",
                "title": "My resource",
                "files": {
                    field_id: {
                        "language": "en",
                        "password": "xxxxxx",
                        "file": load_file_as_FileB64_payload(
                            "assets/text001.txt", "text/plain"
                        ),
                    }
                },
            },
        )
        assert resp.status_code == 201
        rid = resp.json()["uuid"]

        ingest = get_ingest()
        pbrequest = ResourceFieldId()
        pbrequest.kbid = kbid
        pbrequest.rid = rid

        res = await ingest.ResourceFieldExists(pbrequest)  # type: ignore
        assert res.found

    yield kbid, rid, field_id

    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.delete(
            f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}",
        )
        assert resp.status_code == 204


@pytest.mark.asyncio
async def test_reprocess_nonexistent_file_field(
    writer_api, knowledgebox_writer: str, resource: str
):
    kbid = knowledgebox_writer
    rid = resource
    field_id = "nonexistent-field"

    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/file/{field_id}/reprocess",
        )
        assert resp.status_code == 404


@pytest.mark.asyncio
async def test_reprocess_file_field_with_password(
    writer_api, file_field: Tuple[str, str, str], processing_mock
):
    kbid, rid, field_id = file_field
    password = "secret-password"

    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/file/{field_id}/reprocess",
            headers={
                "X-FILE-PASSWORD": password,
            },
        )
        assert resp.status_code == 202

    assert processing_mock.send_to_process.await_count == 1
    assert processing_mock.uploads[-1]["X-PASSWORD"] == password


@pytest.mark.asyncio
async def test_reprocess_file_field_without_password(
    writer_api, file_field: Tuple[str, str, str], processing_mock
):
    kbid, rid, field_id = file_field

    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/file/{field_id}/reprocess",
        )
        assert resp.status_code == 202

    assert processing_mock.send_to_process.await_count == 1
    assert processing_mock.uploads[-1]["X-PASSWORD"] == ""
