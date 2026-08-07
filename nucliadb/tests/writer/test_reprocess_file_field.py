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
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.models.internal.processing import ProcessingInfo
from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RESOURCES_PREFIX
from nucliadb.writer.utilities import get_processing
from nucliadb_models.resource import QueueType
from tests.writer.utils import load_file_as_FileB64_payload

TEST_TEXT_PAYLOAD = {"body": "hello", "format": "PLAIN"}
TEST_LINK_PAYLOAD = {
    "added": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "headers": {},
    "cookies": {},
    "uri": "http://some-link.com",
    "language": "en",
    "localstorage": {},
}
TEST_CONVERSATION_PAYLOAD = {
    "messages": [
        {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "who": "Bob",
            "to": ["Alice"],
            "content": {
                "text": "Hi people!",
                "format": "PLAIN",
            },
            "ident": "message_id_001",
        }
    ]
}


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


@pytest.fixture(scope="function")
async def resource_with_reprocessable_fields(
    nucliadb_writer: AsyncClient, knowledgebox: str
) -> AsyncIterator[tuple[str, str, dict[str, str]]]:
    kbid = knowledgebox
    field_ids = {
        "file": "myfile",
        "text": "mytext",
        "link": "mylink",
        "conversation": "myconversation",
    }

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": f"resource-{uuid4().hex}",
            "title": "My resource",
            "files": {
                field_ids["file"]: {
                    "language": "en",
                    "password": "xxxxxx",
                    "file": load_file_as_FileB64_payload("assets/text001.txt", "text/plain"),
                }
            },
            "texts": {
                field_ids["text"]: TEST_TEXT_PAYLOAD,
            },
            "links": {
                field_ids["link"]: TEST_LINK_PAYLOAD,
            },
            "conversations": {
                field_ids["conversation"]: TEST_CONVERSATION_PAYLOAD,
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    assert (await datamanagers.atomic.resources.resource_exists(kbid=kbid, rid=rid)) is True

    yield kbid, rid, field_ids

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


@pytest.mark.parametrize("field_type", ["file", "text", "link", "conversation"])
@pytest.mark.deploy_modes("component")
async def test_reprocess_nonexistent_field_generic_endpoint(
    nucliadb_writer: AsyncClient,
    knowledgebox: str,
    resource: str,
    field_type: str,
):
    kbid = knowledgebox
    rid = resource
    field_id = "nonexistent-field"

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/{field_type}/{field_id}/reprocess",
    )
    assert resp.status_code == 404


@pytest.mark.parametrize("field_type", ["file", "text", "link", "conversation"])
@pytest.mark.deploy_modes("component")
async def test_reprocess_field_generic_endpoint(
    nucliadb_writer: AsyncClient,
    resource_with_reprocessable_fields: tuple[str, str, dict[str, str]],
    processing_mock,
    field_type: str,
):
    kbid, rid, field_ids = resource_with_reprocessable_fields
    field_id = field_ids[field_type]

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/{field_type}/{field_id}/reprocess",
    )
    assert resp.status_code == 202

    assert processing_mock.send_to_process.await_count == 1


@pytest.mark.deploy_modes("component")
async def test_reprocess_non_file_field_with_password_header(
    nucliadb_writer: AsyncClient,
    resource_with_reprocessable_fields: tuple[str, str, dict[str, str]],
):
    kbid, rid, field_ids = resource_with_reprocessable_fields

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/text/{field_ids['text']}/reprocess",
        headers={
            "X-FILE-PASSWORD": "secret-password",
        },
    )
    assert resp.status_code == 422


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
