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
from typing import Callable

import os
import pytest
from httpx import AsyncClient
from nucliadb_protos.resources_pb2 import FieldType

from nucliadb_ingest.orm.resource import Resource
from nucliadb_ingest.tests.fixtures import TEST_CLOUDFILE, THUMBNAIL
import nucliadb_ingest.tests.fixtures
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_reader.api.v1.router import KB_PREFIX

BASE = ("field_id", "field_type")
VALUE = ("value",)
EXTRACTED = ("extracted",)


@pytest.mark.asyncio
async def test_resource_download_extracted_file(
    reader_api: Callable[..., AsyncClient], test_resource: Resource
) -> None:
    rsc = test_resource
    kbid = rsc.kb.kbid
    rid = rsc.uuid
    field_type = "text"
    field_id = "text1"
    download_type = "extracted"
    download_field = "thumbnail"

    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/resource/{rid}/{field_type}/{field_id}/download/{download_type}/{download_field}",
        )
        assert resp.status_code == 200
        filename = f"{os.path.dirname(nucliadb_ingest.tests.fixtures.__file__)}{THUMBNAIL.bucket_name}/{THUMBNAIL.uri}"

        open(filename, "rb").read() == resp.content


@pytest.mark.asyncio
async def test_resource_download_field_file(
    reader_api: Callable[..., AsyncClient], test_resource: Resource
) -> None:
    rsc = test_resource
    kbid = rsc.kb.kbid
    rid = rsc.uuid
    field_id = "file1"

    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/resource/{rid}?show=values",
        )
        assert (
            resp.json()["data"]["files"]["file1"]["value"]["file"]["filename"]
            == "text.pb"
        )

        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/resource/{rid}/file/{field_id}/download/field",
        )
        assert resp.status_code == 200
        filename = f"{os.path.dirname(nucliadb_ingest.tests.fixtures.__file__)}/{TEST_CLOUDFILE.bucket_name}/{TEST_CLOUDFILE.uri}"

        open(filename, "rb").read() == resp.content


@pytest.mark.asyncio
async def test_resource_download_field_layout(
    reader_api: Callable[..., AsyncClient], test_resource: Resource
) -> None:
    rsc = test_resource
    kbid = rsc.kb.kbid
    rid = rsc.uuid
    field_id = "layout1"
    download_field = "field1"

    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/resource/{rid}/layout/{field_id}/download/field/{download_field}",
        )
        assert resp.status_code == 200
        filename = f"{os.path.dirname(nucliadb_ingest.tests.fixtures.__file__)}/{TEST_CLOUDFILE.bucket_name}/{TEST_CLOUDFILE.uri}"

        open(filename, "rb").read() == resp.content


@pytest.mark.asyncio
async def test_resource_download_field_conversation(
    reader_api: Callable[..., AsyncClient], test_resource: Resource
) -> None:
    rsc = test_resource
    kbid = rsc.kb.kbid
    rid = rsc.uuid
    field_id = "conv1"

    conversation_field = await test_resource.get_field("conv1", FieldType.CONVERSATION)
    conversations = await conversation_field.get_value(page=1)
    message_with_files = conversations.messages[33]

    msg_id, file_id = message_with_files.content.attachments[1].uri.split("/")[-2:]

    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/resource/{rid}/conversation/{field_id}/download/field/{msg_id}/{file_id}",
        )
        assert resp.status_code == 200
        filename = f"{os.path.dirname(nucliadb_ingest.tests.fixtures.__file__)}/{THUMBNAIL.bucket_name}/{THUMBNAIL.uri}"
        open(filename, "rb").read() == resp.content
