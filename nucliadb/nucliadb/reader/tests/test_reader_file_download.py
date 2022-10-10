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
from typing import Callable

import pytest
from httpx import AsyncClient
from nucliadb_protos.resources_pb2 import FieldType

import nucliadb.ingest.tests.fixtures
from nucliadb.ingest.orm.resource import Resource
from nucliadb.ingest.tests.fixtures import TEST_CLOUDFILE, THUMBNAIL
from nucliadb.models.resource import NucliaDBRoles
from nucliadb.reader.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RSLUG_PREFIX

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
            f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/{field_type}/{field_id}/download/{download_type}/{download_field}",  # noqa
        )
        assert resp.status_code == 200
        filename = f"{os.path.dirname(nucliadb.ingest.tests.fixtures.__file__)}{THUMBNAIL.bucket_name}/{THUMBNAIL.uri}"

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
            f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}?show=values",
        )
        assert (
            resp.json()["data"]["files"]["file1"]["value"]["file"]["filename"]
            == "text.pb"
        )

        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/file/{field_id}/download/field",
        )
        assert resp.status_code == 200
        assert resp.headers["Content-Disposition"]

        filename = f"{os.path.dirname(nucliadb.ingest.tests.fixtures.__file__)}/{TEST_CLOUDFILE.bucket_name}/{TEST_CLOUDFILE.uri}"  # noqa

        open(filename, "rb").read() == resp.content

        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}?show=values",
        )
        assert resp.status_code == 200

        assert (
            resp.json()["data"]["texts"]["text1"]["value"]["md5"]
            == "74a3187271f1d526b1f6271bfb7df52e"
        )
        assert (
            resp.json()["data"]["files"]["file1"]["value"]["file"]["md5"]
            == "01cca3f53edb934a445a3112c6caa652"
        )


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
            f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/layout/{field_id}/download/field/{download_field}",
        )
        assert resp.status_code == 200
        filename = f"{os.path.dirname(nucliadb.ingest.tests.fixtures.__file__)}/{TEST_CLOUDFILE.bucket_name}/{TEST_CLOUDFILE.uri}"  # noqa

        open(filename, "rb").read() == resp.content


@pytest.mark.asyncio
async def test_resource_download_field_conversation(
    reader_api: Callable[..., AsyncClient], test_resource: Resource
) -> None:
    rsc = test_resource
    kbid = rsc.kb.kbid
    rid = rsc.uuid
    field_id = "conv1"

    msg_id, file_id = await _get_message_with_file(test_resource)

    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/conversation/{field_id}/download/field/{msg_id}/{file_id}",
        )
        assert resp.status_code == 200
        filename = f"{os.path.dirname(nucliadb.ingest.tests.fixtures.__file__)}/{THUMBNAIL.bucket_name}/{THUMBNAIL.uri}"  # noqa
        assert open(filename, "rb").read() == resp.content


@pytest.mark.parametrize(
    "endpoint_part,endpoint_params",
    [
        [
            "{field_type}/{field_id}/download/extracted/{download_field}",
            {"field_type": "text", "field_id": "text1", "download_field": "thumbnail"},
        ],  # noqa
        ["file/{field_id}/download/field", {"field_id": "file1"}],
        [
            "layout/{field_id}/download/field/{download_field}",
            {"field_id": "layout1", "download_field": "field1"},
        ],
        [
            "conversation/{field_id}/download/field/{message_id}/{file_num}",
            {"field_id": "conv1"},
        ],
    ],
)
@pytest.mark.asyncio
async def test_download_fields_by_resource_slug(
    reader_api, test_resource, endpoint_part, endpoint_params
):
    rsc = test_resource
    kbid = rsc.kb.kbid
    slug = rsc.basic.slug
    if endpoint_part.startswith("conversation"):
        # For conversations, we need to get a message id and a file number
        msg_id, file_num = await _get_message_with_file(test_resource)
        endpoint_params["message_id"] = msg_id
        endpoint_params["file_num"] = file_num

    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        resource_path = f"/{KB_PREFIX}/{kbid}/{RSLUG_PREFIX}/{slug}"
        endpoint = endpoint_part.format(**endpoint_params)
        resp = await client.get(
            f"{resource_path}/{endpoint}",
        )
        assert resp.status_code == 200

        # Check that 404 is returned when a slug does not exist
        unexisting_resource_path = f"/{KB_PREFIX}/{kbid}/{RSLUG_PREFIX}/idonotexist"
        resp = await client.get(
            f"{unexisting_resource_path}/{endpoint}",
        )
        assert resp.status_code == 404
        assert resp.json()["detail"] == "Resource does not exist"


async def _get_message_with_file(test_resource):
    conversation_field = await test_resource.get_field("conv1", FieldType.CONVERSATION)
    conversations = await conversation_field.get_value(page=1)
    message_with_files = conversations.messages[33]
    msg_id, file_num = message_with_files.content.attachments[1].uri.split("/")[-2:]
    return msg_id, file_num
