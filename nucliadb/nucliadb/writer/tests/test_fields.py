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

import hashlib
from base64 import b64encode
from copy import deepcopy
from datetime import datetime
from os.path import dirname
from unittest.mock import patch

import jwt
import pytest

from nucliadb.writer.api.v1.router import (
    KB_PREFIX,
    RESOURCE_PREFIX,
    RESOURCES_PREFIX,
    RSLUG_PREFIX,
)
from nucliadb_models.resource import NucliaDBRoles


def load_file_as_FileB64_payload(f: str, content_type: str) -> dict:
    file_location = f"{dirname(__file__)}/{f}"
    filename = f.split("/")[-1]
    data = b64encode(open(file_location, "rb").read())

    return {
        "filename": filename,
        "content_type": content_type,
        "payload": data.decode("utf-8"),
        "md5": hashlib.md5(data).hexdigest(),
    }


TEST_FILE = {f"{dirname(__file__)}/orm/"}
TEST_TEXT_PAYLOAD = {"body": "test1", "format": "PLAIN"}
TEST_LINK_PAYLOAD = {
    "added": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    "headers": {},
    "cookies": {},
    "uri": "http://some-link.com",
    "language": "en",
    "localstorage": {},
}
TEST_KEYWORDSETS_PAYLOAD = {"keywords": [{"value": "kw1"}, {"value": "kw2"}]}
TEST_DATETIMES_PAYLOAD = {"value": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
TEST_CONVERSATION_PAYLOAD = {
    "messages": [
        {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "who": "Bob",
            "to": ["Alice", "Charlie"],
            "content": {
                "text": "Hi people!",
                "format": "PLAIN",
                "files": [
                    load_file_as_FileB64_payload("/assets/image001.jpg", "image/jpg")
                ],
            },
            "ident": "message_id_001",
        }
    ]
}
TEST_LAYOUT_PAYLOAD = {
    "body": {
        "blocks": {
            "block1": {
                "x": 0,
                "y": 0,
                "cols": 1,
                "rows": 2,
                "type": "TITLE",
                "ident": "main_title",
                "payload": "This is a Test Title",
                "file": load_file_as_FileB64_payload(
                    "/assets/image001.jpg", "image/jpg"
                ),
            }
        }
    },
    "format": "NUCLIAv1",
}

TEST_FILE_PAYLOAD = {
    "language": "en",
    "password": "xxxxxx",
    "file": load_file_as_FileB64_payload("/assets/image001.jpg", "image/jpg"),
}

TEST_EXTERNAL_FILE_PAYLOAD = {
    "file": {
        "uri": "https://mysite.com/files/myfile.pdf",
        "extra_headers": {"foo": "bar"},
    }
}

TEST_CONVERSATION_APPEND_MESSAGES_PAYLOAD = [
    {
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "who": "Bob",
        "to": ["Alice", "Charlie"],
        "content": {
            "text": "Hi people!",
            "format": "PLAIN",
            "attachments": [
                load_file_as_FileB64_payload("/assets/image001.jpg", "image/jpg")
            ],
        },
        "ident": "message_id_001",
    }
]

TEST_LAYOUT_APPEND_BLOCKS_PAYLOAD = {
    "block1": {
        "x": 0,
        "y": 0,
        "cols": 1,
        "rows": 2,
        "type": "TITLE",
        "ident": "main_title",
        "payload": "This is a Test Title",
        "file": load_file_as_FileB64_payload("/assets/image001.jpg", "image/jpg"),
    }
}


@pytest.mark.asyncio
async def test_resource_field_add(writer_api, knowledgebox_writer):
    knowledgebox_id = knowledgebox_writer
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCES_PREFIX}",
            headers={"X-SYNCHRONOUS": "True"},
            json={"slug": "resource1", "title": "My resource"},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert "uuid" in data
        assert "seqid" in data
        rid = data["uuid"]

        # Text
        resp = await client.put(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/text/text1",
            json=TEST_TEXT_PAYLOAD,
        )
        assert resp.status_code == 201
        data = resp.json()
        assert "seqid" in data

        # Link
        resp = await client.put(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/link/link1",
            json=TEST_LINK_PAYLOAD,
        )
        assert resp.status_code == 201
        data = resp.json()
        assert "seqid" in data

        # Keywordset
        resp = await client.put(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/keywordset/kws1",
            json=TEST_KEYWORDSETS_PAYLOAD,
        )
        assert resp.status_code == 201
        data = resp.json()
        assert "seqid" in data

        # Datetimes

        resp = await client.put(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/datetime/date1",
            json=TEST_DATETIMES_PAYLOAD,
        )
        assert resp.status_code == 201
        data = resp.json()
        assert "seqid" in data

        # Conversation
        resp = await client.put(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/conversation/conv1",
            json=TEST_CONVERSATION_PAYLOAD,
        )

        assert resp.status_code == 201
        data = resp.json()
        assert "seqid" in data

        # Layout
        resp = await client.put(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/layout/layout1",
            json=TEST_LAYOUT_PAYLOAD,
        )

        assert resp.status_code == 201
        data = resp.json()
        assert "seqid" in data

        # File
        resp = await client.put(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/file/file1",
            json=TEST_FILE_PAYLOAD,
        )
        assert resp.status_code == 201
        data = resp.json()
        assert "seqid" in data

        # File without storing it in the internal BrokerMessage, only send to process
        resp = await client.put(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/file/file1",
            json=TEST_FILE_PAYLOAD,
            headers={"x_skip_store": "1"},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert "seqid" in data

        # File field pointing to an externally hosted file
        resp = await client.put(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/file/externalfile",
            json=TEST_EXTERNAL_FILE_PAYLOAD,
            headers={"X-SYNCHRONOUS": "True"},
        )
        assert resp.status_code == 201
        data = resp.json()
        assert "seqid" in data


@pytest.mark.asyncio
async def test_resource_field_append_extra(writer_api, knowledgebox_writer):
    knowledgebox_id = knowledgebox_writer
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCES_PREFIX}",
            headers={"X-SYNCHRONOUS": "True"},
            json={
                "slug": "resource1",
                "title": "My resource",
                "layouts": {"layout1": TEST_LAYOUT_PAYLOAD},
                "conversations": {"conv1": TEST_CONVERSATION_PAYLOAD},
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert "uuid" in data
        assert "seqid" in data
        rid = data["uuid"]

        # Conversation
        resp = await client.put(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/conversation/conv1/messages",
            json=TEST_CONVERSATION_APPEND_MESSAGES_PAYLOAD,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "seqid" in data

        # Layout
        resp = await client.put(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/layout/layout1/blocks",
            json=TEST_LAYOUT_APPEND_BLOCKS_PAYLOAD,
        )

        assert resp.status_code == 200
        data = resp.json()
        assert "seqid" in data


@pytest.mark.asyncio
async def test_resource_field_delete(writer_api, knowledgebox_writer):
    knowledgebox_id = knowledgebox_writer
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCES_PREFIX}",
            headers={"X-SYNCHRONOUS": "True"},
            json={
                "slug": "resource1",
                "title": "My resource",
                "texts": {"text1": TEST_TEXT_PAYLOAD},
                "links": {"link1": TEST_LINK_PAYLOAD},
                "files": {"file1": TEST_FILE_PAYLOAD},
                "layouts": {"layout1": TEST_LAYOUT_PAYLOAD},
                "conversations": {"conv1": TEST_CONVERSATION_PAYLOAD},
                "keywordsets": {"keywordset1": TEST_KEYWORDSETS_PAYLOAD},
                "datetimes": {"datetime1": TEST_DATETIMES_PAYLOAD},
            },
        )

        assert resp.status_code == 201
        data = resp.json()
        rid = data["uuid"]

        # Text
        resp = await client.delete(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/text/text1"
        )
        assert resp.status_code == 204

        # Link
        resp = await client.delete(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/link/link1"
        )
        assert resp.status_code == 204

        # Keywords
        resp = await client.delete(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/keywordset/kws1"
        )
        assert resp.status_code == 204

        # Datetimes

        resp = await client.delete(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/datetime/date1"
        )
        assert resp.status_code == 204

        # Conversation
        resp = await client.delete(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/conversation/conv1"
        )

        # Layout
        resp = await client.delete(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/layout/layout1"
        )
        assert resp.status_code == 204

        # File
        resp = await client.delete(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/file/file1"
        )
        assert resp.status_code == 204


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "endpoint,payload",
    [
        ("text/text1", TEST_TEXT_PAYLOAD),
        ("link/link1", TEST_LINK_PAYLOAD),
        ("keywordset/kws1", TEST_KEYWORDSETS_PAYLOAD),
        ("datetime/date1", TEST_DATETIMES_PAYLOAD),
        ("conversation/conv1", TEST_CONVERSATION_PAYLOAD),
        ("conversation/conv1/messages", TEST_CONVERSATION_APPEND_MESSAGES_PAYLOAD),
        ("layout/layout1", TEST_LAYOUT_PAYLOAD),
        ("layout/layout1/blocks", TEST_LAYOUT_APPEND_BLOCKS_PAYLOAD),
        ("file/file1", TEST_FILE_PAYLOAD),
    ],
)
async def test_sync_ops(writer_api, knowledgebox_writer, endpoint, payload):
    knowledgebox_id = knowledgebox_writer
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        HEADERS = {"X-SYNCHRONOUS": "True"}
        # Create a resource
        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCES_PREFIX}",
            headers=HEADERS,
            json={
                "slug": "resource1",
                "title": "My resource",
                "layouts": {"layout1": TEST_LAYOUT_PAYLOAD},
                "conversations": {"conv1": TEST_CONVERSATION_PAYLOAD},
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        rid = data["uuid"]

        resource_path = f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}"
        resp = await client.put(
            f"{resource_path}/{endpoint}",
            headers={"X-SYNCHRONOUS": "True"},
            json=payload,
        )
        assert resp.status_code in (201, 200)


@pytest.fixture(scope="function")
def jwt_encode_mock():
    with patch.object(jwt, attribute="encode") as mock:
        yield mock


@pytest.mark.asyncio
async def test_external_file_field_sends_correct_processing_payload(
    writer_api, knowledgebox_writer, jwt_encode_mock
):
    knowledgebox_id = knowledgebox_writer
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCES_PREFIX}",
            json={"slug": "resource1", "title": "My resource"},
            headers={"X-SYNCHRONOUS": "True"},
        )
        assert resp.status_code == 201
        rid = resp.json()["uuid"]

        # File field pointing to an externally hosted file
        resp = await client.put(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/file/externalfile",
            json=TEST_EXTERNAL_FILE_PAYLOAD,
            headers={"X-SYNCHRONOUS": "True"},
        )
        assert resp.status_code == 201

        # Check that the payload sent to processing is correct
        decoded_payload = jwt_encode_mock.call_args[0][0]
        assert decoded_payload["uri"] == TEST_EXTERNAL_FILE_PAYLOAD["file"]["uri"]
        assert decoded_payload["driver"] == 3
        assert (
            decoded_payload["extra_headers"]
            == TEST_EXTERNAL_FILE_PAYLOAD["file"]["extra_headers"]
        )


@pytest.mark.asyncio
async def test_file_field_validation(writer_api, knowledgebox_writer):
    knowledgebox_id = knowledgebox_writer
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCES_PREFIX}",
            json={"slug": "resource1", "title": "My resource"},
            headers={"X-SYNCHRONOUS": "True"},
        )
        assert resp.status_code == 201
        rid = resp.json()["uuid"]

        # Remove a required key from the payload
        payload = deepcopy(TEST_FILE_PAYLOAD)
        payload["file"].pop("md5")

        resp = await client.put(
            f"/{KB_PREFIX}/{knowledgebox_id}/{RESOURCE_PREFIX}/{rid}/file/file1",
            json=payload,
            headers={"X-SYNCHRONOUS": "True"},
        )
        assert resp.status_code == 201


@pytest.mark.parametrize(
    "method,endpoint,payload",
    [
        ["put", "/text/{field_id}", TEST_TEXT_PAYLOAD],
        ["put", "/link/{field_id}", TEST_LINK_PAYLOAD],
        ["put", "/keywordset/{field_id}", TEST_KEYWORDSETS_PAYLOAD],
        ["put", "/datetime/{field_id}", TEST_DATETIMES_PAYLOAD],
        ["put", "/layout/{field_id}", TEST_LAYOUT_PAYLOAD],
        ["put", "/conversation/{field_id}", TEST_CONVERSATION_PAYLOAD],
        ["put", "/file/{field_id}", TEST_FILE_PAYLOAD],
        ["delete", "", None],
    ],
)
@pytest.mark.asyncio()
async def test_field_endpoints_by_slug(
    writer_api,
    knowledgebox_ingest,
    method,
    endpoint,
    payload,
):
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        slug = "my-resource"
        field_id = "myfield"
        field_type = "text"

        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_ingest}/{RESOURCES_PREFIX}",
            headers={"X-SYNCHRONOUS": "True"},
            json={"slug": slug},
        )
        assert resp.status_code == 201

        extra_params = {}
        if payload is not None:
            extra_params["json"] = payload
        op = getattr(client, method)

        # Try first a non-existing slug should return 404
        url = endpoint.format(
            field_id=field_id,
            field_type=field_type,
        )

        resp = await op(
            f"/{KB_PREFIX}/{knowledgebox_ingest}/{RSLUG_PREFIX}/idonotexist" + url,
            **extra_params,
        )
        assert resp.status_code == 404
        assert resp.json()["detail"] == "Resource does not exist"

        # Try the happy path now
        url = endpoint.format(
            field_id=field_id,
            field_type=field_type,
        )
        resp = await op(
            f"/{KB_PREFIX}/{knowledgebox_ingest}/{RSLUG_PREFIX}/{slug}" + url,
            **extra_params,
        )
        assert str(resp.status_code).startswith("2")
