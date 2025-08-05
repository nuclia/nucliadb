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

from copy import deepcopy
from datetime import datetime, timezone
from os.path import dirname

import pytest
from httpx import AsyncClient

from nucliadb.writer.api.v1.router import (
    KB_PREFIX,
    RESOURCE_PREFIX,
    RESOURCES_PREFIX,
    RSLUG_PREFIX,
)
from tests.writer.utils import load_file_as_FileB64_payload

TEST_FILE = {f"{dirname(__file__)}/orm/"}
TEST_TEXT_PAYLOAD = {"body": "test1", "format": "PLAIN"}
TEST_LINK_PAYLOAD = {
    "added": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "headers": {},
    "cookies": {},
    "uri": "http://some-link.com",
    "language": "en",
    "localstorage": {},
    "css_selector": "main",
    "xpath": "my_xpath",
}
TEST_CONVERSATION_PAYLOAD = {
    "messages": [
        {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "who": "Bob",
            "to": ["Alice", "Charlie"],
            "content": {
                "text": "Hi people!",
                "format": "PLAIN",
                "attachments": [load_file_as_FileB64_payload("/assets/image001.jpg", "image/jpeg")],
                "attachments_fields": [{"field_type": "file", "field_id": "file"}],
            },
            "ident": "message_id_001",
        }
    ]
}

TEST_FILE_PAYLOAD = {
    "language": "en",
    "password": "xxxxxx",
    "file": load_file_as_FileB64_payload("/assets/image001.jpg", "image/jpeg"),
}

TEST_EXTERNAL_FILE_PAYLOAD = {
    "file": {
        "uri": "https://mysite.com/files/myfile.pdf",
        "extra_headers": {"foo": "bar"},
    }
}

TEST_CONVERSATION_APPEND_MESSAGES_PAYLOAD = [
    {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "who": "Bob",
        "to": ["Alice", "Charlie"],
        "content": {
            "text": "Hi people!",
            "format": "PLAIN",
            "attachments": [load_file_as_FileB64_payload("/assets/image001.jpg", "image/jpeg")],
        },
        "ident": "message_id_001",
    }
]


@pytest.mark.deploy_modes("component")
async def test_resource_field_add(nucliadb_writer: AsyncClient, knowledgebox_writer: str):
    kbid = knowledgebox_writer

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={"slug": "resource1", "title": "My resource"},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert "uuid" in data
    assert "seqid" in data
    rid = data["uuid"]

    # Text
    resp = await nucliadb_writer.put(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/text/text1",
        json=TEST_TEXT_PAYLOAD,
    )
    assert resp.status_code == 201
    data = resp.json()
    assert "seqid" in data

    # Link
    resp = await nucliadb_writer.put(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/link/link1",
        json=TEST_LINK_PAYLOAD,
    )
    assert resp.status_code == 201
    data = resp.json()
    assert "seqid" in data

    # Conversation
    resp = await nucliadb_writer.put(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/conversation/conv1",
        json=TEST_CONVERSATION_PAYLOAD,
    )

    assert resp.status_code == 201
    data = resp.json()
    assert "seqid" in data

    # File
    resp = await nucliadb_writer.put(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/file/file1",
        json=TEST_FILE_PAYLOAD,
    )
    assert resp.status_code == 201
    data = resp.json()
    assert "seqid" in data

    # File without storing it in the internal BrokerMessage, only send to process
    resp = await nucliadb_writer.put(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/file/file1",
        json=TEST_FILE_PAYLOAD,
        headers={"x_skip_store": "1"},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert "seqid" in data

    # File field pointing to an externally hosted file
    resp = await nucliadb_writer.put(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/file/externalfile",
        json=TEST_EXTERNAL_FILE_PAYLOAD,
    )
    assert resp.status_code == 201
    data = resp.json()
    assert "seqid" in data


@pytest.mark.deploy_modes("component")
async def test_resource_field_append_extra(nucliadb_writer: AsyncClient, knowledgebox_writer: str):
    kbid = knowledgebox_writer

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": "resource1",
            "title": "My resource",
            "conversations": {"conv1": TEST_CONVERSATION_PAYLOAD},
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    assert "uuid" in data
    assert "seqid" in data
    rid = data["uuid"]

    # Conversation
    resp = await nucliadb_writer.put(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/conversation/conv1/messages",
        json=TEST_CONVERSATION_APPEND_MESSAGES_PAYLOAD,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "seqid" in data


@pytest.mark.deploy_modes("component")
async def test_resource_field_delete(nucliadb_writer: AsyncClient, knowledgebox_writer):
    kbid = knowledgebox_writer

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": "resource1",
            "title": "My resource",
            "texts": {"text1": TEST_TEXT_PAYLOAD},
            "links": {"link1": TEST_LINK_PAYLOAD},
            "files": {"file1": TEST_FILE_PAYLOAD},
            "conversations": {"conv1": TEST_CONVERSATION_PAYLOAD},
        },
    )

    assert resp.status_code == 201
    data = resp.json()
    rid = data["uuid"]

    # Text
    resp = await nucliadb_writer.delete(f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/text/text1")
    assert resp.status_code == 204

    # Link
    resp = await nucliadb_writer.delete(f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/link/link1")
    assert resp.status_code == 204

    # Conversation
    resp = await nucliadb_writer.delete(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/conversation/conv1"
    )

    # File
    resp = await nucliadb_writer.delete(f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/file/file1")
    assert resp.status_code == 204


@pytest.mark.parametrize(
    "endpoint,payload",
    [
        ("text/text1", TEST_TEXT_PAYLOAD),
        ("link/link1", TEST_LINK_PAYLOAD),
        ("conversation/conv1", TEST_CONVERSATION_PAYLOAD),
        ("conversation/conv1/messages", TEST_CONVERSATION_APPEND_MESSAGES_PAYLOAD),
        ("file/file1", TEST_FILE_PAYLOAD),
    ],
)
@pytest.mark.deploy_modes("component")
async def test_sync_ops(nucliadb_writer: AsyncClient, knowledgebox_writer, endpoint, payload):
    kbid = knowledgebox_writer

    # Create a resource
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": "resource1",
            "title": "My resource",
            "conversations": {"conv1": TEST_CONVERSATION_PAYLOAD},
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    rid = data["uuid"]

    resource_path = f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}"
    resp = await nucliadb_writer.put(
        f"{resource_path}/{endpoint}",
        json=payload,
    )
    assert resp.status_code in (201, 200)


@pytest.mark.deploy_modes("component")
async def test_external_file_field(nucliadb_writer: AsyncClient, knowledgebox_writer):
    kbid = knowledgebox_writer

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={"slug": "resource1", "title": "My resource"},
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # File field pointing to an externally hosted file
    resp = await nucliadb_writer.put(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/file/externalfile",
        json=TEST_EXTERNAL_FILE_PAYLOAD,
    )
    assert resp.status_code == 201


@pytest.mark.deploy_modes("component")
async def test_file_field_validation(nucliadb_writer: AsyncClient, knowledgebox_writer):
    kbid = knowledgebox_writer

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={"slug": "resource1", "title": "My resource"},
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Remove a required key from the payload
    payload: dict = deepcopy(TEST_FILE_PAYLOAD)
    payload["file"].pop("md5")

    resp = await nucliadb_writer.put(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/file/file1",
        json=payload,
    )
    assert resp.status_code == 201


@pytest.mark.parametrize(
    "method,endpoint,payload",
    [
        ["put", "/text/{field_id}", TEST_TEXT_PAYLOAD],
        ["put", "/link/{field_id}", TEST_LINK_PAYLOAD],
        ["put", "/conversation/{field_id}", TEST_CONVERSATION_PAYLOAD],
        ["put", "/file/{field_id}", TEST_FILE_PAYLOAD],
        ["delete", "", None],
    ],
)
@pytest.mark.deploy_modes("component")
async def test_field_endpoints_by_slug(
    nucliadb_writer: AsyncClient,
    knowledgebox_ingest,
    method,
    endpoint,
    payload,
):
    slug = "my-resource"
    field_id = "myfield"
    field_type = "text"

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox_ingest}/{RESOURCES_PREFIX}",
        json={"slug": slug},
    )
    assert resp.status_code == 201

    extra_params = {}
    if payload is not None:
        extra_params["json"] = payload
    op = getattr(nucliadb_writer, method)

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
