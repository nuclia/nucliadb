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
from datetime import datetime
from typing import Any, Optional
from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient
from pytest_mock import MockerFixture

from nucliadb.common import datamanagers
from nucliadb.ingest.orm.resource import Resource
from nucliadb.models.internal import processing as processing_models
from nucliadb.models.internal.processing import PushPayload
from nucliadb.writer.api.v1.router import (
    KB_PREFIX,
    RESOURCE_PREFIX,
    RESOURCES_PREFIX,
    RSLUG_PREFIX,
)
from tests.writer.test_fields import (
    TEST_CONVERSATION_PAYLOAD,
    TEST_EXTERNAL_FILE_PAYLOAD,
    TEST_FILE_PAYLOAD,
    TEST_LINK_PAYLOAD,
    TEST_TEXT_PAYLOAD,
)


@pytest.mark.deploy_modes("component")
async def test_resource_crud(nucliadb_writer: AsyncClient, knowledgebox_writer: str):
    kbid = knowledgebox_writer
    # Test create resource
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": "resource1",
            "title": "My resource",
            "summary": "Some summary",
            "icon": "image/png",
            "metadata": {
                "language": "en",
                "metadata": {"key1": "value1", "key2": "value2"},
            },
            "fieldmetadata": [
                {
                    "paragraphs": [
                        {
                            "key": "paragraph1",
                            "classifications": [{"labelset": "ls1", "label": "label1"}],
                        }
                    ],
                    "field": {"field": "text1", "field_type": "text"},
                }
            ],
            "usermetadata": {
                "classifications": [{"labelset": "ls1", "label": "label1"}],
                "relations": [
                    {
                        "relation": "CHILD",
                        "to": {
                            "type": "resource",
                            "value": "resource_uuid",
                        },
                    }
                ],
            },
            "origin": {
                "source_id": "source_id",
                "url": "http://some_source",
                "created": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "modified": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "metadata": {"key1": "value1", "key2": "value2"},
                "tags": ["tag1", "tag2"],
                "collaborators": ["col1", "col2"],
                "filename": "file.pdf",
                "related": ["related1"],
            },
            "texts": {"text1": TEST_TEXT_PAYLOAD},
            "links": {"link1": TEST_LINK_PAYLOAD},
            "files": {
                "file1": TEST_FILE_PAYLOAD,
                "external1": TEST_EXTERNAL_FILE_PAYLOAD,
            },
            "conversations": {"conv1": TEST_CONVERSATION_PAYLOAD},
        },
    )

    assert resp.status_code == 201
    data = resp.json()
    assert "uuid" in data
    assert "seqid" in data
    rid = data["uuid"]

    # Test update resource
    resp = await nucliadb_writer.patch(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}",
        json={},
    )
    assert resp.status_code == 200

    data = resp.json()

    assert "seqid" in data

    # Test delete resource
    resp = await nucliadb_writer.delete(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}",
    )
    assert resp.status_code == 204


@pytest.mark.deploy_modes("component")
async def test_resource_crud_sync(nucliadb_writer: AsyncClient, knowledgebox_writer: str):
    kbid = knowledgebox_writer

    # Test create resource
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": "resource1",
            "title": "My resource",
            "summary": "Some summary",
            "icon": "image/png",
            "metadata": {
                "language": "en",
                "metadata": {"key1": "value1", "key2": "value2"},
            },
            "fieldmetadata": [
                {
                    "paragraphs": [
                        {
                            "key": "paragraph1",
                            "classifications": [{"labelset": "ls1", "label": "label1"}],
                        }
                    ],
                    "field": {"field": "text1", "field_type": "text"},
                }
            ],
            "usermetadata": {
                "classifications": [{"labelset": "ls1", "label": "label1"}],
                "relations": [
                    {
                        "relation": "CHILD",
                        "to": {
                            "type": "resource",
                            "value": "resource_uuid",
                        },
                    }
                ],
            },
            "origin": {
                "source_id": "source_id",
                "url": "http://some_source",
                "created": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "modified": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "metadata": {"key1": "value1", "key2": "value2"},
                "tags": ["tag1", "tag2"],
                "collaborators": ["col1", "col2"],
                "filename": "file.pdf",
                "related": ["related1"],
            },
            "texts": {"text1": TEST_TEXT_PAYLOAD},
            "links": {"link1": TEST_LINK_PAYLOAD},
            "files": {"file1": TEST_FILE_PAYLOAD},
            "conversations": {"conv1": TEST_CONVERSATION_PAYLOAD},
        },
    )

    assert resp.status_code == 201
    data = resp.json()
    assert "uuid" in data
    assert "seqid" in data
    assert "elapsed" in data
    rid = data["uuid"]

    assert (await datamanagers.atomic.resources.resource_exists(kbid=kbid, rid=rid)) is True

    # Test update resource
    resp = await nucliadb_writer.patch(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}",
        json={},
    )
    assert resp.status_code == 200

    # Test delete resource

    resp = await nucliadb_writer.delete(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/resource1",
    )

    assert resp.status_code == 404

    resp = await nucliadb_writer.delete(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}",
    )
    assert resp.status_code == 204

    assert (await datamanagers.atomic.resources.resource_exists(kbid=kbid, rid=rid)) is False


@pytest.mark.deploy_modes("component")
async def test_create_resource_async(
    nucliadb_writer: AsyncClient,
    knowledgebox_writer: str,
    mocker: MockerFixture,
):
    """Create a resoure and don't wait for it"""
    kbid = knowledgebox_writer

    from nucliadb.writer.api.v1.resource import transaction

    spy = mocker.spy(transaction, "commit")

    # create and wait for commit
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": "wait-for-it",
            "title": "My resource",
            "texts": {"text1": TEST_TEXT_PAYLOAD},
            "wait_for_commit": True,
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    assert "uuid" in data
    assert "elapsed" in data
    assert data["elapsed"] is not None
    rid = data["uuid"]
    assert (await datamanagers.atomic.resources.resource_exists(kbid=kbid, rid=rid)) is True
    assert spy.call_args.kwargs["wait"] is True

    # now without waiting for commit
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": "no-wait",
            "title": "My resource",
            "texts": {"text1": TEST_TEXT_PAYLOAD},
            "wait_for_commit": False,
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    assert "uuid" in data
    assert "elapsed" in data
    assert data["elapsed"] is None
    rid = data["uuid"]
    assert (await datamanagers.atomic.resources.resource_exists(kbid=kbid, rid=rid)) is False, (
        "shouldn't be ingest yet"
    )
    assert spy.call_args.kwargs["wait"] is False


@pytest.mark.deploy_modes("component")
async def test_reprocess_resource(
    nucliadb_writer: AsyncClient,
    full_resource: Resource,
    mocker,
    maindb_driver,
) -> None:
    rsc = full_resource
    kbid = rsc.kb.kbid
    rid = rsc.uuid

    from nucliadb.writer.utilities import get_processing

    processing = get_processing()
    processing.values.clear()  # type: ignore

    original = processing.send_to_process
    mocker.patch.object(processing, "send_to_process", AsyncMock(side_effect=original))

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/resource/{rid}/reprocess",
    )
    assert resp.status_code == 202

    assert processing.send_to_process.call_count == 1  # type: ignore
    payload = processing.send_to_process.call_args[0][0]  # type: ignore
    assert isinstance(payload, PushPayload)
    assert payload.uuid == rid
    assert payload.kbid == kbid

    assert isinstance(payload.filefield.get("file1"), str)
    assert payload.filefield["file1"] == "convert_internal_filefield_to_str,0"
    assert isinstance(payload.linkfield.get("link1"), processing_models.LinkUpload)
    assert isinstance(payload.textfield.get("text1"), processing_models.Text)
    assert isinstance(payload.conversationfield.get("conv1"), processing_models.PushConversation)
    assert (
        payload.conversationfield["conv1"].messages[33].content.attachments[0]
        == "convert_internal_cf_to_str,0"
    )
    assert (
        payload.conversationfield["conv1"].messages[33].content.attachments[1]
        == "convert_internal_cf_to_str,1"
    )


@pytest.mark.parametrize(
    "method,endpoint,payload",
    [
        ["patch", "/{KB_PREFIX}/{kb}/{RSLUG_PREFIX}/{slug}", {}],
        ["post", "/{KB_PREFIX}/{kb}/{RSLUG_PREFIX}/{slug}/reprocess", None],
        ["post", "/{KB_PREFIX}/{kb}/{RSLUG_PREFIX}/{slug}/reindex", None],
        ["delete", "/{KB_PREFIX}/{kb}/{RSLUG_PREFIX}/{slug}", None],
    ],
)
@pytest.mark.deploy_modes("component")
async def test_resource_endpoints_by_slug(
    nucliadb_writer: AsyncClient,
    knowledgebox: str,
    method: str,
    endpoint: str,
    payload: Optional[dict[Any, Any]],
):
    kbid = knowledgebox
    slug = "my-resource"
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": slug,
            "texts": {"text1": {"body": "test1", "format": "PLAIN"}},
        },
    )
    assert resp.status_code == 201

    endpoint = endpoint.format(
        KB_PREFIX=KB_PREFIX,
        kb=kbid,
        RSLUG_PREFIX=RSLUG_PREFIX,
        slug=slug,
    )
    extra_params = {}
    if payload is not None:
        extra_params["json"] = payload

    op = getattr(nucliadb_writer, method)
    resp = await op(endpoint, **extra_params)

    assert resp.status_code in (200, 202, 204)


@pytest.mark.parametrize(
    "method,endpoint,payload",
    [
        ["patch", "/{KB_PREFIX}/{kb}/{RSLUG_PREFIX}/{slug}", {}],
        ["post", "/{KB_PREFIX}/{kb}/{RSLUG_PREFIX}/{slug}/reprocess", None],
        ["post", "/{KB_PREFIX}/{kb}/{RSLUG_PREFIX}/{slug}/reindex", None],
        ["delete", "/{KB_PREFIX}/{kb}/{RSLUG_PREFIX}/{slug}", None],
    ],
)
@pytest.mark.deploy_modes("component")
async def test_resource_endpoints_by_slug_404(
    nucliadb_writer: AsyncClient,
    knowledgebox,
    method,
    endpoint,
    payload,
):
    endpoint = endpoint.format(
        KB_PREFIX=KB_PREFIX,
        kb=knowledgebox,
        RSLUG_PREFIX=RSLUG_PREFIX,
        slug="idonotexist",
    )
    extra_params = {}
    if payload is not None:
        extra_params["json"] = payload

    op = getattr(nucliadb_writer, method)
    resp = await op(endpoint, **extra_params)

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Resource does not exist"


@pytest.mark.deploy_modes("component")
async def test_reindex(nucliadb_writer: AsyncClient, full_resource: Resource):
    rsc = full_resource
    kbid = rsc.kb.kbid
    rid = rsc.uuid
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/resource/{rid}/reindex",
    )
    assert resp.status_code == 200

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/resource/{rid}/reindex?reindex_vectors=True",
    )
    assert resp.status_code == 200


@pytest.mark.deploy_modes("component")
async def test_paragraph_annotations(nucliadb_writer: AsyncClient, knowledgebox_writer: str):
    kbid = knowledgebox_writer
    # Must have at least one classification
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/resources",
        json={
            "texts": {"text1": TEST_TEXT_PAYLOAD},
            "fieldmetadata": [
                {
                    "paragraphs": [
                        {
                            "key": "paragraph1",
                            "classifications": [],
                        }
                    ],
                    "field": {"field": "text1", "field_type": "text"},
                }
            ],
        },
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["detail"] == "ensure classifications has at least 1 items"

    classification = {"label": "label", "labelset": "ls"}

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/resources",
        json={
            "texts": {"text1": TEST_TEXT_PAYLOAD},
            "fieldmetadata": [
                {
                    "paragraphs": [
                        {
                            "key": "paragraph1",
                            "classifications": [classification],
                        }
                    ],
                    "field": {"field": "text1", "field_type": "text"},
                }
            ],
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Classifications need to be unique
    resp = await nucliadb_writer.patch(
        f"/{KB_PREFIX}/{kbid}/resource/{rid}",
        json={
            "fieldmetadata": [
                {
                    "paragraphs": [
                        {
                            "key": "paragraph1",
                            "classifications": [classification, classification],
                        }
                    ],
                    "field": {"field": "text1", "field_type": "text"},
                }
            ],
        },
    )
    assert resp.status_code == 422
    body = resp.json()
    assert body["detail"] == "Paragraph classifications need to be unique"


@pytest.mark.deploy_modes("component")
async def test_hide_on_creation(
    nucliadb_writer: AsyncClient,
    nucliadb_writer_manager: AsyncClient,
    knowledgebox_writer: str,
):
    kbid = knowledgebox_writer

    # Create new resource (default = visible)
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": "resource1",
            "title": "My resource",
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    rid = data["uuid"]

    async with datamanagers.utils.with_ro_transaction() as txn:
        basic = await datamanagers.resources.get_basic(txn, kbid=kbid, rid=rid)
        assert basic and basic.hidden is False

    # Set KB to hide new resources
    resp = await nucliadb_writer_manager.patch(
        f"/{KB_PREFIX}/{kbid}",
        json={
            "hidden_resources_enabled": True,
            "hidden_resources_hide_on_creation": True,
        },
    )
    assert resp.status_code == 200, resp.text

    # Create new resource (hidden because of the previous setting)
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": "resource2",
            "title": "My resource",
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    rid = data["uuid"]

    async with datamanagers.utils.with_ro_transaction() as txn:
        basic = await datamanagers.resources.get_basic(txn, kbid=kbid, rid=rid)
        assert basic and basic.hidden is True
