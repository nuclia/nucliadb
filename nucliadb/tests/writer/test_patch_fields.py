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

from datetime import datetime, timezone

import pytest
from httpx import AsyncClient

from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RESOURCES_PREFIX
from tests.writer.utils import load_file_as_FileB64_payload


@pytest.mark.deploy_modes("component")
async def test_update_and_reprocess_file_field(
    nucliadb_writer: AsyncClient,
    knowledgebox: str,
    dummy_processing,
    processing_engine,
):
    kbid = knowledgebox
    field_id = "myfile"

    # Create a file field
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

    processing_engine.send_to_process.reset_mock()
    processing_engine.values.clear()

    # Patch with a different extract strategy and validate it's sent to process
    # with the new strategy

    resp = await nucliadb_writer.patch(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/file/{field_id}",
        json={"extract_strategy": "my-strategy"},
    )
    assert resp.status_code == 200

    assert processing_engine.send_to_process.await_count == 1
    assert len(processing_engine.values["convert_internal_filefield_to_str"]) == 1
    assert (
        processing_engine.values["convert_internal_filefield_to_str"][0][0].extract_strategy
        == "my-strategy"
    )
    assert len(processing_engine.values["send_to_process"]) == 1


@pytest.mark.deploy_modes("component")
async def test_update_and_reprocess_text_field(
    nucliadb_writer: AsyncClient,
    knowledgebox: str,
    dummy_processing,
    processing_engine,
):
    kbid = knowledgebox
    field_id = "mytext"

    # Create a text field
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": "resource",
            "title": "My resource",
            "texts": {
                field_id: {
                    "body": "hello",
                    "format": "PLAIN",
                }
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    processing_engine.send_to_process.reset_mock()
    processing_engine.values.clear()

    # Patch with a different extract strategy and validate it's sent to process
    # with the new strategy

    resp = await nucliadb_writer.patch(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/text/{field_id}",
        json={"extract_strategy": "my-strategy"},
    )
    assert resp.status_code == 200

    assert processing_engine.send_to_process.await_count == 1
    assert len(processing_engine.values["send_to_process"]) == 1
    assert (
        processing_engine.values["send_to_process"][0][0].textfield[field_id].extract_strategy
        == "my-strategy"
    )


@pytest.mark.deploy_modes("component")
async def test_update_and_reprocess_link_field(
    nucliadb_writer: AsyncClient,
    knowledgebox: str,
    dummy_processing,
    processing_engine,
):
    kbid = knowledgebox
    field_id = "mylink"

    # Create a link field
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": "resource",
            "title": "My resource",
            "links": {
                field_id: {
                    "added": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "headers": {},
                    "cookies": {},
                    "uri": "http://some-link.com",
                    "language": "en",
                    "localstorage": {},
                }
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    processing_engine.send_to_process.reset_mock()
    processing_engine.values.clear()

    # Patch with a different extract strategy and validate it's sent to process
    # with the new strategy

    resp = await nucliadb_writer.patch(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/link/{field_id}",
        json={"extract_strategy": "my-strategy"},
    )
    assert resp.status_code == 200

    assert processing_engine.send_to_process.await_count == 1
    assert len(processing_engine.values["send_to_process"]) == 1
    assert (
        processing_engine.values["send_to_process"][0][0].linkfield[field_id].extract_strategy
        == "my-strategy"
    )


@pytest.mark.deploy_modes("component")
async def test_update_and_reprocess_conversation_field(
    nucliadb_writer: AsyncClient,
    knowledgebox: str,
    dummy_processing,
    processing_engine,
):
    kbid = knowledgebox
    field_id = "myconversation"

    # Create a conversation field
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": "resource",
            "title": "My resource",
            "conversations": {
                field_id: {
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
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    processing_engine.send_to_process.reset_mock()
    processing_engine.values.clear()

    # Patch with a different extract strategy and validate it's sent to process
    # with the new strategy

    resp = await nucliadb_writer.patch(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/conversation/{field_id}",
        json={"extract_strategy": "my-strategy"},
    )
    assert resp.status_code == 200

    assert processing_engine.send_to_process.await_count == 1
    assert len(processing_engine.values["send_to_process"]) == 1
    assert (
        processing_engine.values["send_to_process"][0][0].conversationfield[field_id].extract_strategy
        == "my-strategy"
    )
