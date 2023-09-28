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

import pytest
from httpx import AsyncClient
from nucliadb_protos.resources_pb2 import LinkExtractedData
from nucliadb_protos.writer_pb2 import BrokerMessage, OpStatusWriter
from nucliadb_protos.writer_pb2_grpc import WriterStub


@pytest.mark.asyncio
async def test_suggestion_on_link_computed_titles_sc6088(
    nucliadb_writer,
    nucliadb_grpc,
    nucliadb_reader,
    knowledgebox,
):
    # Create a resource with a link field
    link = "http://www.mylink.com"
    kbid = knowledgebox
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": link,
            "links": {
                "mylink": {
                    "uri": link,
                }
            },
        },
        headers={"X-Synchronous": "True"},
        timeout=None,
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Simulate processing link extracted data
    extracted_title = "MyLink Website"
    bm = BrokerMessage()
    bm.type = BrokerMessage.MessageType.AUTOCOMMIT
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    bm.uuid = rid
    bm.kbid = kbid
    led = LinkExtractedData()
    led.field = "mylink"
    led.title = extracted_title
    bm.link_extracted_data.append(led)

    resp = await nucliadb_grpc.ProcessMessage([bm])
    assert resp.status == OpStatusWriter.Status.OK

    # Check that the resource title changed
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid}")
    assert resp.status_code == 200
    assert resp.json()["title"] == extracted_title

    # Test suggest returns the extracted title metadata
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/suggest", params={"query": "MyLink", "fields": "a/title"}
    )
    assert resp.status_code == 200
    body = resp.json()
    suggested = body["paragraphs"]["results"][0]
    assert suggested["field"] == "title"
    assert suggested["field_type"] == "a"
    assert suggested["rid"] == rid
    assert suggested["text"] == extracted_title


@pytest.mark.asyncio
async def test_suggest_features(
    nucliadb_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    knowledgebox: str,
    texts: dict[str, str],
    entities,
):
    """Test description:

    Validate how responses are returned depending on requested features

    """

    def assert_expected_paragraphs(response):
        assert len(response["paragraphs"]["results"]) == 2
        assert "People and places" in {
            response["paragraphs"]["results"][0]["text"],
            response["paragraphs"]["results"][1]["text"],
        }
        assert texts["little_prince"] in {
            response["paragraphs"]["results"][0]["rid"],
            response["paragraphs"]["results"][1]["rid"],
        }

    def assert_expected_entities(response):
        assert response["entities"]["total"] == 1
        assert set(response["entities"]["entities"]) == {"Anna"}

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/suggest",
        params={"query": "ann", "features": ["paragraph", "entities"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert_expected_entities(body)
    assert_expected_paragraphs(body)

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/suggest",
        params={"query": "ann", "features": ["paragraph"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["entities"]["total"] == 0
    assert_expected_paragraphs(body)

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/suggest", params={"query": "ann", "features": ["entities"]}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert_expected_entities(body)
    assert len(body["paragraphs"]["results"]) == 0


@pytest.mark.asyncio
@pytest.fixture(scope="function")
async def texts(
    nucliadb_writer: AsyncClient,
    knowledgebox: str,
) -> dict[str, str]:
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": "My resource",
            "slug": "myresource",
            "summary": "Some summary",
        },
    )
    assert resp.status_code == 201
    rid1 = resp.json()["uuid"]

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": "The little prince",
            "slug": "the-little-prince",
            "summary": (
                "The story follows a young prince who visits various planets in space, "
                "including Earth, and addresses themes of loneliness, friendship, love, "
                "and loss."
            ),
            "metadata": {
                "language": "en",
            },
        },
    )
    assert resp.status_code == 201
    rid2 = resp.json()["uuid"]
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": "Thus Spoke Zarathustra",
            "slug": "thus-spoke-zarathustra",
            "summary": "Philosophical written by Frederich Nietzche",
            "metadata": {
                "language": "de",
            },
        },
    )
    assert resp.status_code == 201
    rid3 = resp.json()["uuid"]

    return {
        "myresource": rid1,
        "little_prince": rid2,
        "zarathrustra": rid3,
    }


@pytest.mark.asyncio
@pytest.fixture(scope="function")
async def entities(nucliadb_writer: AsyncClient, knowledgebox: str):
    collaborators = ["Irene", "Anastasia"]
    entities = [
        ("Anna", "person"),
        ("Anthony", "person"),
        ("Bárcenas", "person"),
        ("Ben", "person"),
        ("John", "person"),
        ("Barcelona", "city"),
        ("New York", "city"),
        ("York", "city"),
        ("Israel", "country"),
        ("Netherlands", "country"),
        ("Solomon Islands", "country"),
    ]
    relations = [
        {
            "relation": "ENTITY",
            "to": {
                "type": "entity",
                "value": entity,
                "group": type,
            },
        }
        for entity, type in entities
    ]
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        headers={"X-Synchronous": "True"},
        json={
            "title": "People and places",
            "slug": "pap",
            "summary": "Test entities to validate suggest on relations index",
            "origin": {
                "collaborators": collaborators,
            },
            "usermetadata": {
                "classifications": [
                    {"labelset": "labelset-1", "label": "label-1"},
                    {"labelset": "labelset-2", "label": "label-2"},
                ],
                "relations": relations,
            },
        },
    )
    assert resp.status_code == 201
