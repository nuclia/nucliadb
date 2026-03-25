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

import json
from typing import Any, Literal

import pytest
from httpx import AsyncClient, Response
from typing_extensions import assert_never

from nucliadb_protos.resources_pb2 import LinkExtractedData
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message


async def suggest(
    nucliadb_reader: AsyncClient,
    method: Literal["GET"] | Literal["POST"],
    kbid: str,
    payload: dict[str, Any],
) -> Response:
    if method == "GET":
        return await nucliadb_reader.get(
            f"/kb/{kbid}/suggest",
            params=payload,
        )

    elif method == "POST":
        return await nucliadb_reader.post(
            f"/kb/{kbid}/suggest",
            json=payload,
        )

    else:  # pragma: no cover
        assert_never(method)


@pytest.mark.parametrize(
    "method",
    ["GET", "POST"],
)
@pytest.mark.deploy_modes("standalone")
async def test_suggest_paragraphs(
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    method: Literal["GET"] | Literal["POST"],
):
    """
    Test description:

    Create some resource on a standalone_knowledgebox and use the /suggest endpoint
    to search them.
    """
    kbid = standalone_knowledgebox
    payload: dict[str, Any]

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "My resource",
            "slug": "myresource",
            "summary": "Some summary",
        },
    )
    assert resp.status_code == 201

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
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
        f"/kb/{kbid}/resources",
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

    # exact match
    resp = await suggest(nucliadb_reader, method, kbid, {"query": "Nietzche"})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["paragraphs"]["results"]) == 1
    assert body["paragraphs"]["results"][0]["rid"] == rid3

    # typo tolerant search
    resp = await suggest(nucliadb_reader, method, kbid, {"query": "princes"})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["paragraphs"]["results"]) == 2
    assert body["paragraphs"]["results"][0]["rid"] == rid2
    assert body["paragraphs"]["results"][1]["rid"] == rid2
    assert {"summary", "title"} == {result["field"] for result in body["paragraphs"]["results"]}

    # we won't match anything, as 'z' is too short to do fuzzy
    resp = await suggest(nucliadb_reader, method, kbid, {"query": "z"})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["paragraphs"]["results"]) == 0
    # however, 'a' will match exactly and match one resource
    resp = await suggest(nucliadb_reader, method, kbid, {"query": "a"})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["paragraphs"]["results"]) == 1
    assert body["paragraphs"]["results"][0]["rid"] == rid2
    assert body["paragraphs"]["results"][0]["field"] == "summary"

    # nonexistent term
    resp = await suggest(nucliadb_reader, method, kbid, {"query": "Hanna Adrent"})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["paragraphs"]["results"]) == 0

    # by field

    if method == "GET":
        payload = {"query": "prince", "fields": "a/title"}
    elif method == "POST":
        payload = {
            "query": "prince",
            "filter_expression": {"field": {"prop": "field", "type": "generic", "name": "title"}},
        }
    else:  # pragma: no cover
        assert_never(method)
    resp = await suggest(nucliadb_reader, method, kbid, payload)
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["paragraphs"]["results"]) == 1
    assert body["paragraphs"]["results"][0]["field"] == "title"

    # filter by language
    if method == "GET":
        payload = {"query": "prince", "filters": "/metadata.language/en"}
    elif method == "POST":
        payload = {
            "query": "prince",
            "filter_expression": {"field": {"prop": "language", "only_primary": True, "language": "en"}},
        }
    else:  # pragma: no cover
        assert_never(method)
    resp = await suggest(nucliadb_reader, method, kbid, payload)
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["paragraphs"]["results"]) == 2
    assert {"summary", "title"} == {result["field"] for result in body["paragraphs"]["results"]}

    # No "prince" appear in any german resource

    if method == "GET":
        payload = {"query": "prince", "filters": "/metadata.language/de"}
    elif method == "POST":
        payload = {
            "query": "prince",
            "filter_expression": {"field": {"prop": "language", "only_primary": True, "language": "de"}},
        }
    else:  # pragma: no cover
        assert_never(method)
    resp = await suggest(nucliadb_reader, method, kbid, payload)
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["paragraphs"]["results"]) == 0

    # filter by language (filtering expression)

    if method == "GET":
        payload = {
            "query": "prince",
            "filter_expression": json.dumps(
                {"field": {"prop": "language", "only_primary": True, "language": "en"}}
            ),
        }
    elif method == "POST":
        payload = {
            "query": "prince",
            "filter_expression": {"field": {"prop": "language", "only_primary": True, "language": "en"}},
        }
    else:  # pragma: no cover
        assert_never(method)
    resp = await suggest(nucliadb_reader, method, kbid, payload)
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["paragraphs"]["results"]) == 2
    assert {"summary", "title"} == {result["field"] for result in body["paragraphs"]["results"]}

    if method == "GET":
        payload = {
            "query": "prince",
            "filter_expression": json.dumps({"paragraph": {"prop": "kind", "kind": "OCR"}}),
        }
    elif method == "POST":
        payload = {
            "query": "prince",
            "filter_expression": {"paragraph": {"prop": "kind", "kind": "OCR"}},
        }
    else:  # pragma: no cover
        assert_never(method)
    resp = await suggest(nucliadb_reader, method, kbid, payload)
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["paragraphs"]["results"]) == 0


@pytest.mark.deploy_modes("standalone")
async def test_suggest_rejects_mixture_of_filters(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox

    # filter by expression and old filters (error)
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/suggest",
        params={
            "query": "prince",
            "filters": "/metadata.language/de",
            "filter_expression": json.dumps({"field": {"prop": "language", "language": "en"}}),
        },
    )
    assert resp.status_code == 412


@pytest.mark.parametrize(
    "method",
    ["GET", "POST"],
)
@pytest.mark.deploy_modes("standalone")
async def test_suggest_related_entities(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    method: Literal["GET"] | Literal["POST"],
):
    """
    Test description:

    Create a new resoure with some entities and relations and use
    /suggest endpoint to make autocomplete suggestions.
    """
    kbid = standalone_knowledgebox

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
        f"/kb/{kbid}/resources",
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

    def assert_expected_entities(body, expected):
        assert {e["value"] for e in body["entities"]["entities"]} == expected

    # Test simple suggestions
    resp = await suggest(nucliadb_reader, method, kbid, {"query": "Ann"})
    assert resp.status_code == 200
    body = resp.json()
    assert_expected_entities(body, {"Anna", "Anthony"})

    resp = await suggest(nucliadb_reader, method, kbid, {"query": "joh"})
    assert resp.status_code == 200
    body = resp.json()
    assert_expected_entities(body, {"John"})

    resp = await suggest(nucliadb_reader, method, kbid, {"query": "xxxxx"})
    assert resp.status_code == 200
    body = resp.json()
    assert not body["entities"]["entities"]

    # Test correct query tokenization

    resp = await suggest(nucliadb_reader, method, kbid, {"query": "bar"})
    assert resp.status_code == 200
    body = resp.json()
    assert_expected_entities(body, {"Barcelona", "Bárcenas"})

    resp = await suggest(nucliadb_reader, method, kbid, {"query": "Bar"})
    assert resp.status_code == 200
    body = resp.json()
    assert_expected_entities(body, {"Barcelona", "Bárcenas"})

    resp = await suggest(nucliadb_reader, method, kbid, {"query": "BAR"})
    assert resp.status_code == 200
    body = resp.json()
    assert_expected_entities(body, {"Barcelona", "Bárcenas"})

    resp = await suggest(nucliadb_reader, method, kbid, {"query": "BÄR"})
    assert resp.status_code == 200
    body = resp.json()
    assert_expected_entities(body, {"Barcelona", "Bárcenas"})

    resp = await suggest(nucliadb_reader, method, kbid, {"query": "BáR"})
    assert resp.status_code == 200
    body = resp.json()
    assert_expected_entities(body, {"Barcelona", "Bárcenas"})

    # Test multiple word suggest and ordering
    resp = await suggest(nucliadb_reader, method, kbid, {"query": "Solomon Is"})
    assert resp.status_code == 200
    body = resp.json()
    assert_expected_entities(body, {"Solomon Islands", "Israel"})


@pytest.mark.deploy_modes("standalone")
async def test_suggestion_on_link_computed_titles_sc6088(
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox,
):
    # Create a resource with a link field
    link = "http://www.mylink.com"
    kbid = standalone_knowledgebox
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

    await inject_message(nucliadb_ingest_grpc, bm)

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


@pytest.mark.parametrize(
    "method",
    ["GET", "POST"],
)
@pytest.mark.deploy_modes("standalone")
async def test_suggest_features(
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    method: Literal["GET"] | Literal["POST"],
    texts: dict[str, str],
    entities,
):
    """Test description:

    Validate how responses are returned depending on requested features

    """
    kbid = standalone_knowledgebox

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
        expected = {"Anna", "Anthony"}
        assert len(response["entities"]) == len(expected)
        assert {e["value"] for e in response["entities"]["entities"]} == expected

    resp = await suggest(
        nucliadb_reader, method, kbid, {"query": "ann", "features": ["paragraph", "entities"]}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert_expected_entities(body)
    assert_expected_paragraphs(body)

    resp = await suggest(nucliadb_reader, method, kbid, {"query": "ann", "features": ["paragraph"]})
    assert resp.status_code == 200
    body = resp.json()
    assert body["entities"]["total"] == 0
    assert_expected_paragraphs(body)

    resp = await suggest(nucliadb_reader, method, kbid, {"query": "ann", "features": ["entities"]})
    assert resp.status_code == 200
    body = resp.json()
    assert_expected_entities(body)
    assert len(body["paragraphs"]["results"]) == 0


@pytest.mark.deploy_modes("standalone")
async def test_search_kb_not_found(nucliadb_reader: AsyncClient) -> None:
    resp = await nucliadb_reader.get(
        f"/kb/00000000000000/suggest?query=own+text",
    )
    assert resp.status_code == 404


@pytest.fixture(scope="function")
async def texts(
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
) -> dict[str, str]:
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": "My resource",
            "slug": "myresource",
            "summary": "Some summary",
        },
    )
    assert resp.status_code == 201
    rid1 = resp.json()["uuid"]

    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
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
        f"/kb/{standalone_knowledgebox}/resources",
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


@pytest.fixture(scope="function")
async def entities(nucliadb_writer: AsyncClient, standalone_knowledgebox: str):
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
        f"/kb/{standalone_knowledgebox}/resources",
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
