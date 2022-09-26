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
from nucliadb_protos.writer_pb2_grpc import WriterStub


@pytest.mark.asyncio
async def test_suggest_fuzzy_search(
    nucliadb_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    """
    Test description:

    Create some resource on a knowledgebox and use the /suggest endpoint
    to fuzzy search them.
    """
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": "My resource",
            "slug": "myresource",
            "summary": "Some summary",
        },
    )
    assert resp.status_code == 201

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
        },
    )
    assert resp.status_code == 201
    rid3 = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/suggest?query=Nietzche")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["paragraphs"]["results"]) == 1
    assert body["paragraphs"]["results"][0]["rid"] == rid3

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/suggest?query=princes")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["paragraphs"]["results"]) == 2
    assert body["paragraphs"]["results"][0]["rid"] == rid2
    assert body["paragraphs"]["results"][1]["rid"] == rid2
    assert {"summary", "title"} == {
        result["field"] for result in body["paragraphs"]["results"]
    }

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/suggest?query=Hanna+Adrent")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["paragraphs"]["results"]) == 0

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/suggest?query=a")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["paragraphs"]["results"]) == 6


# @pytest.mark.asyncio
# async def test_suggest_related_entities(
#     nucliadb_grpc: WriterStub,
#     nucliadb_reader: AsyncClient,
#     nucliadb_writer: AsyncClient,
#     knowledgebox,
# ):
#     """
#     Test description:

#     Create a new resoure with some entities and relations and use
#     /suggest endpoint to make autocomplete suggestions.
#     """
#     resp = await nucliadb_writer.post(
#         f"/kb/{knowledgebox}/resources",
#         json={
#             "title": "My resource",
#             "slug": "myresource",
#             "summary": "Some summary",
#             "origin": {
#                 "colaborators": ["Irene", "Anastasia"],
#             },
#             "usermetadata": {
#                 "classifications": [
#                     {"labelset": "labelset-1", "label": "label-1"},
#                     {"labelset": "labelset-2", "label": "label-2"},
#                 ],
#                 "relations": [
#                     {
#                         "relation": "ENTITY",
#                         "entity": {
#                             "entity": "Anna",
#                             "entity_type": "person",
#                         },
#                     },
#                     {
#                         "relation": "ENTITY",
#                         "entity": {
#                             "entity": "Annika",
#                             "entity_type": "person",
#                         },
#                     },
#                     {
#                         "relation": "ENTITY",
#                         "entity": {
#                             "entity": "Anthony",
#                             "entity_type": "person",
#                         },
#                     },
#                     {
#                         "relation": "ENTITY",
#                         "entity": {
#                             "entity": "John",
#                             "entity_type": "person",
#                         },
#                     },
#                     {
#                         "relation": "ENTITY",
#                         "entity": {
#                             "entity": "Barcelona",
#                             "entity_type": "city",
#                         },
#                     },
#                     {
#                         "relation": "ENTITY",
#                         "entity": {
#                             "entity": "New York",
#                             "entity_type": "city",
#                         },
#                     },
#                     {
#                         "relation": "ENTITY",
#                         "entity": {
#                             "entity": "Israel",
#                             "entity_type": "country",
#                         },
#                     },
#                     {
#                         "relation": "ENTITY",
#                         "entity": {
#                             "entity": "Netherlands",
#                             "entity_type": "country",
#                         },
#                     },
#                     {
#                         "relation": "ENTITY",
#                         "entity": {
#                             "entity": "Solomon Islands",
#                             "entity_type": "country",
#                         },
#                     },
#                 ],
#             },
#         },
#     )
#     assert resp.status_code == 201

#     # Test simple suggestions

#     resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/suggest?query=An")
#     assert resp.status_code == 200
#     body = resp.json()
#     assert set(body["entities"]["entities"]) == {"Anna", "Annika", "Anthony"}

#     resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/suggest?query=ann")
#     assert resp.status_code == 200
#     body = resp.json()
#     assert set(body["entities"]["entities"]) == {"Anna", "Annika"}

#     resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/suggest?query=jo")
#     assert resp.status_code == 200
#     body = resp.json()
#     assert set(body["entities"]["entities"]) == {"John"}

#     resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/suggest?query=any")
#     assert resp.status_code == 200
#     body = resp.json()
#     assert not body["entities"]["entities"]

#     # Test correct query tokenization

#     resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/suggest?query=bar")
#     assert resp.status_code == 200
#     body = resp.json()
#     assert set(body["entities"]["entities"]) == {"Barcelona"}

#     resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/suggest?query=Bar")
#     assert resp.status_code == 200
#     body = resp.json()
#     assert set(body["entities"]["entities"]) == {"Barcelona"}

#     resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/suggest?query=BAR")
#     assert resp.status_code == 200
#     body = resp.json()
#     assert set(body["entities"]["entities"]) == {"Barcelona"}

#     resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/suggest?query=BÄR")
#     assert resp.status_code == 200
#     body = resp.json()
#     assert set(body["entities"]["entities"]) == {"Barcelona"}

#     resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/suggest?query=BàR")
#     assert resp.status_code == 200
#     body = resp.json()
#     assert set(body["entities"]["entities"]) == {"Barcelona"}

#     # Test multiple word suggest and ordering

#     resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/suggest?query=Solomon+Is")
#     assert resp.status_code == 200
#     body = resp.json()
#     assert body["entities"]["entities"] == ["Solomon Islands", "Israel"]
