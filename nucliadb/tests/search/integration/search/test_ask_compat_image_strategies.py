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

"""/ask endpoint compat tests: image strategies

There tests imitate the behavior of the image RAG strategies from the /ask
endpoint (now living outside nucliadb repo) to maintain code coverage. All of
them could be moved to a better place (reader/search tests).

Image strategies augment images from matches with visuals. There are some
variants for a paragraph extracted from an image (e.g., OCR), a table or a
paragraph from a page with a visual (image, table...).

A search endpoint is used for retrieval and reader download to get images.

"""

import pytest
from httpx import AsyncClient

from nucliadb_models.search import KnowledgeboxFindResults
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.ndbfixtures.resources import cookie_tale_resource


@pytest.mark.deploy_modes("standalone")
async def test_ask_paragraph_image_rag_strategy(
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox
    rid = await cookie_tale_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/find",
        json={
            "query": "A yummy image of some cookies",
            "top_k": 1,
            "reranker": "noop",
        },
    )
    assert resp.status_code == 200, resp.text
    data = KnowledgeboxFindResults.model_validate_json(resp.content)
    assert data.resources.keys() == set((rid,))
    resource = data.resources[rid]
    assert resource.fields.keys() == set(("/f/cookie-recipie",))
    field = resource.fields["/f/cookie-recipie"]
    assert field.paragraphs.keys() == set((f"{rid}/f/cookie-recipie/0-29",))
    paragraph = field.paragraphs[f"{rid}/f/cookie-recipie/0-29"]
    assert paragraph.page_with_visual is True
    assert paragraph.reference is not None
    assert paragraph.reference == "cookies.png"
    image_name = "generated/" + paragraph.reference

    async with nucliadb_reader.stream(
        "GET",
        f"/kb/{kbid}/resource/{rid}/file/cookie-recipie/download/extracted/{image_name}",
    ) as resp:
        image = await resp.aread()
    assert image == b"delicious cookies image"


@pytest.mark.deploy_modes("standalone")
async def test_ask_page_image_rag_strategy(
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox
    rid = await cookie_tale_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/find",
        json={
            "query": "A yummy image of some cookies",
            "top_k": 1,
            "reranker": "noop",
        },
    )
    assert resp.status_code == 200, resp.text
    data = KnowledgeboxFindResults.model_validate_json(resp.content)
    assert data.resources.keys() == set((rid,))
    resource = data.resources[rid]
    assert resource.fields.keys() == set(("/f/cookie-recipie",))
    field = resource.fields["/f/cookie-recipie"]
    assert field.paragraphs.keys() == set((f"{rid}/f/cookie-recipie/0-29",))
    paragraph = field.paragraphs[f"{rid}/f/cookie-recipie/0-29"]
    assert paragraph.position is not None
    assert paragraph.position.page_number == 0
    assert paragraph.page_with_visual is True
    image_name = "generated/" + f"extracted_images_{paragraph.position.page_number}.png"

    async with nucliadb_reader.stream(
        "GET",
        f"/kb/{kbid}/resource/{rid}/file/cookie-recipie/download/extracted/{image_name}",
    ) as resp:
        image = await resp.aread()
    assert image == b"A page with an image of cookies"


@pytest.mark.deploy_modes("standalone")
async def test_ask_table_image_rag_strategy(
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox
    rid = await cookie_tale_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/find",
        json={
            "query": "Ingredient: peanut butter",
            "top_k": 1,
            "reranker": "noop",
        },
    )
    assert resp.status_code == 200, resp.text
    data = KnowledgeboxFindResults.model_validate_json(resp.content)
    assert data.resources.keys() == set((rid,))
    resource = data.resources[rid]
    assert resource.fields.keys() == set(("/f/cookie-recipie",))
    field = resource.fields["/f/cookie-recipie"]
    assert field.paragraphs.keys() == set((f"{rid}/f/cookie-recipie/29-75",))
    paragraph = field.paragraphs[f"{rid}/f/cookie-recipie/29-75"]
    assert paragraph.is_a_table is True
    assert paragraph.reference is not None
    assert paragraph.reference == "ingredients_table.png"
    image_name = "generated/" + paragraph.reference

    async with nucliadb_reader.stream(
        "GET",
        f"/kb/{kbid}/resource/{rid}/file/cookie-recipie/download/extracted/{image_name}",
    ) as resp:
        image = await resp.aread()
    assert image == b"ingredients table"
