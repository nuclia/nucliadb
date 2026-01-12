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
import base64

import pytest
from httpx import AsyncClient

from nucliadb_models.search import (
    SyncAskResponse,
)
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
        f"/kb/{standalone_knowledgebox}/ask",
        headers={"x-synchronous": "true"},
        json={
            "query": "A yummy image of some cookies",
            "top_k": 1,
            "reranker": "noop",
            "debug": True,
            "rag_images_strategies": [
                {
                    "name": "paragraph_image",
                },
            ],
        },
    )
    assert resp.status_code == 200, resp.text
    data = SyncAskResponse.model_validate_json(resp.content)
    assert data.predict_request is not None
    assert f"{rid}/f/cookie-recipie/0-29" in data.predict_request["query_context_images"]
    assert (
        base64.b64decode(
            data.predict_request["query_context_images"][f"{rid}/f/cookie-recipie/0-29"]["b64encoded"]
        )
        == b"delicious cookies image"
    )
    assert (
        data.predict_request["query_context_images"][f"{rid}/f/cookie-recipie/0-29"]["content_type"]
        == "image/png"
    )


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
        f"/kb/{standalone_knowledgebox}/ask",
        headers={"x-synchronous": "true"},
        json={
            "query": "A yummy image of some cookies",
            "reranker": "noop",
            "top_k": 1,
            "debug": True,
            "rag_images_strategies": [
                {
                    "name": "page_image",
                    "count": 2,
                },
            ],
        },
    )
    assert resp.status_code == 200, resp.text
    data = SyncAskResponse.model_validate_json(resp.content)
    assert data.predict_request is not None
    assert f"{rid}/f/cookie-recipie/0" in data.predict_request["query_context_images"]
    assert (
        base64.b64decode(
            data.predict_request["query_context_images"][f"{rid}/f/cookie-recipie/0"]["b64encoded"]
        )
        == b"A page with an image of cookies"
    )
    assert (
        data.predict_request["query_context_images"][f"{rid}/f/cookie-recipie/0"]["content_type"]
        == "image/png"
    )


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
        f"/kb/{standalone_knowledgebox}/ask",
        headers={"x-synchronous": "true"},
        json={
            "query": "Ingredient: peanut butter",
            "reranker": "noop",
            "top_k": 1,
            "debug": True,
            "rag_images_strategies": [
                {
                    "name": "tables",
                },
            ],
        },
    )
    assert resp.status_code == 200, resp.text
    data = SyncAskResponse.model_validate_json(resp.content)
    assert data.predict_request is not None
    assert f"{rid}/f/cookie-recipie/29-75" in data.predict_request["query_context_images"]
    assert (
        base64.b64decode(
            data.predict_request["query_context_images"][f"{rid}/f/cookie-recipie/29-75"]["b64encoded"]
        )
        == b"ingredients table"
    )
    assert (
        data.predict_request["query_context_images"][f"{rid}/f/cookie-recipie/29-75"]["content_type"]
        == "image/png"
    )
