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

from nucliadb.tests.vectors import Q
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder


@pytest.mark.deploy_modes("standalone")
async def test_search_after_pagination_matches_single_request(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
) -> None:
    """Paginating with search_after (top_k=1) must yield the same ordered
    paragraph IDs as a single large request."""

    kbid = standalone_knowledgebox

    for i in range(5):
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json={
                "title": f"The best book about potatoes",
                "summary": f"Just some book telling you what to do with potatoes",
                "icon": "text/plain",
            },
        )
        assert resp.status_code == 201, resp.text

    # One request with big top_k to list all resources expected
    find_payload = {
        "query": "potatoes",
        "features": ["keyword"],
        "reranker": "noop",
        "top_k": 100,
    }
    resp = await nucliadb_reader.post(f"/kb/{kbid}/find", json=find_payload)
    assert resp.status_code == 200, resp.text
    baseline_data = resp.json()

    baseline_ids = baseline_data["best_matches"]

    assert len(baseline_ids) == 10  # 5 * 2 (title+content)

    # Paginated with k = 1, should iterate all resources one by one
    paginated_ids: list[str] = []
    token: str | None = None

    # Limit as defensive programming to avoid infinite loops
    for _ in range(20):
        payload = {
            "query": "potatoes",
            "top_k": 1,
        }
        if token is not None:
            payload["search_after"] = token

        resp = await nucliadb_reader.post(f"/kb/{kbid}/find", json=payload)
        assert resp.status_code == 200, resp.text
        body = resp.json()

        page_ids = body["best_matches"]
        paginated_ids.extend(page_ids)

        token = body.get("search_after")
        token_present = token is not None
        assert token_present == body["next_page"]

        if not token or len(page_ids) < 1:
            break

    assert set(paginated_ids) == set(baseline_ids)


@pytest.mark.deploy_modes("standalone")
async def test_search_after_hybrid_pagination_exercises_skip(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
) -> None:
    """
    When the first paginated page uses hybrid search, a keyword result may surface
    earlier than its BM25 rank because of vector boosting. This result must be skipped
    (no duplicates) so the state must be kept through all pages.
    """
    kbid = standalone_knowledgebox

    # A few keyword-only resources with double "potatoes" counts so they are first
    for n in range(6):
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json={"title": "About potatoes (not sweet potatoes)", "icon": "text/plain"},
        )
        assert resp.status_code == 201, resp.text

    # One resource with a single "potatoes" keyword and a vector so it will rank first with rank fusion
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "A book about potatoes and other things with lower bm25 score",
            "icon": "text/plain",
        },
    )
    assert resp.status_code == 201, resp.text
    hybrid_rid = resp.json()["uuid"]

    bmb = BrokerMessageBuilder(kbid=kbid, rid=hybrid_rid, source=BrokerMessage.MessageSource.PROCESSOR)
    bmb.with_title("potatoes")
    content_field = bmb.field_builder("content", FieldType.TEXT)
    content_field.add_paragraph(
        "A book about potatoes and other things with lower bm25 score",
        vectors={"multilingual": list(Q)},
    )
    await inject_message(nucliadb_ingest_grpc, bmb.build())

    # List all results
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query": "potatoes",
            "features": ["keyword"],
            "reranker": "noop",
            "top_k": 100,
        },
    )
    assert resp.status_code == 200, resp.text
    baseline_ids = set(resp.json()["best_matches"])
    assert len(baseline_ids) == 8  # 6 * 1 (title) + 1 * 2 (title + content)

    # Ensure the hybrid result is last by bm25 score
    assert resp.json()["best_matches"][-1].startswith(hybrid_rid)

    # Compare with pagination
    paginated_ids: list[str] = []
    token: str | None = None
    first_page = True

    # Limit as defensive programming to avoid infinite loops
    for _ in range(20):
        payload: dict = {
            "query": "potatoes",
            "top_k": 2,
            "rank_fusion": {"name": "rrf", "window": 20},
            "reranker": "noop",
        }
        if token is not None:
            payload["search_after"] = token

        resp = await nucliadb_reader.post(f"/kb/{kbid}/find", json=payload)
        assert resp.status_code == 200, resp.text
        body = resp.json()

        page_ids = body["best_matches"]
        paginated_ids.extend(page_ids)
        token = body.get("search_after")
        assert (token is not None) == body["next_page"]

        if first_page:
            # Hybrid result in first page
            assert resp.json()["best_matches"][0].startswith(hybrid_rid)
            first_page = False
        else:
            # Hybrid result not in any other page
            for match in resp.json()["best_matches"]:
                assert not match.startswith(hybrid_rid)

        if not token or not page_ids or not body["next_page"]:
            break

    # No duplicates
    assert len(paginated_ids) == len(set(paginated_ids))

    # Same results
    assert set(baseline_ids) == set(paginated_ids)
