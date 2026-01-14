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

from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb.tests.vectors import Q
from nucliadb_models.retrieval import RetrievalResponse
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.ndbfixtures.resources import smb_wonder_resource


@pytest.mark.deploy_modes("standalone")
async def test_retrieve(
    nucliadb_search: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox: str,
) -> None:
    """Basic test for the /retrieve endpoint with a keyword and semantic query.

    We expect to match all paragraphs from the smb wonder resource. One of them
    should only match semantic, and we'll use it to check proper score history.

    """

    kbid = knowledgebox
    rid = await smb_wonder_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    resp = await nucliadb_search.post(
        f"/internal/{KB_PREFIX}/{kbid}/retrieve",
        json={
            "query": {
                "keyword": {
                    "query": "smb wonder",
                    "min_score": 0.0,
                },
                "semantic": {
                    "query": Q,
                    "vectorset": "my-semantic-model",
                    "min_score": -1,
                },
            },
            "top_k": 10,
        },
    )
    assert resp.status_code == 200

    body = RetrievalResponse.model_validate(resp.json())

    # we expect to match all paragraphs in the resource
    paragraph_ids = [match.id for match in body.matches]
    expected_paragraph_ids = [
        f"{rid}/a/title/0-24",
        f"{rid}/a/summary/0-44",
        f"{rid}/f/smb-wonder/0-99",
        f"{rid}/f/smb-wonder/99-145",
        f"{rid}/f/smb-wonder/145-234",
    ]
    assert sorted(paragraph_ids) == sorted(expected_paragraph_ids)

    # the best match has been found in both indexes and RRF boosted it
    assert len(body.matches[0].score.history) == 3
    assert body.matches[0].score.history[0].source == "index"
    assert body.matches[0].score.history[0].type == "keyword"
    assert body.matches[0].score.history[1].source == "index"
    assert body.matches[0].score.history[1].type == "semantic"
    assert body.matches[0].score.history[2].source == "rank_fusion"
    assert body.matches[0].score.history[2].type == "rrf"

    # this paragraph has no keyord match, so we matched it due to semantic
    matched_only_with_semantic = next(
        filter(
            lambda match: match.id == f"{rid}/f/smb-wonder/145-234",
            body.matches,
        )
    )
    assert len(matched_only_with_semantic.score.history) == 2
    assert matched_only_with_semantic.score.history[0].source == "index"
    assert matched_only_with_semantic.score.history[0].type == "semantic"
    assert matched_only_with_semantic.score.source == "rank_fusion"
    assert matched_only_with_semantic.score.type == "rrf"

    # Check that invalid semantic model is rejected
    resp = await nucliadb_search.post(
        f"/internal/{KB_PREFIX}/{kbid}/retrieve",
        json={
            "query": {
                "semantic": {
                    "query": Q,
                    "vectorset": "non-existing-model",
                },
            },
        },
    )
    assert resp.status_code == 422
    json_body = resp.json()
    assert (
        json_body["detail"]
        == "Invalid query. Error in vectorset: Vectorset non-existing-model doesn't exist in your Knowledge Box"
    )

    # Test that matryochka dimension is enforced
    resp = await nucliadb_search.post(
        f"/internal/{KB_PREFIX}/{kbid}/retrieve",
        json={
            "query": {
                "semantic": {
                    "query": Q[:50],  # too short
                    "vectorset": "my-semantic-model",
                },
            },
        },
    )
    assert resp.status_code == 422
    json_body = resp.json()
    assert (
        json_body["detail"]
        == "Invalid query. Error in vector: Invalid vector length, please check valid embedding size for my-semantic-model model"
    )
