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

from typing import Any
from unittest.mock import patch

import pytest

from nucliadb.search.search.find_merge import build_find_response
from nucliadb.search.search.rank_fusion import LegacyRankFusion
from nucliadb.search.search.rerankers import MultiMatchBoosterReranker
from nucliadb_models.resource import Resource
from nucliadb_models.search import SCORE_TYPE, ResourceProperties
from nucliadb_protos import nodereader_pb2, noderesources_pb2


async def test_find_post_index_search(expected_find_response: dict[str, Any]):
    query = "How should I validate this?"
    search_responses = [
        nodereader_pb2.SearchResponse(
            paragraph=nodereader_pb2.ParagraphSearchResponse(
                fuzzy_distance=1,
                total=1,
                results=[
                    nodereader_pb2.ParagraphResult(
                        uuid="rid-1",
                        field="/f/field-a",
                        start=10,
                        end=20,
                        index=3,
                        paragraph="rid-1/f/field-a/10-20",
                        score=nodereader_pb2.ResultScore(
                            bm25=1.125,
                        ),
                        labels=["/a/title"],
                    ),
                    nodereader_pb2.ParagraphResult(
                        uuid="rid-2",
                        field="/f/field-b",
                        split="subfield-x",
                        start=100,
                        end=150,
                        index=0,
                        paragraph="rid-2/f/field-b/subfield-x/100-150",
                        score=nodereader_pb2.ResultScore(
                            bm25=0.64,
                        ),
                    ),
                    nodereader_pb2.ParagraphResult(
                        uuid="rid-2",
                        field="/f/field-b",
                        split="subfield-y",
                        start=0,
                        end=17,
                        index=2,
                        paragraph="rid-2/f/field-b/subfield-y/0-17",
                        score=nodereader_pb2.ResultScore(
                            bm25=0.7025,
                        ),
                        metadata=noderesources_pb2.ParagraphMetadata(
                            position=noderesources_pb2.Position(
                                index=2,
                                start=0,
                                end=17,
                                page_number=10,
                                in_page=True,
                            ),
                            page_with_visual=True,
                            representation=noderesources_pb2.Representation(
                                is_a_table=True,
                                file="myfile.pdf",
                            ),
                        ),
                    ),
                ],
                page_number=0,
                result_per_page=20,
                query=query,
                next_page=False,
                bm25=True,
            ),
            vector=nodereader_pb2.VectorSearchResponse(
                documents=[
                    nodereader_pb2.DocumentScored(
                        doc_id=nodereader_pb2.DocumentVectorIdentifier(
                            id="rid-3/t/field-c/5/0-30",
                        ),
                        score=1.5,
                        labels=["u/link", "/k/text"],  # these are not propagated
                    ),
                    nodereader_pb2.DocumentScored(
                        doc_id=nodereader_pb2.DocumentVectorIdentifier(
                            id="rid-2/f/field-b/subfield-y/10/0-17",
                        ),
                        score=0.89,
                    ),
                ],
                page_number=0,
                result_per_page=20,
            ),
        )
    ]

    async def mock_hydrate_resource_metadata(kbid: str, rid: str, *args, **kwargs):
        return Resource(id=rid)

    with (
        patch("nucliadb.search.search.find.get_external_index_manager", return_value=None),
        patch(
            "nucliadb.search.search.find_merge.hydrate_resource_metadata",
            side_effect=mock_hydrate_resource_metadata,
        ),
        patch(
            "nucliadb.search.search.hydrator.paragraphs.get_paragraph_text",
            return_value="extracted text",
        ),
    ):
        find_response = await build_find_response(
            search_responses,
            kbid="kbid",
            query=query,
            relation_subgraph_query=nodereader_pb2.EntitiesSubgraphRequest(),
            page_size=20,
            page_number=0,
            min_score_bm25=0.2,
            min_score_semantic=0.4,
            show=[ResourceProperties.BASIC],
            field_type_filter=[],
            extracted=[],
            highlight=True,
            rank_fusion_algorithm=LegacyRankFusion(window=20),
            reranker=MultiMatchBoosterReranker(),
        )
        resp = find_response.model_dump()
        assert expected_find_response == resp


@pytest.fixture
def expected_find_response():
    """This is the expected find response previous to a refactor on
    find_merge.py code.

    Resource ids have been fixed, as there was a bug on resource id setting
    fixed by resource metadata serialization (which we aren't mocking)
    """
    yield {
        "autofilters": [],
        "best_matches": [
            "rid-2/f/field-b/subfield-y/0-17",
            "rid-3/t/field-c/0-30",
            "rid-1/f/field-a/10-20",
            "rid-2/f/field-b/subfield-x/100-150",
        ],
        "min_score": {"bm25": 0.2, "semantic": 0.4},
        "next_page": False,
        "nodes": None,
        "page_number": 0,
        "page_size": 20,
        "query": "How should I validate this?",
        "relations": {"entities": {}},
        "resources": {
            "rid-1": {
                "computedmetadata": None,
                "created": None,
                "data": None,
                "extra": None,
                "fieldmetadata": None,
                "fields": {
                    "/f/field-a": {
                        "paragraphs": {
                            "rid-1/f/field-a/10-20": {
                                "fuzzy_result": False,
                                "id": "rid-1/f/field-a/10-20",
                                "is_a_table": False,
                                "labels": ["/a/title"],
                                "order": 2,
                                "page_with_visual": False,
                                "position": {
                                    "end": 20,
                                    "end_seconds": [],
                                    "index": 0,
                                    "page_number": 0,
                                    "start": 10,
                                    "start_seconds": [],
                                },
                                "reference": "",
                                "score": 1.125,
                                "score_type": SCORE_TYPE.BM25,
                                "text": "extracted text",
                            }
                        }
                    }
                },
                "hidden": None,
                "icon": None,
                "id": "rid-1",
                "last_account_seq": None,
                "last_seqid": None,
                "metadata": None,
                "modified": None,
                "origin": None,
                "queue": None,
                "relations": None,
                "security": None,
                "slug": None,
                "summary": None,
                "thumbnail": None,
                "title": None,
                "usermetadata": None,
            },
            "rid-2": {
                "computedmetadata": None,
                "created": None,
                "data": None,
                "extra": None,
                "fieldmetadata": None,
                "fields": {
                    "/f/field-b": {
                        "paragraphs": {
                            "rid-2/f/field-b/subfield-x/100-150": {
                                "fuzzy_result": False,
                                "id": "rid-2/f/field-b/subfield-x/100-150",
                                "is_a_table": False,
                                "labels": [],
                                "order": 3,
                                "page_with_visual": False,
                                "position": {
                                    "end": 150,
                                    "end_seconds": [],
                                    "index": 0,
                                    "page_number": 0,
                                    "start": 100,
                                    "start_seconds": [],
                                },
                                "reference": "",
                                "score": 0.6399999856948853,
                                "score_type": SCORE_TYPE.BM25,
                                "text": "extracted text",
                            },
                            "rid-2/f/field-b/subfield-y/0-17": {
                                "fuzzy_result": False,
                                "id": "rid-2/f/field-b/subfield-y/0-17",
                                "is_a_table": True,
                                "labels": [],
                                "order": 0,
                                "page_with_visual": True,
                                "position": {
                                    "end": 17,
                                    "end_seconds": [],
                                    "index": 2,
                                    "page_number": 10,
                                    "start": 0,
                                    "start_seconds": [],
                                },
                                "reference": "myfile.pdf",
                                "score": 1.7799999713897705,
                                "score_type": SCORE_TYPE.BOTH,
                                "text": "extracted text",
                            },
                        }
                    }
                },
                "hidden": None,
                "icon": None,
                "id": "rid-2",
                "last_account_seq": None,
                "last_seqid": None,
                "metadata": None,
                "modified": None,
                "origin": None,
                "queue": None,
                "relations": None,
                "security": None,
                "slug": None,
                "summary": None,
                "thumbnail": None,
                "title": None,
                "usermetadata": None,
            },
            "rid-3": {
                "computedmetadata": None,
                "created": None,
                "data": None,
                "extra": None,
                "fieldmetadata": None,
                "fields": {
                    "/t/field-c": {
                        "paragraphs": {
                            "rid-3/t/field-c/0-30": {
                                "fuzzy_result": False,
                                "id": "rid-3/t/field-c/0-30",
                                "is_a_table": False,
                                "labels": [],
                                "order": 1,
                                "page_with_visual": False,
                                "position": {
                                    "end": 30,
                                    "end_seconds": [],
                                    "index": 0,
                                    "page_number": 0,
                                    "start": 0,
                                    "start_seconds": [],
                                },
                                "reference": "",
                                "score": 1.5,
                                "score_type": SCORE_TYPE.VECTOR,
                                "text": "extracted text",
                            }
                        }
                    }
                },
                "hidden": None,
                "icon": None,
                "id": "rid-3",
                "last_account_seq": None,
                "last_seqid": None,
                "metadata": None,
                "modified": None,
                "origin": None,
                "queue": None,
                "relations": None,
                "security": None,
                "slug": None,
                "summary": None,
                "thumbnail": None,
                "title": None,
                "usermetadata": None,
            },
        },
        "shards": None,
        "total": 1,
    }
