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
import datetime
import json
import unittest
from unittest.mock import AsyncMock, MagicMock

import pytest

from nucliadb.common.external_index_providers.pinecone import (
    PineconeIndexManager,
    PineconeQueryResults,
    convert_label_filter_expression,
    convert_timestamp_filter,
    convert_to_pinecone_filter,
)
from nucliadb_protos import nodereader_pb2
from nucliadb_protos.noderesources_pb2 import IndexParagraph, IndexParagraphs
from nucliadb_protos.noderesources_pb2 import Resource as PBResourceBrain
from nucliadb_utils.aiopynecone.models import QueryResponse, Vector, VectorMatch


@pytest.fixture()
def query_response():
    return QueryResponse(
        matches=[
            VectorMatch(
                id="rid/f/field/0/0-10",
                score=0.8,
                values=None,
                metadata={"labels": ["/t/text/label"], "access_groups": ["ag1", "ag2"]},
            )
        ]
    )


@pytest.fixture()
def data_plane(query_response):
    mock = MagicMock()
    mock.delete_by_id_prefix = AsyncMock()
    mock.upsert_in_batches = AsyncMock()
    mock.query = AsyncMock(return_value=query_response)
    return mock


@pytest.fixture()
def external_index_manager(data_plane):
    mock = MagicMock()
    mock.data_plane.return_value = data_plane
    with unittest.mock.patch(
        "nucliadb.common.external_index_providers.pinecone.get_pinecone", return_value=mock
    ):
        return PineconeIndexManager(
            kbid="kbid",
            api_key="api_key",
            index_host="index_host",
            upsert_parallelism=3,
            delete_parallelism=2,
            upsert_timeout=10,
            delete_timeout=10,
        )


async def test_delete_resource(external_index_manager: PineconeIndexManager, data_plane):
    await external_index_manager._delete_resource("resource_uuid")
    data_plane.delete_by_id_prefix.assert_awaited_once()
    resource_uuid = data_plane.delete_by_id_prefix.call_args[1]["id_prefix"]
    assert resource_uuid == "resource_uuid"


async def test_index_resource(external_index_manager: PineconeIndexManager, data_plane):
    index_data = PBResourceBrain()
    index_data.texts["f/field"].text = "some text"
    index_data.texts["f/field"].labels.append("/t/text/label")
    index_data.paragraphs_to_delete.append("pid-foobar")
    index_data.paragraphs_to_delete.append("rid/f/field/0-10")
    index_data.labels.extend(["/e/PERSON/John Doe", "/e/ORG/ACME", "/n/s/PROCESSED", "/t/private"])
    index_data.security.access_groups.extend(["ag1", "ag2"])
    index_paragraphs = IndexParagraphs()
    index_paragraph = IndexParagraph()
    index_paragraph.sentences["rid/f/field/0/0-10"].vector.extend([1, 2, 3])
    index_paragraphs.paragraphs["rid/f/field/0-10"].CopyFrom(index_paragraph)
    index_data.paragraphs["f/field"].CopyFrom(index_paragraphs)

    await external_index_manager._index_resource("resource_uuid", index_data)
    data_plane.upsert_in_batches.assert_awaited_once()
    vectors = data_plane.upsert_in_batches.call_args[1]["vectors"]
    assert len(vectors) == 1
    vector = vectors[0]
    assert isinstance(vector, Vector)
    assert vector.id == "rid/f/field/0/0-10"
    assert vector.values == [1, 2, 3]
    metadata = vector.metadata
    assert "labels" in metadata
    labels = vector.metadata["labels"]
    # Check that ignored labels are not present
    assert "/e/PERSON/John Doe" not in labels
    assert "/e/ORG/ACME" not in labels
    assert "/n/s/PROCESSED" not in labels
    assert sorted(labels) == ["/t/private", "/t/text/label"]
    assert "access_groups" in metadata
    access_groups = vector.metadata["access_groups"]
    assert sorted(access_groups) == ["ag1", "ag2"]


async def test_query(external_index_manager: PineconeIndexManager, data_plane):
    search_request = nodereader_pb2.SearchRequest()
    search_request.vector.extend([1, 2, 3])
    search_request.result_per_page = 20
    search_request.page_number = 0
    query_results = await external_index_manager.query(search_request)
    data_plane.query.assert_awaited_once()
    top_k = data_plane.query.call_args[1]["top_k"]
    assert top_k == 20
    assert isinstance(query_results, PineconeQueryResults)
    assert len(query_results.results.matches) == 1


def test_iter_matching_text_blocks(query_response):
    query_results = PineconeQueryResults(results=query_response)
    text_blocks = list(query_results.iter_matching_text_blocks())
    assert len(text_blocks) == 1
    text_block = text_blocks[0]
    assert text_block.id == "rid/f/field/0/0-10"
    assert text_block.score == 0.8
    assert text_block.order == 0
    assert text_block.text is None
    assert text_block.index == 0
    assert text_block.field_id == "rid/f/field"
    assert text_block.resource_id == "rid"
    assert text_block.position_end == 10
    assert text_block.position_start == 0
    assert text_block.subfield_id is None


def test_convert_timestamp_filter():
    req = nodereader_pb2.SearchRequest()
    utcnow = datetime.datetime.now(datetime.timezone.utc)
    utcnow_as_int = int(utcnow.timestamp())
    req.timestamps.from_created.FromDatetime(utcnow)
    req.timestamps.to_created.FromDatetime(utcnow)
    req.timestamps.from_modified.FromDatetime(utcnow)
    req.timestamps.to_modified.FromDatetime(utcnow)
    terms = convert_timestamp_filter(req.timestamps)
    assert len(terms) == 4
    assert terms[0] == {"date_modified": {"$gte": utcnow_as_int}}
    assert terms[1] == {"date_modified": {"$lte": utcnow_as_int}}
    assert terms[2] == {"date_created": {"$gte": utcnow_as_int}}
    assert terms[3] == {"date_created": {"$lte": utcnow_as_int}}


@pytest.mark.parametrize(
    "expression, converted",
    [
        # Simple case: literal
        ({"literal": "foo"}, {"paragraph_labels": {"$in": ["foo"]}}),
        ({"not": {"literal": "foo"}}, {"paragraph_labels": {"$nin": ["foo"]}}),
        # Simple case: and
        (
            {"and": [{"literal": "foo"}, {"literal": "bar"}]},
            {"$and": [{"paragraph_labels": {"$in": ["foo"]}}, {"paragraph_labels": {"$in": ["bar"]}}]},
        ),
        (
            {"and": [{"literal": "foo"}, {"not": {"literal": "bar"}}]},
            {"$and": [{"paragraph_labels": {"$in": ["foo"]}}, {"paragraph_labels": {"$nin": ["bar"]}}]},
        ),
        # Simple case: or
        (
            {"or": [{"literal": "foo"}, {"literal": "bar"}]},
            {"$or": [{"paragraph_labels": {"$in": ["foo"]}}, {"paragraph_labels": {"$in": ["bar"]}}]},
        ),
        (
            {"or": [{"literal": "foo"}, {"not": {"literal": "bar"}}]},
            {"$or": [{"paragraph_labels": {"$in": ["foo"]}}, {"paragraph_labels": {"$nin": ["bar"]}}]},
        ),
        # And/or negated
        (
            {"not": {"and": [{"literal": "foo"}, {"literal": "bar"}]}},
            {"$or": [{"paragraph_labels": {"$nin": ["foo"]}}, {"paragraph_labels": {"$nin": ["bar"]}}]},
        ),
        (
            {"not": {"or": [{"literal": "foo"}, {"literal": "bar"}]}},
            {"$and": [{"paragraph_labels": {"$nin": ["foo"]}}, {"paragraph_labels": {"$nin": ["bar"]}}]},
        ),
    ],
)
def test_convert_label_filter_expression(expression, converted):
    assert convert_label_filter_expression("paragraph_labels", expression) == converted


def test_convert_to_pinecone_filter():
    request = nodereader_pb2.SearchRequest()
    filter_expression = json.dumps({"literal": "/t/text/label"})
    request.filter.field_labels.append("/t/text/label")
    request.filter.expression = filter_expression

    utcnow = datetime.datetime.now(datetime.timezone.utc)
    request.timestamps.from_created.FromDatetime(utcnow)

    request.key_filters.append("rid")
    request.security.access_groups.extend(["ag1", "ag2"])

    filters = convert_to_pinecone_filter(request)
    assert "$and" in filters
    and_terms = filters["$and"]
    assert len(and_terms) == 4
    label_filter_term = and_terms[0]
    assert label_filter_term == {"field_labels": {"$in": ["/t/text/label"]}}
    timestamp_filter_term = and_terms[1]
    assert timestamp_filter_term == {"date_created": {"$gte": int(utcnow.timestamp())}}
    key_filter_term = and_terms[2]
    assert key_filter_term == {"rid": {"$in": ["rid"]}}
    access_groups_term = and_terms[3]
    assert access_groups_term == {
        "$or": [
            {"security_public": {"$eq": True}},
            {"security_ids_with_access": {"$in": ["ag1", "ag2"]}},
        ]
    }
