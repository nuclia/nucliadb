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
import unittest
from unittest.mock import AsyncMock, MagicMock

import pytest
from nidx_protos import nodereader_pb2
from nidx_protos.nodereader_pb2 import FilterExpression
from nidx_protos.noderesources_pb2 import IndexParagraph, IndexParagraphs
from nidx_protos.noderesources_pb2 import Resource as PBResourceBrain

from nucliadb.common.external_index_providers.exceptions import ExternalIndexCreationError
from nucliadb.common.external_index_providers.pinecone import (
    PineconeIndexManager,
    PineconeQueryResults,
    VectorMetadata,
    convert_filter_expression,
    convert_to_pinecone_filter,
)
from nucliadb.common.ids import ParagraphId
from nucliadb_protos.knowledgebox_pb2 import PineconeIndexMetadata
from nucliadb_protos.utils_pb2 import VectorSimilarity
from nucliadb_utils.aiopynecone.models import MAX_INDEX_NAME_LENGTH, QueryResponse, Vector, VectorMatch


@pytest.fixture()
def query_response():
    return QueryResponse(
        matches=[
            VectorMatch(
                id="rid/f/field/0/0-10",
                score=0.8,
                values=None,
                metadata={
                    "rid": "rid",
                    "field_id": "f/field",
                    "field_type": "f",
                    "resource_labels": ["/t/text/label"],
                    "access_groups": ["ag1", "ag2"],
                },
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
def vectorsets():
    return [
        "multilingual-2023-02-02",
        "english-2023-02-02",
    ]


@pytest.fixture()
def external_index_manager(data_plane, vectorsets):
    mock = MagicMock()
    mock.data_plane.return_value = data_plane
    with unittest.mock.patch(
        "nucliadb.common.external_index_providers.pinecone.get_pinecone", return_value=mock
    ):
        multilingual, english = vectorsets
        multilingual_index_name = PineconeIndexManager.get_index_name()
        english_index_name = PineconeIndexManager.get_index_name()
        indexes = {
            multilingual: PineconeIndexMetadata(
                index_name=multilingual_index_name,
                index_host="index1_host",
                vector_dimension=10,
                similarity=VectorSimilarity.COSINE,
            ),
            english: PineconeIndexMetadata(
                index_name=english_index_name,
                index_host="index2_host",
                vector_dimension=10,
                similarity=VectorSimilarity.DOT,
            ),
        }
        return PineconeIndexManager(
            kbid="kbid",
            api_key="api_key",
            indexes=indexes,
            upsert_parallelism=3,
            delete_parallelism=2,
            upsert_timeout=10,
            delete_timeout=10,
            default_vectorset=multilingual,
        )


async def test_delete_resource(external_index_manager: PineconeIndexManager, data_plane):
    await external_index_manager._delete_resource("resource_uuid")
    # The resource is deleted by prefix on every vectorset index
    assert data_plane.delete_by_id_prefix.call_count == 2
    call_0_kwargs = data_plane.delete_by_id_prefix.call_args_list[0][1]
    assert call_0_kwargs["id_prefix"] == "resource_uuid"


async def test_index_resource(external_index_manager: PineconeIndexManager, data_plane, vectorsets):
    multilingual, english = vectorsets
    index_data = PBResourceBrain()
    index_data.texts["f/field"].text = "some text"
    index_data.texts["f/field"].labels.append("/t/text/label")
    index_data.sentences_to_delete.append("invalid-sid")
    index_data.sentences_to_delete.append("rid/f/field/0/0-10")
    index_data.paragraphs_to_delete.append("pid-foobar")
    index_data.paragraphs_to_delete.append("rid/f/field/0-10")
    index_data.labels.extend(["/e/PERSON/John Doe", "/e/ORG/ACME", "/n/s/PROCESSED", "/t/private"])
    index_data.security.access_groups.extend(["ag1", "ag2"])
    index_paragraphs = IndexParagraphs()
    index_paragraph = IndexParagraph()
    # This tests the default index (previous to vectorset changes)
    index_paragraph.sentences["rid/f/field/0/0-10"].vector.extend([1, 2, 3])
    index_paragraph.sentences["rid/f/field/0/0-10"].metadata.page_with_visual = False
    # Add at least one vector on a vectorset with a different dimension
    index_paragraph.vectorsets_sentences[english].sentences["rid/f/field/0/0-10"].vector.extend(
        [5, 6, 7, 8]
    )
    index_paragraph.vectorsets_sentences[english].sentences[
        "rid/f/field/0/0-10"
    ].metadata.page_with_visual = True
    index_paragraphs.paragraphs["rid/f/field/0-10"].CopyFrom(index_paragraph)
    index_data.paragraphs["f/field"].CopyFrom(index_paragraphs)

    await external_index_manager._index_resource("resource_uuid", index_data)

    # One upsert for every vectorset in the index data
    assert data_plane.upsert_in_batches.call_count == 2

    # Check that the upsert for every vectorset holds the corresponding vectors and metadata
    call_0_kwargs = data_plane.upsert_in_batches.call_args_list[0][1]
    vectors = call_0_kwargs["vectors"]
    assert len(vectors) == 1
    vector = vectors[0]
    assert isinstance(vector, Vector)
    assert vector.id == "rid/f/field/0/0-10"
    assert vector.values == [1, 2, 3]
    metadata = vector.metadata
    vmetadata = VectorMetadata.model_validate(metadata)
    assert set(vmetadata.field_labels) == {  # type: ignore
        "/t/private",
        "/t/text/label",
    }
    assert not vmetadata.security_public
    assert set(vmetadata.security_ids_with_access) == {"ag1", "ag2"}  # type: ignore
    assert vmetadata.page_with_visual is None

    call_1_kwargs = data_plane.upsert_in_batches.call_args_list[1][1]
    vectors = call_1_kwargs["vectors"]
    assert len(vectors) == 1
    vector = vectors[0]
    assert isinstance(vector, Vector)
    assert vector.id == "rid/f/field/0/0-10"
    assert vector.values == [5, 6, 7, 8]
    metadata = vector.metadata
    vmetadata = VectorMetadata.model_validate(metadata)
    assert set(vmetadata.field_labels) == {  # type: ignore
        "/t/private",
        "/t/text/label",
    }
    assert not vmetadata.security_public
    assert set(vmetadata.security_ids_with_access) == {"ag1", "ag2"}  # type: ignore
    assert vmetadata.page_with_visual is True


async def test_query(external_index_manager: PineconeIndexManager, data_plane):
    search_request = nodereader_pb2.SearchRequest()
    search_request.vector.extend([1, 2, 3])
    search_request.result_per_page = 20
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
    assert text_block.paragraph_id == ParagraphId.from_string("rid/f/field/0-10")
    assert text_block.score == 0.8
    assert text_block.order == 0
    assert text_block.text is None
    assert text_block.position.index == 0
    assert text_block.position.start == 0
    assert text_block.position.end == 10


def test_convert_timestamp_filter():
    utcnow = datetime.datetime.now(datetime.timezone.utc)
    utcnow_as_int = int(utcnow.timestamp())

    created = FilterExpression()
    created.date.field = FilterExpression.DateRangeFilter.DateField.CREATED
    created.date.since.FromDatetime(utcnow)
    created.date.until.FromDatetime(utcnow)

    modified = FilterExpression()
    modified.date.field = FilterExpression.DateRangeFilter.DateField.MODIFIED
    modified.date.since.FromDatetime(utcnow)
    modified.date.until.FromDatetime(utcnow)

    expr = FilterExpression()
    expr.bool_and.operands.append(created)
    expr.bool_and.operands.append(modified)

    terms = convert_filter_expression("field_labels", expr)
    assert terms == {
        "$and": [
            {
                "$and": [
                    {"date_created": {"$gte": utcnow_as_int}},
                    {"date_created": {"$lte": utcnow_as_int}},
                ]
            },
            {
                "$and": [
                    {"date_modified": {"$gte": utcnow_as_int}},
                    {"date_modified": {"$lte": utcnow_as_int}},
                ]
            },
        ]
    }


@pytest.mark.parametrize(
    "expression, converted",
    [
        # Simple case: literal
        (
            FilterExpression(facet=FilterExpression.FacetFilter(facet="foo")),
            {"paragraph_labels": {"$in": ["foo"]}},
        ),
        (
            FilterExpression(bool_not=FilterExpression(facet=FilterExpression.FacetFilter(facet="foo"))),
            {"paragraph_labels": {"$nin": ["foo"]}},
        ),
        # Simple case: and
        (
            FilterExpression(
                bool_and=FilterExpression.FilterExpressionList(
                    operands=[
                        FilterExpression(facet=FilterExpression.FacetFilter(facet="foo")),
                        FilterExpression(facet=FilterExpression.FacetFilter(facet="bar")),
                    ]
                )
            ),
            {"$and": [{"paragraph_labels": {"$in": ["foo"]}}, {"paragraph_labels": {"$in": ["bar"]}}]},
        ),
        (
            FilterExpression(
                bool_and=FilterExpression.FilterExpressionList(
                    operands=[
                        FilterExpression(facet=FilterExpression.FacetFilter(facet="foo")),
                        FilterExpression(
                            bool_not=FilterExpression(facet=FilterExpression.FacetFilter(facet="bar"))
                        ),
                    ]
                )
            ),
            {"$and": [{"paragraph_labels": {"$in": ["foo"]}}, {"paragraph_labels": {"$nin": ["bar"]}}]},
        ),
        # Simple case: or
        (
            FilterExpression(
                bool_or=FilterExpression.FilterExpressionList(
                    operands=[
                        FilterExpression(facet=FilterExpression.FacetFilter(facet="foo")),
                        FilterExpression(facet=FilterExpression.FacetFilter(facet="bar")),
                    ]
                )
            ),
            {"$or": [{"paragraph_labels": {"$in": ["foo"]}}, {"paragraph_labels": {"$in": ["bar"]}}]},
        ),
        (
            FilterExpression(
                bool_or=FilterExpression.FilterExpressionList(
                    operands=[
                        FilterExpression(facet=FilterExpression.FacetFilter(facet="foo")),
                        FilterExpression(
                            bool_not=FilterExpression(facet=FilterExpression.FacetFilter(facet="bar"))
                        ),
                    ]
                )
            ),
            {"$or": [{"paragraph_labels": {"$in": ["foo"]}}, {"paragraph_labels": {"$nin": ["bar"]}}]},
        ),
        # And/or negated
        (
            FilterExpression(
                bool_not=FilterExpression(
                    bool_and=FilterExpression.FilterExpressionList(
                        operands=[
                            FilterExpression(facet=FilterExpression.FacetFilter(facet="foo")),
                            FilterExpression(facet=FilterExpression.FacetFilter(facet="bar")),
                        ]
                    )
                )
            ),
            {"$or": [{"paragraph_labels": {"$nin": ["foo"]}}, {"paragraph_labels": {"$nin": ["bar"]}}]},
        ),
        (
            FilterExpression(
                bool_not=FilterExpression(
                    bool_or=FilterExpression.FilterExpressionList(
                        operands=[
                            FilterExpression(facet=FilterExpression.FacetFilter(facet="foo")),
                            FilterExpression(facet=FilterExpression.FacetFilter(facet="bar")),
                        ]
                    )
                )
            ),
            {"$and": [{"paragraph_labels": {"$nin": ["foo"]}}, {"paragraph_labels": {"$nin": ["bar"]}}]},
        ),
    ],
)
def test_convert_label_filter_expression(expression, converted):
    assert convert_filter_expression("paragraph_labels", expression) == converted


def test_convert_to_pinecone_filter():
    request = nodereader_pb2.SearchRequest()

    # Label filter
    label_filter = FilterExpression()
    label_filter.facet.facet = "/t/text/label"
    request.field_filter.bool_and.operands.append(label_filter)

    # Field filter
    ftitle_filter = FilterExpression()
    ftitle_filter.field.field_type = "a"
    ftitle_filter.field.field_id = "title"

    ftext_filter = FilterExpression()
    ftext_filter.field.field_type = "t"
    ftext_filter.field.field_id = "text"

    fields_filter = FilterExpression()
    fields_filter.bool_or.operands.append(ftitle_filter)
    fields_filter.bool_or.operands.append(ftext_filter)
    request.field_filter.bool_and.operands.append(fields_filter)

    # Date filter
    utcnow = datetime.datetime.now(datetime.timezone.utc)
    date_filter = FilterExpression()
    date_filter.date.field = FilterExpression.DateRangeFilter.DateField.CREATED
    date_filter.date.since.FromDatetime(utcnow)
    request.field_filter.bool_and.operands.append(date_filter)

    # Resource filter
    rid_filter = FilterExpression()
    rid_filter.resource.resource_id = "rid"
    request.field_filter.bool_and.operands.append(rid_filter)

    # Security
    request.security.access_groups.extend(["ag1", "ag2"])

    filters = convert_to_pinecone_filter(request)
    assert "$and" in filters
    and_terms = filters["$and"]
    assert len(and_terms) == 2
    assert and_terms[0] == {
        "$and": [
            {"field_labels": {"$in": ["/t/text/label"]}},
            {"$or": [{"field_id": {"$eq": "a/title"}}, {"field_id": {"$eq": "t/text"}}]},
            {"date_created": {"$gte": int(utcnow.timestamp())}},
            {"rid": {"$eq": "rid"}},
        ]
    }
    access_groups_term = and_terms[1]
    assert access_groups_term["$or"][0] == {"security_public": {"$eq": True}}
    access_groups = access_groups_term["$or"][1]["security_ids_with_access"]["$in"]
    assert set(access_groups) == {"ag1", "ag2"}


def test_convert_to_pinecone_filter_empty():
    request = nodereader_pb2.SearchRequest()
    filters = convert_to_pinecone_filter(request)
    assert filters is None


def test_exceptions():
    ExternalIndexCreationError("pinecone", "foo")


def test_get_index_name():
    computed_index_name = PineconeIndexManager.get_index_name()
    assert computed_index_name.startswith("nuclia-")
    assert len(computed_index_name) <= MAX_INDEX_NAME_LENGTH
