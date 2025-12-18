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
import uuid
from typing import Any
from unittest.mock import patch

import pytest

import nucliadb_models.labels
from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.models.internal import retrieval as retrieval_models
from nucliadb.search.search.chat.fetcher import RAOFetcher
from nucliadb.search.search.chat.parser import RAOFindParser
from nucliadb_models.common import FieldTypeName
from nucliadb_models.filters import (
    And,
    DateCreated,
    DateModified,
    Field,
    FilterExpression,
    Keyword,
    Label,
    Not,
    Or,
    Resource,
)
from nucliadb_models.labels import KnowledgeBoxLabels, LabelSet, LabelSetKind
from nucliadb_models.search import FindRequest

now = datetime.datetime.now()
rid = uuid.uuid4().hex


@pytest.mark.parametrize(
    "filters,expression",
    [
        # label filters: mixture of many kind of filters
        (
            {"filters": ["/l/films"]},  # resource classification
            FilterExpression(field=Label(labelset="films")),
        ),
        (
            {"filters": ["/l/landscapes/mountain"]},  # paragraph classification
            FilterExpression(paragraph=Label(labelset="landscapes", label="mountain")),
        ),
        (
            {"filters": ["/l/films/horror", "/l/landscapes/beach"]},
            FilterExpression(
                field=Label(
                    labelset="films",
                    label="horror",
                ),
                paragraph=Label(
                    labelset="landscapes",
                    label="beach",
                ),
            ),
        ),
        (
            {
                "filters": [
                    "/l/films/horror",
                    "/l/films/romantic",
                    "/l/landscapes/beach",
                    "/l/landscapes/desert",
                ]
            },
            FilterExpression(
                field=And(
                    operands=[
                        Label(
                            labelset="films",
                            label="horror",
                        ),
                        Label(
                            labelset="films",
                            label="romantic",
                        ),
                    ]
                ),
                paragraph=And(
                    operands=[
                        Label(
                            labelset="landscapes",
                            label="beach",
                        ),
                        Label(
                            labelset="landscapes",
                            label="desert",
                        ),
                    ]
                ),
            ),
        ),
        (
            {
                "filters": [
                    {
                        "all": [
                            "/l/films/horror",
                            "/l/films/romantic",
                            "/l/landscapes/beach",
                            "/l/landscapes/desert",
                        ]
                    }
                ]
            },
            FilterExpression(
                field=And(
                    operands=[
                        Label(
                            labelset="films",
                            label="horror",
                        ),
                        Label(
                            labelset="films",
                            label="romantic",
                        ),
                    ]
                ),
                paragraph=And(
                    operands=[
                        Label(
                            labelset="landscapes",
                            label="beach",
                        ),
                        Label(
                            labelset="landscapes",
                            label="desert",
                        ),
                    ]
                ),
            ),
        ),
        (
            {"filters": [{"any": ["/l/films/horror", "/l/films/romantic"]}]},
            FilterExpression(
                field=Or(
                    operands=[
                        Label(
                            labelset="films",
                            label="horror",
                        ),
                        Label(
                            labelset="films",
                            label="romantic",
                        ),
                    ]
                ),
            ),
        ),
        (
            {"filters": [{"none": ["/l/films/horror", "/l/films/romantic"]}]},
            FilterExpression(
                field=And(
                    operands=[
                        Not(
                            operand=Label(
                                labelset="films",
                                label="horror",
                            )
                        ),
                        Not(
                            operand=Label(
                                labelset="films",
                                label="romantic",
                            )
                        ),
                    ]
                ),
            ),
        ),
        (
            {"filters": [{"not_all": ["/l/films/horror", "/l/films/romantic"]}]},
            FilterExpression(
                field=Or(
                    operands=[
                        Not(
                            operand=Label(
                                labelset="films",
                                label="horror",
                            )
                        ),
                        Not(
                            operand=Label(
                                labelset="films",
                                label="romantic",
                            )
                        ),
                    ]
                ),
            ),
        ),
        # time filters: creation/modification dates
        (
            {"range_creation_start": now},
            FilterExpression(
                field=DateCreated(
                    since=now,
                    until=None,
                )
            ),
        ),
        (
            {"range_creation_end": now},
            FilterExpression(
                field=DateCreated(
                    since=None,
                    until=now,
                )
            ),
        ),
        (
            {"range_modification_start": now},
            FilterExpression(
                field=DateModified(
                    since=now,
                    until=None,
                )
            ),
        ),
        (
            {"range_modification_end": now},
            FilterExpression(
                field=DateModified(
                    since=None,
                    until=now,
                )
            ),
        ),
        (
            {
                "range_creation_start": now,
                "range_creation_end": now,
                "range_modification_start": now,
                "range_modification_end": now,
            },
            FilterExpression(
                field=And(
                    operands=[
                        DateCreated(
                            since=now,
                            until=now,
                        ),
                        DateModified(
                            since=now,
                            until=now,
                        ),
                    ]
                )
            ),
        ),
        # resource filters: resource, file type and file name
        (
            {
                "resource_filters": [rid],
            },
            FilterExpression(field=Resource(id=rid)),
        ),
        (
            {
                "resource_filters": [f"{rid}/f"],
            },
            FilterExpression(
                field=And(
                    operands=[
                        Resource(id=rid),
                        Field(type=FieldTypeName.FILE, name=None),
                    ]
                )
            ),
        ),
        (
            {
                "resource_filters": [f"{rid}/f/myfile"],
            },
            FilterExpression(
                field=And(
                    operands=[
                        Resource(id=rid),
                        Field(type=FieldTypeName.FILE, name="myfile"),
                    ]
                )
            ),
        ),
        (
            {
                "resource_filters": [f"{rid}/f/myfile", f"{rid}/t/mytext"],
            },
            FilterExpression(
                field=Or(
                    operands=[
                        And(
                            operands=[
                                Resource(id=rid),
                                Field(type=FieldTypeName.FILE, name="myfile"),
                            ]
                        ),
                        And(
                            operands=[
                                Resource(id=rid),
                                Field(type=FieldTypeName.TEXT, name="mytext"),
                            ]
                        ),
                    ]
                )
            ),
        ),
        # field filters: field type and name
        (
            {
                "fields": ["c"],
            },
            FilterExpression(field=Field(type=FieldTypeName.CONVERSATION, name=None)),
        ),
        (
            {
                "fields": ["a/title"],
            },
            FilterExpression(field=Field(type=FieldTypeName.GENERIC, name="title")),
        ),
        (
            {
                "fields": ["u", "a/title"],
            },
            FilterExpression(
                field=Or(
                    operands=[
                        Field(type=FieldTypeName.LINK, name=None),
                        Field(type=FieldTypeName.GENERIC, name="title"),
                    ]
                )
            ),
        ),
        # keyword filters: words appearing in docs
        (
            {
                "keyword_filters": ["myword"],
            },
            FilterExpression(
                field=Keyword(word="myword"),
            ),
        ),
        (
            {
                "keyword_filters": ["hello", "world"],
            },
            FilterExpression(
                field=And(
                    operands=[
                        Keyword(word="hello"),
                        Keyword(word="world"),
                    ]
                )
            ),
        ),
        (
            {"keyword_filters": [{"all": ["hello", "world"]}]},
            FilterExpression(
                field=And(
                    operands=[
                        Keyword(word="hello"),
                        Keyword(word="world"),
                    ]
                )
            ),
        ),
        (
            {"keyword_filters": [{"any": ["hello", "world"]}]},
            FilterExpression(
                field=Or(
                    operands=[
                        Keyword(word="hello"),
                        Keyword(word="world"),
                    ]
                )
            ),
        ),
        (
            {"keyword_filters": [{"none": ["hello", "world"]}]},
            FilterExpression(
                field=Not(
                    operand=Or(
                        operands=[
                            Keyword(word="hello"),
                            Keyword(word="world"),
                        ]
                    )
                )
            ),
        ),
        (
            {"keyword_filters": [{"not_all": ["hello", "world"]}]},
            FilterExpression(
                field=Not(
                    operand=And(
                        operands=[
                            Keyword(word="hello"),
                            Keyword(word="world"),
                        ]
                    )
                )
            ),
        ),
    ],
)
async def test_old_filters_parsing(
    kb_labelsets: KnowledgeBoxLabels,
    filters: dict[str, Any],
    expression: FilterExpression,
):
    item = FindRequest(query="query", **filters)
    fetcher = RAOFetcher(
        "kbid",
        query=item.query,
        user_vector=item.vector,
        vectorset=item.vectorset,
        rephrase=item.rephrase,
        rephrase_prompt=item.rephrase_prompt,
        generative_model=item.generative_model,
        query_image=item.query_image,
    )
    parser = RAOFindParser("kbid", item, fetcher)
    parser._query = retrieval_models.Query()

    with patch("nucliadb.search.search.chat.fetcher.rpc.labelsets", return_value=kb_labelsets):
        parsed_filters = await parser._parse_filters()
        assert parsed_filters.filter_expression == expression


@pytest.mark.parametrize(
    "filters,error",
    [
        # can't use paragraph labels with any/none/not_all
        ({"filters": [{"any": ["/l/landscapes/beach", "/l/landscapes/desert"]}]}, None),
        ({"filters": [{"none": ["/l/landscapes/beach", "/l/landscapes/desert"]}]}, None),
        ({"filters": [{"not_all": ["/l/landscapes/beach", "/l/landscapes/desert"]}]}, None),
        (
            {"resource_filters": ["not-a-uuid"]},
            r"^Invalid query. Error in resource_filters: Value error, resource id filter 'not-a-uuid' should be a valid UUID$",
        ),
    ],
)
async def test_old_filters_parsing_invalid_combinations(
    kb_labelsets: KnowledgeBoxLabels,
    filters: dict[str, Any],
    error: str | None,
):
    item = FindRequest(query="query", **filters)
    fetcher = RAOFetcher(
        "kbid",
        query=item.query,
        user_vector=item.vector,
        vectorset=item.vectorset,
        rephrase=item.rephrase,
        rephrase_prompt=item.rephrase_prompt,
        generative_model=item.generative_model,
        query_image=item.query_image,
    )
    parser = RAOFindParser("kbid", item, fetcher)
    parser._query = retrieval_models.Query()

    with patch("nucliadb.search.search.chat.fetcher.rpc.labelsets", return_value=kb_labelsets):
        with pytest.raises(InvalidQueryError, match=error):
            await parser._parse_filters()


@pytest.fixture
def kb_labelsets():
    yield KnowledgeBoxLabels(
        uuid="kbid",
        labelsets={
            "films": LabelSet(
                title="Films",
                kind=[LabelSetKind.RESOURCES],
                labels=[
                    nucliadb_models.labels.Label(title="horror"),
                    nucliadb_models.labels.Label(title="romantic"),
                    nucliadb_models.labels.Label(title="comedy"),
                ],
            ),
            "landscapes": LabelSet(
                title="Landscapes",
                kind=[LabelSetKind.SENTENCES, LabelSetKind.PARAGRAPHS],
                labels=[
                    nucliadb_models.labels.Label(title="mountain"),
                    nucliadb_models.labels.Label(title="beach"),
                    nucliadb_models.labels.Label(title="desert"),
                ],
            ),
        },
    )
