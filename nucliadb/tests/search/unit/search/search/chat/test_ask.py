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


from nucliadb.search.search.chat.ask import calculate_prequeries_for_json_schema
from nucliadb_models.search import AskRequest, ChatOptions, SearchOptions


def test_calculate_prequeries_for_json_schema():
    ask_request = AskRequest(
        query="",
        min_score=0.1,
        features=[ChatOptions.KEYWORD],
        answer_json_schema={
            "name": "book_ordering",
            "description": "Structured answer for a book to order",
            "parameters": {
                "type": "object",
                "properties": {
                    "title": {"type": "string", "description": "The title of the book"},
                    "author": {"type": "string", "description": "The author of the book"},
                    "ref_num": {"type": "string", "description": "The ISBN of the book"},
                    "price": {"type": "number", "description": "The price of the book"},
                },
                "required": ["title", "author", "ref_num", "price"],
            },
        },
    )
    prequeries = calculate_prequeries_for_json_schema(ask_request)
    assert prequeries is not None
    assert len(prequeries.queries) == 4

    # Check that the queries have the expected values
    assert prequeries.queries[0].request.query == "title: The title of the book"
    assert prequeries.queries[1].request.query == "author: The author of the book"
    assert prequeries.queries[2].request.query == "ref_num: The ISBN of the book"
    assert prequeries.queries[3].request.query == "price: The price of the book"

    # All queries should have the same weight
    assert prequeries.main_query_weight == 1.0
    for preq in prequeries.queries:
        assert preq.weight == 1.0
        assert preq.request.features == [SearchOptions.KEYWORD]
        assert preq.request.rephrase is False
        assert preq.request.highlight is False
        # Only the top-10 results are requested for each query
        assert preq.request.page_size == 10
        assert preq.request.page_number == 0
        assert preq.request.show == []
        # Min score is propagated from the main query
        assert preq.request.min_score == 0.1
