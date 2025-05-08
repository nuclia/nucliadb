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


from nucliadb.search.search.chat.ask import calculate_prequeries_for_json_schema, compute_best_matches
from nucliadb_models.search import (
    SCORE_TYPE,
    AskRequest,
    ChatOptions,
    FindField,
    FindOptions,
    FindParagraph,
    FindRequest,
    FindResource,
    KnowledgeboxFindResults,
    PreQuery,
)


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
        assert preq.request.features == [FindOptions.KEYWORD]
        assert preq.request.rephrase is False
        assert preq.request.highlight is False
        # Only the top-10 results are requested for each query
        assert preq.request.top_k == 10
        assert preq.request.show == []
        # Min score is propagated from the main query
        assert preq.request.min_score == 0.1


def test_compute_best_matches():
    main_results = KnowledgeboxFindResults(
        resources={
            "main-result": FindResource(
                id="main-result",
                fields={
                    "f/f1": FindField(
                        paragraphs={
                            "main-result/f/f1/0-10": FindParagraph(
                                id="main-result/f/f1/0-10",
                                score=2,
                                score_type=SCORE_TYPE.BM25,
                                order=0,
                                text="First Paragraph text",
                            ),
                            "main-result/f/f1/10-20": FindParagraph(
                                id="main-result/f/f1/10-20",
                                score=1,
                                score_type=SCORE_TYPE.BM25,
                                order=1,
                                text="Second paragraph text",
                            ),
                        }
                    )
                },
            )
        },
    )
    prequery_1 = PreQuery(
        request=FindRequest(query="prequery_1"),
        weight=10,
    )
    prequery_1_results = KnowledgeboxFindResults(
        resources={
            "prequery-1-result": FindResource(
                id="prequery-1-result",
                fields={
                    "f/f1": FindField(
                        paragraphs={
                            "prequery-1-result/f/f1/0-10": FindParagraph(
                                id="prequery-1-result/f/f1/0-10",
                                score=2,
                                score_type=SCORE_TYPE.BM25,
                                order=0,
                                text="First Paragraph text",
                            ),
                            "prequery-1-result/f/f1/10-20": FindParagraph(
                                id="prequery-1-result/f/f1/10-20",
                                score=1,
                                score_type=SCORE_TYPE.BM25,
                                order=1,
                                text="Second paragraph text",
                            ),
                        }
                    )
                },
            )
        },
    )
    prequery_2 = PreQuery(
        request=FindRequest(query="prequery_2"),
        weight=90,
    )
    prequery_2_results = KnowledgeboxFindResults(
        resources={
            "prequery-2-result": FindResource(
                id="prequery-2-result",
                fields={
                    "f/f1": FindField(
                        paragraphs={
                            "prequery-2-result/f/f1/0-10": FindParagraph(
                                id="prequery-2-result/f/f1/0-10",
                                score=2,
                                score_type=SCORE_TYPE.BM25,
                                order=0,
                                text="First Paragraph text",
                            ),
                            "prequery-2-result/f/f1/10-20": FindParagraph(
                                id="prequery-2-result/f/f1/10-20",
                                score=1,
                                score_type=SCORE_TYPE.BM25,
                                order=1,
                                text="Second paragraph text",
                            ),
                        }
                    )
                },
            )
        },
    )
    best_matches = compute_best_matches(
        main_results=main_results,
        prequeries_results=[
            (prequery_1, prequery_1_results),
            (prequery_2, prequery_2_results),
        ],
    )
    assert len(best_matches) == 6
    # The first paragraphs come from the prequery with the highest weight
    assert best_matches[0].paragraph.id == "prequery-2-result/f/f1/0-10"
    assert best_matches[1].paragraph.id == "prequery-2-result/f/f1/10-20"
    # The second paragraphs come from the prequery with the lowest weight
    assert best_matches[2].paragraph.id == "prequery-1-result/f/f1/0-10"
    assert best_matches[3].paragraph.id == "prequery-1-result/f/f1/10-20"
    # The last paragraphs come from the main results
    assert best_matches[4].paragraph.id == "main-result/f/f1/0-10"
    assert best_matches[5].paragraph.id == "main-result/f/f1/10-20"
    # validate score computation
    assert best_matches[0].weighted_score == best_matches[0].paragraph.score * 90 / 101
    assert best_matches[1].weighted_score == best_matches[1].paragraph.score * 90 / 101
    assert best_matches[2].weighted_score == best_matches[2].paragraph.score * 10 / 101
    assert best_matches[3].weighted_score == best_matches[3].paragraph.score * 10 / 101
    assert best_matches[4].weighted_score == best_matches[4].paragraph.score * 1 / 101
    assert best_matches[5].weighted_score == best_matches[5].paragraph.score * 1 / 101

    # Test that the main query weight can be set to a huge
    # value so that the main results are always at the beginning
    best_matches = compute_best_matches(
        main_results=main_results,
        prequeries_results=[
            (prequery_1, prequery_1_results),
            (prequery_2, prequery_2_results),
        ],
        main_query_weight=1000,
    )
    assert len(best_matches) == 6
    assert best_matches[0].paragraph.id == "main-result/f/f1/0-10"
    assert best_matches[1].paragraph.id == "main-result/f/f1/10-20"
    assert best_matches[2].paragraph.id == "prequery-2-result/f/f1/0-10"
    assert best_matches[3].paragraph.id == "prequery-2-result/f/f1/10-20"
    assert best_matches[4].paragraph.id == "prequery-1-result/f/f1/0-10"
    assert best_matches[5].paragraph.id == "prequery-1-result/f/f1/10-20"
    # validate score computation
    assert best_matches[0].weighted_score == best_matches[0].paragraph.score * 1000 / 1100
    assert best_matches[1].weighted_score == best_matches[1].paragraph.score * 1000 / 1100
    assert best_matches[2].weighted_score == best_matches[2].paragraph.score * 90 / 1100
    assert best_matches[3].weighted_score == best_matches[3].paragraph.score * 90 / 1100
    assert best_matches[4].weighted_score == best_matches[4].paragraph.score * 10 / 1100
    assert best_matches[5].weighted_score == best_matches[5].paragraph.score * 10 / 1100
