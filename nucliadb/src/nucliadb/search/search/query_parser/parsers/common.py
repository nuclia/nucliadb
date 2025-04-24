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
import re
import string
from typing import Optional, Union

from nucliadb.search import logger
from nucliadb.search.search.query_parser.exceptions import InvalidQueryError
from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb.search.search.query_parser.models import (
    KeywordQuery,
    SemanticQuery,
)
from nucliadb_models import search as search_models

DEFAULT_GENERIC_SEMANTIC_THRESHOLD = 0.7

# -* is an invalid query in tantivy and it won't return results but if you add some whitespaces
# between - and *, it will actually trigger a tantivy bug and panic
INVALID_QUERY = re.compile(r"- +\*")


def validate_query_syntax(query: str):
    # Filter some queries that panic tantivy, better than returning the 500
    if INVALID_QUERY.search(query):
        raise InvalidQueryError("query", "Invalid query syntax")


def is_empty_query(request: search_models.BaseSearchRequest) -> bool:
    return len(request.query) == 0


def has_user_vectors(request: search_models.BaseSearchRequest) -> bool:
    return request.vector is not None and len(request.vector) > 0


def is_exact_match_only_query(request: search_models.BaseSearchRequest) -> bool:
    """
    '"something"' -> True
    'foo "something" else' -> False
    """
    query = request.query.strip()
    return len(query) > 0 and query.startswith('"') and query.endswith('"')


def should_disable_vector_search(request: search_models.BaseSearchRequest) -> bool:
    if has_user_vectors(request):
        return False

    if is_exact_match_only_query(request):
        return True

    if is_empty_query(request):
        return True

    return False


def parse_top_k(item: search_models.BaseSearchRequest) -> int:
    assert item.top_k is not None, "top_k must have an int value"
    top_k = item.top_k
    return top_k


async def parse_keyword_query(
    item: search_models.BaseSearchRequest,
    *,
    fetcher: Fetcher,
) -> KeywordQuery:
    query = item.query
    is_synonyms_query = False

    if item.with_synonyms:
        synonyms_query = await query_with_synonyms(query, fetcher=fetcher)
        if synonyms_query is not None:
            query = synonyms_query
            is_synonyms_query = True

    min_score = parse_keyword_min_score(item.min_score)

    return KeywordQuery(
        query=query,
        is_synonyms_query=is_synonyms_query,
        min_score=min_score,
    )


async def parse_semantic_query(
    item: Union[search_models.SearchRequest, search_models.FindRequest],
    *,
    fetcher: Fetcher,
) -> SemanticQuery:
    vectorset = await fetcher.get_vectorset()
    query = await fetcher.get_query_vector()

    min_score = await parse_semantic_min_score(item.min_score, fetcher=fetcher)

    return SemanticQuery(query=query, vectorset=vectorset, min_score=min_score)


def parse_keyword_min_score(
    min_score: Optional[Union[float, search_models.MinScore]],
) -> float:
    # Keep backward compatibility with the deprecated min_score payload
    # parameter being a float (specifying semantic)
    if min_score is None or isinstance(min_score, float):
        return 0.0
    else:
        return min_score.bm25


async def parse_semantic_min_score(
    min_score: Optional[Union[float, search_models.MinScore]],
    *,
    fetcher: Fetcher,
):
    if min_score is None:
        min_score = None
    elif isinstance(min_score, float):
        min_score = min_score
    else:
        min_score = min_score.semantic

    if min_score is None:
        # min score not defined by the user, we'll try to get the default
        # from Predict API
        min_score = await fetcher.get_semantic_min_score()
        if min_score is None:
            logger.warning(
                "Semantic threshold not found in query information, using default",
                extra={"kbid": fetcher.kbid},
            )
            min_score = DEFAULT_GENERIC_SEMANTIC_THRESHOLD

    return min_score


async def query_with_synonyms(
    query: str,
    *,
    fetcher: Fetcher,
) -> Optional[str]:
    """
    Replace the terms in the query with an expression that will make it match with the configured synonyms.
    We're using the Tantivy's query language here: https://docs.rs/tantivy/latest/tantivy/query/struct.QueryParser.html

    Example:
    - Synonyms: Foo -> Bar, Baz
    - Query: "What is Foo?"
    - Advanced Query: "What is (Foo OR Bar OR Baz)?"
    """
    if not query:
        return None

    synonyms = await fetcher.get_synonyms()
    if synonyms is None:
        # No synonyms found
        return None

    # Calculate term variants: 'term' -> '(term OR synonym1 OR synonym2)'
    variants: dict[str, str] = {}
    for term, term_synonyms in synonyms.terms.items():
        if len(term_synonyms.synonyms) > 0:
            variants[term] = "({})".format(" OR ".join([term] + list(term_synonyms.synonyms)))

    # Split the query into terms
    query_terms = query.split()

    # Remove punctuation from the query terms
    clean_query_terms = [term.strip(string.punctuation) for term in query_terms]

    # Replace the original terms with the variants if the cleaned term is in the variants
    term_with_synonyms_found = False
    for index, clean_term in enumerate(clean_query_terms):
        if clean_term in variants:
            term_with_synonyms_found = True
            query_terms[index] = query_terms[index].replace(clean_term, variants[clean_term])

    if term_with_synonyms_found:
        advanced_query = " ".join(query_terms)
        return advanced_query

    return None
