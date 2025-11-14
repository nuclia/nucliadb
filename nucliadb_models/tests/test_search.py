# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pytest
from pydantic_core import ValidationError

from nucliadb_models import search


def test_filter_model_validator():
    search.Filter(none=["c"])

    # can only set one of: all, any, none or not_all
    with pytest.raises(ValidationError):
        search.Filter(all=["a"], any=["b"])


def test_field_extension_strategy_fields_field_validator():
    search.FieldExtensionStrategy(
        name="field_extension",
        fields=["f/myfield"],
    )

    # not a set of fields
    with pytest.raises(ValidationError):
        search.FieldExtensionStrategy(
            name="field_extension",
            fields=0,  # type: ignore
        )

    # fields must be in the format {field_type}/{field_name}
    with pytest.raises(ValidationError):
        search.FieldExtensionStrategy(
            name="field_extension",
            fields=["myfield"],
        )

    # not an allowed field
    with pytest.raises(ValidationError):
        search.FieldExtensionStrategy(
            name="field_extension",
            fields=["z/myfield"],
        )


def test_find_request_fulltext_feature_not_allowed():
    with pytest.raises(ValidationError):
        search.FindRequest(features=[search.SearchOptions.FULLTEXT])  # type: ignore


def test_find_supports_search_options():
    search.FindRequest(features=[search.SearchOptions.KEYWORD])  # type: ignore
    search.FindRequest(features=[search.SearchOptions.SEMANTIC])  # type: ignore
    search.FindRequest(features=[search.SearchOptions.RELATIONS])  # type: ignore


def test_search_semantic_with_offset_not_supported():
    # Semantic without offset is OK
    search.SearchRequest(features=[search.SearchOptions.SEMANTIC])
    # Keyword with offset is OK
    search.SearchRequest(
        features=[search.SearchOptions.KEYWORD, search.SearchOptions.FULLTEXT], offset=1
    )

    # Semantic with offset is not OK
    with pytest.raises(ValidationError):
        search.SearchRequest(features=[search.SearchOptions.SEMANTIC], offset=1)

    # Relations with offset is not OK
    with pytest.raises(ValidationError):
        search.SearchRequest(features=[search.SearchOptions.RELATIONS], offset=1)

    # Default features (all) with offset is not OK
    with pytest.raises(ValidationError):
        search.SearchRequest(offset=1)


def test_search_semantic_with_sorting_not_supported():
    # Semantic with default sort (score) is OK
    search.SearchRequest(features=[search.SearchOptions.SEMANTIC])
    search.SearchRequest(
        features=[search.SearchOptions.SEMANTIC], sort=search.SortOptions(field=search.SortField.SCORE)
    )

    # Semantic with date sorting is not OK
    with pytest.raises(ValidationError):
        search.SearchRequest(
            features=[search.SearchOptions.SEMANTIC],
            sort=search.SortOptions(field=search.SortField.CREATED),
        )

    with pytest.raises(ValidationError):
        search.SearchRequest(
            features=[search.SearchOptions.SEMANTIC],
            sort=search.SortOptions(field=search.SortField.MODIFIED),
        )

    # It is supported in keyword indexes
    search.SearchRequest(
        features=[search.SearchOptions.KEYWORD, search.SearchOptions.FULLTEXT],
        sort=search.SortOptions(field=search.SortField.CREATED),
    )

    # Title sorting is not supported for any index (only catalog)
    with pytest.raises(ValidationError):
        search.SearchRequest(
            features=[search.SearchOptions.KEYWORD],
            sort=search.SortOptions(field=search.SortField.TITLE),
        )


# Rank fusion


@pytest.mark.parametrize(
    "rank_fusion,expected",
    [
        ("rrf", search.RankFusionName.RECIPROCAL_RANK_FUSION),
        (search.RankFusionName.RECIPROCAL_RANK_FUSION, search.RankFusionName.RECIPROCAL_RANK_FUSION),
        (search.ReciprocalRankFusion(), search.ReciprocalRankFusion()),
    ],
)
def test_rank_fusion(rank_fusion, expected):
    req = search.FindRequest(rank_fusion=rank_fusion)
    assert req.rank_fusion == expected
    req = search.AskRequest(query="q", rank_fusion=rank_fusion)
    assert req.rank_fusion == expected


def test_rank_fusion_errors():
    with pytest.raises(ValueError):
        search.FindRequest(rank_fusion="unknown")  # type: ignore
    with pytest.raises(ValueError):
        search.AskRequest(query="q", rank_fusion="unknown")  # type: ignore


def test_legacy_rank_fusion_fix():
    req = search.FindRequest(rank_fusion="legacy")  # type: ignore
    assert req.rank_fusion == "rrf"

    req = search.FindRequest.model_validate({"rank_fusion": "legacy"})
    assert req.rank_fusion == "rrf"


# /ask


def test_ask_rename_context_to_chat_history():
    payload = [search.ChatContextMessage(author=search.Author.USER, text="my context")]

    req = search.AskRequest(query="")
    assert req.context is None
    assert req.chat_history is None

    req = search.AskRequest(query="", context=payload)
    assert req.context is None
    assert req.chat_history == payload

    req = search.AskRequest(query="", chat_history=payload)
    assert req.context is None
    assert req.chat_history == payload

    with pytest.raises(ValidationError):
        search.AskRequest(query="", context=payload, chat_history=payload)
