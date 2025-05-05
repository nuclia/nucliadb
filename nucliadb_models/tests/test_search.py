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
        fields={"f/myfield"},
    )

    # not a set of fields
    with pytest.raises(ValidationError):
        search.FieldExtensionStrategy(
            name="field_extension",
            fields=0,
        )

    # fields must be in the format {field_type}/{field_name}
    with pytest.raises(ValidationError):
        search.FieldExtensionStrategy(
            name="field_extension",
            fields={"myfield"},
        )

    # not an allowed field
    with pytest.raises(ValidationError):
        search.FieldExtensionStrategy(
            name="field_extension",
            fields={"z/myfield"},
        )


def test_find_request_fulltext_feature_not_allowed():
    with pytest.raises(ValidationError):
        search.FindRequest(features=[search.SearchOptions.FULLTEXT])


def test_find_supports_search_options():
    search.FindRequest(features=[search.SearchOptions.KEYWORD])
    search.FindRequest(features=[search.SearchOptions.SEMANTIC])
    search.FindRequest(features=[search.SearchOptions.RELATIONS])


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
        search.FindRequest(rank_fusion="unknown")
    with pytest.raises(ValueError):
        search.AskRequest(query="q", rank_fusion="unknown")


def test_legacy_rank_fusion_fix():
    req = search.FindRequest(rank_fusion="legacy")
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
