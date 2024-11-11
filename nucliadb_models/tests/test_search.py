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


@pytest.mark.filterwarnings("ignore:deprecated")
def test_base_search_request_top_k():
    request = search.BaseSearchRequest(
        page_number=10,
        page_size=20,
    )
    assert request.page_number == 10
    assert request.page_size == 20

    request = search.BaseSearchRequest(
        top_k=100,
    )
    assert request.page_number == 0
    assert request.page_size == 100

    request = search.BaseSearchRequest(
        page_number=10,
        page_size=20,
        top_k=100,
    )
    assert request.page_number == 0
    assert request.page_size == 100


def test_find_request_fulltext_feature_not_allowed():
    with pytest.raises(ValidationError):
        search.FindRequest(features=[search.SearchOptions.FULLTEXT])


# Rank fusion


@pytest.mark.parametrize(
    "rank_fusion,expected",
    [
        ("legacy", search.RankFusionName.LEGACY),
        ("rrf", search.RankFusionName.RECIPROCAL_RANK_FUSION),
        (search.RankFusionName.LEGACY, search.RankFusionName.LEGACY),
        (search.RankFusionName.RECIPROCAL_RANK_FUSION, search.RankFusionName.RECIPROCAL_RANK_FUSION),
        (search.LegacyRankFusion(), search.LegacyRankFusion()),
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
