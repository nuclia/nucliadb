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

from pydantic import BaseModel
from pydantic.json_schema import SkipJsonSchema

from nucliadb_models.common import ParamDefault
from nucliadb_models.filters import FilterExpression
from nucliadb_models.search import SearchParamDefaults, SuggestOptions
from nucliadb_models.security import RequestSecurity

# bw/c with definition in search module
SuggestOptions = SuggestOptions


class SuggestParamDefaults:
    suggest_query = ParamDefault(
        default=..., title="Query", description="The query to get suggestions for"
    )
    suggest_features = ParamDefault(
        default=[
            SuggestOptions.PARAGRAPH,
            SuggestOptions.ENTITIES,
        ],
        title="Suggest features",
        description="Features enabled for the suggest endpoint.",
    )

    # re-exports from search to have them in scope here
    debug = SearchParamDefaults.debug
    faceted = SearchParamDefaults.faceted
    fields = SearchParamDefaults.fields
    filter_expression = SearchParamDefaults.filter_expression
    filters = SearchParamDefaults.filters
    highlight = SearchParamDefaults.highlight
    range_creation_end = SearchParamDefaults.range_creation_end
    range_creation_start = SearchParamDefaults.range_creation_start
    range_modification_end = SearchParamDefaults.range_modification_end
    range_modification_start = SearchParamDefaults.range_modification_start
    security = SearchParamDefaults.security
    security_groups = SearchParamDefaults.security_groups
    show_hidden = SearchParamDefaults.show_hidden


class SuggestRequest(BaseModel, extra="forbid"):
    # query
    query: str = SuggestParamDefaults.suggest_query.to_pydantic_field()
    features: list[SuggestOptions] = SuggestParamDefaults.suggest_features.to_pydantic_field()

    # filters
    filter_expression: FilterExpression | None = (
        SuggestParamDefaults.filter_expression.to_pydantic_field()
    )
    security: RequestSecurity | None = SuggestParamDefaults.security.to_pydantic_field()
    show_hidden: bool = SuggestParamDefaults.show_hidden.to_pydantic_field()

    # post-process
    highlight: bool = SuggestParamDefaults.highlight.to_pydantic_field()

    debug: SkipJsonSchema[bool] = SuggestParamDefaults.debug.to_pydantic_field()
