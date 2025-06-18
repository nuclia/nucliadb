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

import warnings
from typing import Annotated, Any, Literal, Optional, Union

from pydantic import BaseModel, Field, create_model

from .graph.requests import GraphPathQuery  # noqa # we need this import for pydantic magic
from .search import AskRequest, FindRequest


class KBConfiguration(BaseModel):
    def __init__(self, **data):
        warnings.warn("KBConfiguration model is deprecated", DeprecationWarning)
        super().__init__(**data)

    # Do not touch this model synced on Processing side
    semantic_model: Optional[str] = None
    generative_model: Optional[str] = None
    ner_model: Optional[str] = None
    anonymization_model: Optional[str] = None
    visual_labeling: Optional[str] = None


#
# Search configurations
#
def _model_fields(model: type[BaseModel], skip: list[str]) -> dict[str, Any]:
    return {
        name: (field.annotation, field) for name, field in model.model_fields.items() if name not in skip
    }


# FindConfig is a FindConfig without `search_configuration`
FindConfig = create_model("FindConfig", **_model_fields(FindRequest, skip=["search_configuration"]))


class FindSearchConfiguration(BaseModel):
    kind: Literal["find"]
    config: FindConfig  # type: ignore[valid-type]


# AskConfig is an AskRequest where `query` is not mandatory and without `search_configuration`
AskConfig = create_model(
    "AskConfig",
    **_model_fields(AskRequest, skip=["query", "search_configuration"]),
    query=(Optional[str], None),
)


class AskSearchConfiguration(BaseModel):
    kind: Literal["ask"]
    config: AskConfig  # type: ignore[valid-type]


SearchConfiguration = Annotated[
    Union[FindSearchConfiguration, AskSearchConfiguration], Field(discriminator="kind")
]

# We need this to avoid issues with pydantic and generic types defined in another module
FindConfig.model_rebuild()
AskConfig.model_rebuild()
