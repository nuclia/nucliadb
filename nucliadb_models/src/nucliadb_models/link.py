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
from datetime import datetime

from pydantic import BaseModel, Field

# Shared classes

# NOTHING TO SEE HERE

# Visualization classes (Those used on reader endpoints)


class FieldLink(BaseModel):
    added: datetime | None = None
    headers: dict[str, str] | None = None
    cookies: dict[str, str] | None = None
    uri: str | None = None
    language: str | None = None
    localstorage: dict[str, str] | None = None
    css_selector: str | None = None
    xpath: str | None = None
    extract_strategy: str | None = Field(
        default=None,
        description="Id of the Nuclia extract strategy used at processing time. If not set, the default strategy was used. Extract strategies are defined at the learning configuration api.",
    )
    split_strategy: str | None = Field(
        default=None,
        description="Id of the Nuclia split strategy used at processing time. If not set, the default strategy was used. Split strategies are defined at the learning configuration api.",
    )


# Creation and update classes (Those used on writer endpoints)


class LinkField(BaseModel):
    headers: dict[str, str] | None = {}
    cookies: dict[str, str] | None = {}
    uri: str
    language: str | None = None
    localstorage: dict[str, str] | None = {}
    css_selector: str | None = None
    xpath: str | None = None
    extract_strategy: str | None = Field(
        default=None,
        description="Id of the Nuclia extract strategy to use at processing time. If not set, the default strategy will be used. Extract strategies are defined at the learning configuration api.",
    )
    split_strategy: str | None = Field(
        default=None,
        description="Id of the Nuclia split strategy used at processing time. If not set, the default strategy was used. Split strategies are defined at the learning configuration api.",
    )
