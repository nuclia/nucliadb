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
from typing import List

from pydantic import BaseModel, Field


class ResourceSecurity(BaseModel):
    """
    Security metadata for the resource
    """

    access_groups: List[str] = Field(
        default=[],
        title="Access groups",
        description="List of group ids that can access the resource.",
    )


class RequestSecurity(BaseModel):
    """
    Security metadata for the search request
    """

    groups: List[str] = Field(
        default=[],
        title="Groups",
        description="List of group ids to do the request with. ",
    )
