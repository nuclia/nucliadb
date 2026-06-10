# Copyright 2021 Bosutech XXI S.L.
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

from dataclasses import dataclass

from pydantic import BaseModel


class ObjectInfo(BaseModel):
    name: str


class ObjectMetadata(BaseModel):
    filename: str
    content_type: str
    size: int


@dataclass
class Range:
    """
    Represents a range of bytes to be downloaded from a file. The range is inclusive.
    The start and end values are 0-based.
    """

    start: int | None = None
    end: int | None = None

    def any(self) -> bool:
        return self.start is not None or self.end is not None

    def to_header(self) -> str:
        return f"bytes={self.start or 0}-{self.end if self.end is not None else ''}"
