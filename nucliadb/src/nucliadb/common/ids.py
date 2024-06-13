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

"""
This module aims to centralize how we build ids for resources, fields,
paragraphs... Avoiding spread of id construction and parsing everywhere
"""

from dataclasses import dataclass
from typing import Optional

# Index


@dataclass
class FieldId:
    rid: str
    field_id: str
    # also knwon as `split`, this indicates a part of a field in, for example, conversations
    subfield_id: Optional[str] = None

    def full(self) -> str:
        if self.subfield_id is None:
            return f"{self.rid}/{self.field_id}"
        else:
            return f"{self.rid}/{self.field_id}/{self.subfield_id}"


@dataclass
class ParagraphId:
    field_id: FieldId
    paragraph_start: int
    paragraph_end: int

    def full(self) -> str:
        return f"{self.field_id.full()}/{self.paragraph_start}-{self.paragraph_end}"


@dataclass
class VectorId:
    field_id: FieldId
    index: int
    vector_start: int
    vector_end: int

    def full(self) -> str:
        return f"{self.field_id.full()}/{self.index}/{self.vector_start}-{self.vector_end}"
