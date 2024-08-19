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

from nucliadb_protos.resources_pb2 import FieldType

FIELD_TYPE_STR_TO_PB: dict[str, FieldType.ValueType] = {
    "t": FieldType.TEXT,
    "f": FieldType.FILE,
    "u": FieldType.LINK,
    "a": FieldType.GENERIC,
    "c": FieldType.CONVERSATION,
}

FIELD_TYPE_PB_TO_STR = {v: k for k, v in FIELD_TYPE_STR_TO_PB.items()}


@dataclass
class FieldId:
    rid: str
    field_id: str
    # also knwon as `split`, this indicates a part of a field in, for example, conversations
    subfield_id: Optional[str] = None

    def __repr__(self) -> str:
        return f"FieldId({self.full()})"

    def full(self) -> str:
        if self.subfield_id is None:
            return f"{self.rid}/{self.field_id}"
        else:
            return f"{self.rid}/{self.field_id}/{self.subfield_id}"

    @property
    def field_type(self) -> str:
        return self.field_id.split("/")[0]

    def __hash__(self) -> int:
        return hash(self.full())

    @property
    def pb_type(self) -> FieldType.ValueType:
        return FIELD_TYPE_STR_TO_PB[self.field_type]

    @classmethod
    def from_string(cls, value: str) -> "FieldId":
        """
        Parse a FieldId from a string
        Example:
        >>> FieldId.from_string("rid/u/field_id")
        FieldId(rid="rid", field_id="u/field_id")
        >>> FieldId.from_string("rid/u/field_id/subfield_id")
        FieldId(rid="rid", field_id="u/field_id", subfield_id="subfield_id")
        """
        parts = value.split("/")
        if len(parts) == 3:
            rid, field_type, field_id = parts
            return cls(rid=rid, field_id=f"{field_type}/{field_id}")
        elif len(parts) == 4:
            rid, field_type, field_id, subfield_id = parts
            return cls(
                rid=rid,
                field_id=f"{field_type}/{field_id}",
                subfield_id=subfield_id,
            )
        else:
            raise ValueError(f"Invalid FieldId: {value}")


@dataclass
class ParagraphId:
    field_id: FieldId
    paragraph_start: int
    paragraph_end: int

    def __repr__(self) -> str:
        return f"ParagraphId({self.full()})"

    def full(self) -> str:
        return f"{self.field_id.full()}/{self.paragraph_start}-{self.paragraph_end}"

    def __hash__(self) -> int:
        return hash(self.full())

    @classmethod
    def from_string(cls, value: str) -> "ParagraphId":
        parts = value.split("/")
        paragraph_range = parts[-1]
        start, end = map(int, paragraph_range.split("-"))
        field_id = FieldId.from_string("/".join(parts[:-1]))
        return cls(field_id=field_id, paragraph_start=start, paragraph_end=end)

    @classmethod
    def from_vector_key(cls, vector_key: str) -> "ParagraphId":
        """
        Returns a ParagraphId from a vector_key (the index part of the vector_key is ignored).
        >>> ParagraphId.from_vector_key("rid/u/field_id/0/0-1")
        ParagraphId("rid/u/field_id/0-1")
        """
        parts = vector_key.split("/")
        vector_range = parts[-1]
        start, end = map(int, vector_range.split("-"))
        field_id = FieldId.from_string("/".join(parts[:-2]))
        return cls(field_id=field_id, paragraph_start=start, paragraph_end=end)


@dataclass
class VectorId:
    """
    Ids of vectors are very similar to ParagraphIds, but for legacy reasons, they have an index
    indicating the position of the corresponding text block in the list of text blocks for the field.
    """

    field_id: FieldId
    index: int
    vector_start: int
    vector_end: int

    def __repr__(self) -> str:
        return f"VectorId({self.full()})"

    def full(self) -> str:
        return f"{self.field_id.full()}/{self.index}/{self.vector_start}-{self.vector_end}"

    def __hash__(self) -> int:
        return hash(self.full())

    @classmethod
    def from_string(cls, value: str) -> "VectorId":
        parts = value.split("/")
        vector_range = parts[-1]
        start, end = map(int, vector_range.split("-"))
        index = int(parts[-2])
        field_id = FieldId.from_string("/".join(parts[:-2]))
        return cls(field_id=field_id, index=index, vector_start=start, vector_end=end)
