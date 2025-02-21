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

from nucliadb_models.common import FieldTypeName
from nucliadb_protos.resources_pb2 import FieldType

FIELD_TYPE_STR_TO_PB: dict[str, FieldType.ValueType] = {
    "t": FieldType.TEXT,
    "f": FieldType.FILE,
    "u": FieldType.LINK,
    "a": FieldType.GENERIC,
    "c": FieldType.CONVERSATION,
}

FIELD_TYPE_PB_TO_STR = {v: k for k, v in FIELD_TYPE_STR_TO_PB.items()}

FIELD_TYPE_NAME_TO_STR = {
    FieldTypeName.TEXT: "t",
    FieldTypeName.FILE: "f",
    FieldTypeName.LINK: "u",
    FieldTypeName.GENERIC: "a",
    FieldTypeName.CONVERSATION: "c",
}


@dataclass
class FieldId:
    """
    Field ids are used to identify fields in resources. They usually have the following format:

        `rid/field_type/field_key`

    where field type is one of: `t`, `f`, `u`, `a`, `c` (text, file, link, generic, conversation)
    and field_key is an identifier for that field type on the resource, usually chosen by the user.

    In some cases, fields can have subfields, for example, in conversations, where each part of the
    conversation is a subfield. In those cases, the id has the following format:

        `rid/field_type/field_key/subfield_id`

    Examples:

    >>> FieldId(rid="rid", type="u", key="/my-link")
    FieldID("rid/u/my-link")
    >>> FieldId.from_string("rid/u/my-link")
    FieldID("rid/u/my-link")
    """

    rid: str
    type: str
    key: str
    # also knwon as `split`, this indicates a part of a field in, for example, conversations
    subfield_id: Optional[str] = None

    def __repr__(self) -> str:
        return f"FieldId({self.full()})"

    def short_without_subfield(self) -> str:
        return f"/{self.type}/{self.key}"

    def full(self) -> str:
        if self.subfield_id is None:
            return f"{self.rid}/{self.type}/{self.key}"
        else:
            return f"{self.rid}/{self.type}/{self.key}/{self.subfield_id}"

    def __hash__(self) -> int:
        return hash(self.full())

    @property
    def pb_type(self) -> FieldType.ValueType:
        return FIELD_TYPE_STR_TO_PB[self.type]

    @classmethod
    def from_pb(
        cls, rid: str, field_type: FieldType.ValueType, key: str, subfield_id: Optional[str] = None
    ) -> "FieldId":
        return cls(rid=rid, type=FIELD_TYPE_PB_TO_STR[field_type], key=key, subfield_id=subfield_id)

    @classmethod
    def from_string(cls, value: str) -> "FieldId":
        """
        Parse a FieldId from a string
        Example:
        >>> fid = FieldId.from_string("rid/u/foo")
        >>> fid
        FieldId("rid/u/foo")
        >>> fid.type
        'u'
        >>> fid.key
        'foo'
        >>> FieldId.from_string("rid/u/foo/subfield_id").subfield_id
        'subfield_id'
        """
        parts = value.split("/")
        if len(parts) == 3:
            rid, _type, key = parts
            _type = cls.parse_field_type(_type)
            return cls(rid=rid, type=_type, key=key)
        elif len(parts) == 4:
            rid, _type, key, subfield_id = parts
            _type = cls.parse_field_type(_type)
            return cls(
                rid=rid,
                type=_type,
                key=key,
                subfield_id=subfield_id,
            )
        else:
            raise ValueError(f"Invalid FieldId: {value}")

    @classmethod
    def parse_field_type(cls, _type: str) -> str:
        if _type not in FIELD_TYPE_STR_TO_PB:
            # Try to parse the enum value
            # XXX: This is to support field types that are integer values of FieldType
            # Which is how legacy processor relations reported the paragraph_id
            try:
                type_pb = FieldType.ValueType(int(_type))
            except ValueError:
                raise ValueError(f"Invalid FieldId: {_type}")
            if type_pb in FIELD_TYPE_PB_TO_STR:
                return FIELD_TYPE_PB_TO_STR[type_pb]
            else:
                raise ValueError(f"Invalid FieldId: {_type}")
        return _type


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

    @property
    def rid(self) -> str:
        return self.field_id.rid

    @classmethod
    def from_string(cls, value: str) -> "ParagraphId":
        parts = value.split("/")
        paragraph_range = parts[-1]
        start, end = map(int, paragraph_range.split("-"))
        field_id = FieldId.from_string("/".join(parts[:-1]))
        return cls(field_id=field_id, paragraph_start=start, paragraph_end=end)

    @classmethod
    def from_vector_id(cls, vid: "VectorId") -> "ParagraphId":
        """
        Returns a ParagraphId from a vector_key (the index part of the vector_key is ignored).
        >>> vid = VectorId.from_string("rid/u/field_id/0/0-1")
        >>> ParagraphId.from_vector_id(vid)
        ParagraphId("rid/u/field_id/0-1")
        """
        return cls(
            field_id=vid.field_id,
            paragraph_start=vid.vector_start,
            paragraph_end=vid.vector_end,
        )


@dataclass
class VectorId:
    """
    Ids of vectors are very similar to ParagraphIds, but for legacy reasons, they have an index
    indicating the position of the corresponding text block in the list of text blocks for the field.

    Examples:

    >>> VectorId.from_string("rid/u/field_id/0/0-10")
    VectorId("rid/u/field_id/0/0-10")
    >>> VectorId(
    ...    field_id=FieldId.from_string("rid/u/field_id"),
    ...    index=0,
    ...    vector_start=0,
    ...    vector_end=10,
    ... )
    VectorId("rid/u/field_id/0/0-10")
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

    @property
    def rid(self) -> str:
        return self.field_id.rid

    @classmethod
    def from_string(cls, value: str) -> "VectorId":
        parts = value.split("/")
        vector_range = parts[-1]
        start, end = map(int, vector_range.split("-"))
        index = int(parts[-2])
        field_id = FieldId.from_string("/".join(parts[:-2]))
        return cls(field_id=field_id, index=index, vector_start=start, vector_end=end)


def extract_data_augmentation_id(generated_field_id: str) -> Optional[str]:
    """Data augmentation generated fields have a strict id with the following
    format:
    `da-{task_id}-{original:field_type}-{original:field_id}[-{original:split}]`

    @return the `task_id`

    ATENTION: we are assuming ids have been properly generated and `-` is not a
    valid character, otherwise, this extraction would be wrong and a partial id
    would be returned.

    """
    parts = generated_field_id.split("-")

    if len(parts) < 4:
        return None

    if parts[0] != "da":
        return None

    return parts[1] or None
