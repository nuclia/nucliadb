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
from enum import Enum
from typing import TYPE_CHECKING, Dict, List, Optional, Type, TypeVar

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel

from nucliadb_models import CloudLink, FileB64
from nucliadb_protos import resources_pb2

_T = TypeVar("_T")


if TYPE_CHECKING:
    LayoutFormatValue = resources_pb2.FieldLayout.Format.V
else:
    LayoutFormatValue = str


# Shared classes


class LayoutFormat(LayoutFormatValue, Enum):  # type: ignore
    NUCLIAv1 = "NUCLIAv1"


class TypeBlock(str, Enum):  # type: ignore
    TITLE = "TITLE"
    DESCRIPTION = "DESCRIPTION"
    RICHTEXT = "RICHTEXT"
    TEXT = "TEXT"
    ATTACHMENTS = "ATTACHMENTS"
    COMMENTS = "COMMENTS"
    CLASSIFICATIONS = "CLASSIFICATIONS"


# Visualization classes (Those used on reader endpoints)


class Block(BaseModel):
    x: int
    y: int
    cols: int
    rows: int
    type: Optional[TypeBlock]
    ident: Optional[str]
    payload: Optional[str]
    file: Optional[CloudLink]


class LayoutContent(BaseModel):
    blocks: Dict[str, Block] = {}
    deleted_blocks: Optional[List[str]]


class FieldLayout(BaseModel):
    body: LayoutContent
    format: LayoutFormat

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.FieldLayout) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


# Creation and update classes (Those used on writer endpoints)


class InputBlock(BaseModel):
    x: int
    y: int
    cols: int
    rows: int
    type: TypeBlock
    ident: str
    payload: str
    file: FileB64


class InputLayoutContent(BaseModel):
    blocks: Dict[str, InputBlock] = {}


class InputLayoutField(BaseModel):
    body: InputLayoutContent
    format: LayoutFormat = LayoutFormat.NUCLIAv1


# Processing classes (Those used to sent to push endpoints)


class PushLayoutFormat(Enum):  # type: ignore
    NUCLIAv1 = 0


class PushLayoutBlock(BaseModel):
    x: int
    y: int
    cols: int
    rows: int
    type: TypeBlock
    ident: str
    payload: str
    file: Optional[str] = None


class LayoutDiff(BaseModel):
    format: PushLayoutFormat
    blocks: Dict[str, PushLayoutBlock] = {}
