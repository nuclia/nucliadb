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
from typing import TYPE_CHECKING, Optional, Type, TypeVar

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, root_validator

from nucliadb_models.utils import validate_json
from nucliadb_protos import resources_pb2

_T = TypeVar("_T")


if TYPE_CHECKING:  # pragma: no cover
    TextFormatValue = resources_pb2.FieldText.Format.V
else:
    TextFormatValue = int


# Shared classes


class TextFormat(Enum):  # type: ignore
    PLAIN = "PLAIN"
    HTML = "HTML"
    RST = "RST"
    MARKDOWN = "MARKDOWN"
    JSON = "JSON"


TEXT_FORMAT_TO_MIMETYPE = {
    TextFormat.PLAIN: "text/plain",
    TextFormat.HTML: "text/html",
    TextFormat.RST: "text/x-rst",
    TextFormat.MARKDOWN: "text/markdown",
    TextFormat.JSON: "application/json",
}


# Visualization classes (Those used on reader endpoints)


class FieldText(BaseModel):
    body: Optional[str]
    format: Optional[TextFormat]
    md5: Optional[str]

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.FieldText) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


# Creation and update classes (Those used on writer endpoints)


class TextField(BaseModel):
    body: str
    format: TextFormat = TextFormat.PLAIN

    @root_validator(pre=False, skip_on_failure=True)
    def check_text_format(cls, values):
        if values.get("format") == TextFormat.JSON:
            validate_json(values.get("body", ""))
        return values


# Processing classes (Those used to sent to push endpoints)


class PushTextFormat(TextFormatValue, Enum):  # type: ignore
    PLAIN = 0
    HTML = 1
    MARKDOWN = 2
    RST = 3
    JSON = 4


class Text(BaseModel):
    body: str
    format: PushTextFormat
