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
from typing import List, Optional, Type, TypeVar

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel

from nucliadb_protos import resources_pb2

_T = TypeVar("_T")


# Shared classes


class Keyword(BaseModel):
    value: str


class FieldKeywordset(BaseModel):
    keywords: Optional[List[Keyword]]

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.FieldKeywordset) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


# Visualization classes (Those used on reader endpoints)
# Creation and update classes (Those used on writer endpoints)
# - This model is unified

# Processing classes (Those used to sent to push endpoints)
# - Doesn't apply, Datetimes are not sent to process
