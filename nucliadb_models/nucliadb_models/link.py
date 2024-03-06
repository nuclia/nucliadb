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
from datetime import datetime
from typing import Dict, Optional, Type, TypeVar

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, Field

from nucliadb_protos import resources_pb2

_T = TypeVar("_T")


# Shared classes

# NOTHING TO SEE HERE

# Visualization classes (Those used on reader endpoints)


class FieldLink(BaseModel):
    added: Optional[datetime]
    headers: Optional[Dict[str, str]]
    cookies: Optional[Dict[str, str]]
    uri: Optional[str]
    language: Optional[str]
    localstorage: Optional[Dict[str, str]]
    css_selector: Optional[str]

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.FieldLink) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


# Creation and update classes (Those used on writer endpoints)


class LinkField(BaseModel):
    headers: Optional[Dict[str, str]] = {}
    cookies: Optional[Dict[str, str]] = {}
    uri: str
    language: Optional[str] = None
    localstorage: Optional[Dict[str, str]] = {}
    css_selector: Optional[str] = None


# Processing classes (Those used to sent to push endpoints)


class LinkUpload(BaseModel):
    link: str
    headers: Dict[str, str] = {}
    cookies: Dict[str, str] = {}
    localstorage: Dict[str, str] = {}
    css_selector: Optional[str] = Field(
        None,
        title="Css selector",
        description="Css selector to parse the link",
    )
