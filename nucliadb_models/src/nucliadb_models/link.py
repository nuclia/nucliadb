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
from typing import Dict, Optional

from pydantic import BaseModel, Field

# Shared classes

# NOTHING TO SEE HERE

# Visualization classes (Those used on reader endpoints)


class FieldLink(BaseModel):
    added: Optional[datetime] = None
    headers: Optional[Dict[str, str]] = None
    cookies: Optional[Dict[str, str]] = None
    uri: Optional[str] = None
    language: Optional[str] = None
    localstorage: Optional[Dict[str, str]] = None
    css_selector: Optional[str] = None
    xpath: Optional[str] = None
    extract_strategy: Optional[str] = Field(
        default=None,
        description="Id of the Nuclia extract strategy used at processing time. If not set, the default strategy was used. Extract strategies are defined at the learning configuration api.",
    )


# Creation and update classes (Those used on writer endpoints)


class LinkField(BaseModel):
    headers: Optional[Dict[str, str]] = {}
    cookies: Optional[Dict[str, str]] = {}
    uri: str
    language: Optional[str] = None
    localstorage: Optional[Dict[str, str]] = {}
    css_selector: Optional[str] = None
    xpath: Optional[str] = None
    extract_strategy: Optional[str] = Field(
        default=None,
        description="Id of the Nuclia extract strategy to use at processing time. If not set, the default strategy will be used. Extract strategies are defined at the learning configuration api.",
    )
