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
from typing import Dict, List, Optional

from pydantic import BaseModel


class WidgetMode(str, Enum):
    BUTTON = "button"
    INPUT = "input"
    FORM = "form"


class WidgetFeatures(BaseModel):
    useFilters: bool = False
    suggestEntities: bool = False
    suggestSentences: bool = False
    suggestParagraphs: bool = False
    suggestLabels: bool = False
    editLabels: bool = False
    entityAnnotation: bool = False


class Widget(BaseModel):
    id: str
    description: Optional[str]
    mode: Optional[WidgetMode]
    features: Optional[WidgetFeatures]
    filters: List[str] = []
    topEntities: List[str] = []
    style: Dict[str, str] = {}


class KnowledgeBoxWidgets(BaseModel):
    uuid: str
    widgets: Dict[str, Widget] = {}
