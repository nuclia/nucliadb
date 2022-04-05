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
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from nucliadb_protos.nodereader_pb2 import OrderBy
from pydantic import BaseModel

from nucliadb_models.resource import Resource

if TYPE_CHECKING:
    SortValue = OrderBy.OrderType.V
else:
    SortValue = int


class NucliaDBRoles(str, Enum):
    MANAGER = "MANAGER"
    READER = "READER"
    WRITER = "WRITER"


class SearchOptions(str, Enum):
    PARAGRAPH = "paragraph"
    DOCUMENT = "document"
    RELATIONS = "relations"
    VECTOR = "vector"


class SearchClientType(str, Enum):
    API = "api"
    WIDGET = "widget"
    WEB = "web"
    DASHBOARD = "dashboard"


class Sort(int, Enum):
    DESC = 0
    ASC = 1


class Facet(BaseModel):
    pass


FacetsResult = Dict[str, Facet]


class Sentence(BaseModel):
    score: float
    rid: str
    text: str


class Sentences(BaseModel):
    results: List[Sentence] = []
    facets: FacetsResult


class Paragraph(BaseModel):
    score: float
    rid: str
    field_type: str
    field: str
    text: str
    labels: List[str] = []


class Paragraphs(BaseModel):
    results: List[Paragraph] = []
    facets: FacetsResult


class ResourceResult(BaseModel):
    score: Union[float, int]
    rid: str
    field_type: str
    field: str


class Resources(BaseModel):
    results: List[ResourceResult]
    facets: FacetsResult


class Relation(BaseModel):
    title: str
    uri: str
    resources: List[ResourceResult]


class Relations(BaseModel):
    results: List[Relation] = []


class ResourceSearchResults(BaseModel):
    sentences: Optional[Sentences] = None
    paragraphs: Optional[Paragraphs] = None
    relations: Optional[Relations] = None


class KnowledgeboxSearchResults(BaseModel):
    resources: Dict[str, Resource] = {}
    sentences: Optional[Sentences] = None
    paragraphs: Optional[Paragraphs] = None
    fulltext: Optional[Resources] = None
    relations: Optional[Relations] = None


class KnowledgeboxCounters(BaseModel):
    resources: int
    paragraphs: int
    fields: int
    sentences: int


class SortOption(str, Enum):
    MODIFIED = "modified"
    CREATED = "created"


class KnowledgeBoxCount(BaseModel):
    paragraphs: int
    fields: int
    sentences: int
