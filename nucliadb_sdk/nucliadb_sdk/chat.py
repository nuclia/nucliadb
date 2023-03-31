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

from dataclasses import dataclass

from nucliadb_models.search import (
    FindField,
    FindParagraph,
    FindResource,
    KnowledgeboxFindResults,
)
from nucliadb_sdk.client import NucliaDBClient
from nucliadb_sdk.find import FindResult


@dataclass
class Paragraph:
    paragraph: FindParagraph
    field: FindField
    resource: FindResource


class ChatResut:
    find_result: FindResult
    answer: str
    relations: KnowledgeboxRelationsResults

    def __init__(self, stream: bytes, client: NucliaDBClient):
        stream
        self.inner_find_results = inner_find_results
        self.client = client

    def __iter__(self) -> Iterator[Paragraph]:
        for rid, resource in self.inner_find_results.resources.items():
            for field_id, field in resource.fields.items():
                for paragraph_id, paragraph in field.paragraphs.items():
                    yield Paragraph(paragraph=paragraph, field=field, resource=resource)

    @property
    def find(self) -> FindResult:
        return self.find_result

    @property
    def resources(self):
        return self.inner_find_results.resources
