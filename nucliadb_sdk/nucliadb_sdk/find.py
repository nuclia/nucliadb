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
from typing import Dict, Iterator

from nucliadb_models.search import (
    FindField,
    FindParagraph,
    FindResource,
    KnowledgeboxFindResults,
)


@dataclass
class Paragraph:
    paragraph: FindParagraph
    field: FindField
    resource: FindResource


class FindResult:
    inner_find_results: KnowledgeboxFindResults

    def __init__(self, inner_find_results: KnowledgeboxFindResults):
        self.inner_find_results = inner_find_results

    def __iter__(self) -> Iterator[Paragraph]:
        for rid, resource in self.resources.items():
            for field_id, field in resource.fields.items():
                for paragraph_id, paragraph in field.paragraphs.items():
                    yield Paragraph(paragraph=paragraph, field=field, resource=resource)

    @property
    def total(self) -> int:
        return self.inner_find_results.total

    @property
    def relations(self):
        return self.inner_find_results.relations

    @property
    def resources(self) -> Dict[str, FindResource]:
        return self.inner_find_results.resources
