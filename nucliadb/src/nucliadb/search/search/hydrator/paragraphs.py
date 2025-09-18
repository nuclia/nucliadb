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
from typing import Optional, Union

from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb_models import hydration as hydration_models
from nucliadb_protos import resources_pb2
from nucliadb_protos.resources_pb2 import FieldComputedMetadata


class ParagraphIndex:
    """Small helper class to cache field paragraphs and its relations and be
    used as an index.

    """

    NEXT = "next"
    PREVIOUS = "previous"
    PARENTS = "parents"
    SIBLINGS = "siblings"
    REPLACEMENTS = "replacements"

    def __init__(self) -> None:
        self.paragraphs: dict[str, resources_pb2.Paragraph] = {}
        self.neighbours: dict[tuple[str, str], str] = {}
        self.related: dict[tuple[str, str], list[str]] = {}

    async def build(self, field_id: FieldId, field_metadata: FieldComputedMetadata):
        self.paragraphs.clear()
        self.neighbours.clear()
        self.related.clear()

        if field_id.subfield_id is None:
            field_paragraphs = field_metadata.metadata.paragraphs
        else:
            field_paragraphs = field_metadata.split_metadata[field_id.subfield_id].paragraphs

        previous = None
        for paragraph in field_paragraphs:
            paragraph_id = field_id.paragraph_id(paragraph.start, paragraph.end).full()
            self.paragraphs[paragraph_id] = paragraph

            if previous is not None:
                self.neighbours[(previous, ParagraphIndex.NEXT)] = paragraph_id
                self.neighbours[(paragraph_id, ParagraphIndex.PREVIOUS)] = previous
            previous = paragraph_id

            self.related[(paragraph_id, ParagraphIndex.PARENTS)] = [
                parent for parent in paragraph.relations.parents
            ]
            self.related[(paragraph_id, ParagraphIndex.SIBLINGS)] = [
                sibling for sibling in paragraph.relations.siblings
            ]
            self.related[(paragraph_id, ParagraphIndex.REPLACEMENTS)] = [
                replacement for replacement in paragraph.relations.replacements
            ]

    def get(self, paragraph_id: Union[str, ParagraphId]) -> Optional[resources_pb2.Paragraph]:
        paragraph_id = str(paragraph_id)
        return self.paragraphs.get(paragraph_id)

    def previous(self, paragraph_id: Union[str, ParagraphId]) -> Optional[str]:
        paragraph_id = str(paragraph_id)
        return self.neighbours.get((paragraph_id, ParagraphIndex.PREVIOUS))

    def next(self, paragraph_id: Union[str, ParagraphId]) -> Optional[str]:
        paragraph_id = str(paragraph_id)
        return self.neighbours.get((paragraph_id, ParagraphIndex.NEXT))

    def n_previous(self, paragraph_id: Union[str, ParagraphId], count: int = 1) -> list[str]:
        assert count >= 1, f"can't find negative previous {count}"
        paragraph_id = str(paragraph_id)
        previous: list[str] = []
        current_id = paragraph_id
        for _ in range(count):
            previous_id = self.previous(current_id)
            if previous_id is None:
                # we've reached the first paragraph
                break
            previous.insert(0, previous_id)
            current_id = previous_id
        return previous

    def n_next(self, paragraph_id: Union[str, ParagraphId], count: int = 1) -> list[str]:
        assert count >= 1, f"can't find negative nexts {count}"
        paragraph_id = str(paragraph_id)
        nexts = []
        current_id = paragraph_id
        for _ in range(count):
            next_id = self.next(current_id)
            if next_id is None:
                # we've reached the last paragraph
                break
            current_id = next_id
            nexts.append(next_id)
        return nexts

    def parents(self, paragraph_id: Union[str, ParagraphId]) -> list[str]:
        paragraph_id = str(paragraph_id)
        return self.related.get((paragraph_id, ParagraphIndex.PARENTS), [])

    def siblings(self, paragraph_id: Union[str, ParagraphId]) -> list[str]:
        paragraph_id = str(paragraph_id)
        return self.related.get((paragraph_id, ParagraphIndex.SIBLINGS), [])

    def replacements(self, paragraph_id: Union[str, ParagraphId]) -> list[str]:
        paragraph_id = str(paragraph_id)
        return self.related.get((paragraph_id, ParagraphIndex.REPLACEMENTS), [])


async def related_paragraphs_refs(
    paragraph_id: ParagraphId,
    index: ParagraphIndex,
    config: hydration_models.RelatedParagraphHydration,
) -> tuple[hydration_models.RelatedParagraphRefs, list[ParagraphId]]:
    """Compute the related paragraph references for a specific `paragraph_id`
    and return them with the plain list of unique related paragraphs (to
    facilitate work to the caller).

    """
    hydrated = hydration_models.RelatedParagraphRefs()
    related = set()

    if config.neighbours:
        hydrated.neighbours = hydration_models.RelatedNeighbourParagraphRefs()

        if config.neighbours.before is not None:
            hydrated.neighbours.before = []
            if config.neighbours.before > 0:
                for previous_id in index.n_previous(paragraph_id, config.neighbours.before):
                    hydrated.neighbours.before.insert(0, previous_id)
                    related.add(ParagraphId.from_string(previous_id))

        if config.neighbours.after is not None:
            hydrated.neighbours.after = []
            if config.neighbours.after > 0:
                for next_id in index.n_next(paragraph_id, config.neighbours.after):
                    hydrated.neighbours.after.append(next_id)
                    related.add(ParagraphId.from_string(next_id))

    if config.parents:
        hydrated.parents = []
        for parent_id in index.parents(paragraph_id):
            hydrated.parents.append(parent_id)
            related.add(ParagraphId.from_string(parent_id))

    if config.siblings:
        hydrated.siblings = []
        for sibling_id in index.siblings(paragraph_id):
            hydrated.siblings.append(sibling_id)
            related.add(ParagraphId.from_string(sibling_id))

    if config.replacements:
        hydrated.replacements = []
        for replacement_id in index.replacements(paragraph_id):
            hydrated.replacements.append(replacement_id)
            related.add(ParagraphId.from_string(replacement_id))

    return hydrated, list(related)
