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
import asyncio
from dataclasses import dataclass

from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.orm.resource import Resource
from nucliadb.search.augmentor.paragraphs import get_paragraph_text
from nucliadb.search.search.hydrator.fields import page_preview_id
from nucliadb.search.search.hydrator.images import paragraph_source_image
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

    def __init__(self, field_id: FieldId) -> None:
        self.field_id = field_id
        self.paragraphs: dict[str, resources_pb2.Paragraph] = {}
        self.neighbours: dict[tuple[str, str], str] = {}
        self.related: dict[tuple[str, str], list[str]] = {}
        self._lock = asyncio.Lock()
        self._built = False

    async def build(self, field: Field):
        """Build the index if it hasn't been built yet.

        This function is async-safe, multiple concurrent tasks can ask for a
        built and it'll only be done once
        """
        if self._built:
            return

        async with self._lock:
            # double check we haven't built the index meanwhile we waited for the
            # lock
            if self._built:
                return

            field_metadata = await field.get_field_metadata()

            if field_metadata is None:
                # field metadata may be still processing. As we want to provide a
                # consistent view, even if it can appear meanwhile we hydrate, we
                # consider we don't have it. We mark the index as built and any
                # paragraph will be found for this field
                self._built = True
                return None

            # REVIEW: this is a CPU-bound code, we may consider running this in an
            # executor to not block the loop
            self._build(field_metadata)
            self._built = True

    def _build(self, field_metadata: FieldComputedMetadata):
        self.paragraphs.clear()
        self.neighbours.clear()
        self.related.clear()

        if self.field_id.subfield_id is None:
            field_paragraphs = field_metadata.metadata.paragraphs
        else:
            field_paragraphs = field_metadata.split_metadata[self.field_id.subfield_id].paragraphs

        previous = None
        for paragraph in field_paragraphs:
            paragraph_id = self.field_id.paragraph_id(paragraph.start, paragraph.end).full()
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

    def get(self, paragraph_id: str | ParagraphId) -> resources_pb2.Paragraph | None:
        paragraph_id = str(paragraph_id)
        return self.paragraphs.get(paragraph_id)

    def previous(self, paragraph_id: str | ParagraphId) -> str | None:
        paragraph_id = str(paragraph_id)
        return self.neighbours.get((paragraph_id, ParagraphIndex.PREVIOUS))

    def next(self, paragraph_id: str | ParagraphId) -> str | None:
        paragraph_id = str(paragraph_id)
        return self.neighbours.get((paragraph_id, ParagraphIndex.NEXT))

    def n_previous(self, paragraph_id: str | ParagraphId, count: int = 1) -> list[str]:
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

    def n_next(self, paragraph_id: str | ParagraphId, count: int = 1) -> list[str]:
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

    def parents(self, paragraph_id: str | ParagraphId) -> list[str]:
        paragraph_id = str(paragraph_id)
        return self.related.get((paragraph_id, ParagraphIndex.PARENTS), [])

    def siblings(self, paragraph_id: str | ParagraphId) -> list[str]:
        paragraph_id = str(paragraph_id)
        return self.related.get((paragraph_id, ParagraphIndex.SIBLINGS), [])

    def replacements(self, paragraph_id: str | ParagraphId) -> list[str]:
        paragraph_id = str(paragraph_id)
        return self.related.get((paragraph_id, ParagraphIndex.REPLACEMENTS), [])


@dataclass
class ExtraParagraphHydration:
    field_page: int | None
    field_table_page: int | None
    related_paragraph_ids: list[ParagraphId]


async def hydrate_paragraph(
    resource: Resource,
    field: Field,
    paragraph_id: ParagraphId,
    config: hydration_models.ParagraphHydration,
    field_paragraphs_index: ParagraphIndex,
) -> tuple[hydration_models.HydratedParagraph, ExtraParagraphHydration]:
    """Hydrate a paragraph and return the extra hydration to built a coherent
    hydration around this paragraph.

    Although the resource and field exist, the paragraph doesn't necessarily
    need to be a real one in the paragraph metadata, it can be made-up to
    include more or less text than the originally extracted.

    """
    kbid = resource.kbid

    hydrated = hydration_models.HydratedParagraph(
        id=paragraph_id.full(),
        field=paragraph_id.field_id.full(),
        resource=paragraph_id.rid,
    )
    extra_hydration = ExtraParagraphHydration(
        field_page=None, field_table_page=None, related_paragraph_ids=[]
    )

    if config.text:
        text = await get_paragraph_text(field, paragraph_id)
        hydrated.text = text

    requires_paragraph_metadata = config.image or config.table or config.page or config.related
    if requires_paragraph_metadata:
        await field_paragraphs_index.build(field)
        paragraph = field_paragraphs_index.get(paragraph_id)
        if paragraph is not None:
            # otherwise, this is a fake paragraph. We can't hydrate anything else here

            if config.related:
                if config.related.neighbours is not None:
                    before = config.related.neighbours.before
                    after = config.related.neighbours.after
                else:
                    before, after = None, None

                hydrated.related, related_ids = await related_paragraphs_refs(
                    paragraph_id,
                    field_paragraphs_index,
                    neighbours_before=before,
                    neighbours_after=after,
                    parents=config.related.parents or False,
                    siblings=config.related.siblings or False,
                    replacements=config.related.replacements or False,
                )
                extra_hydration.related_paragraph_ids = related_ids

            if config.image:
                hydrated.image = hydration_models.HydratedParagraphImage()

                if config.image.source_image:
                    hydrated.image.source_image = await paragraph_source_image(
                        kbid, paragraph_id, paragraph
                    )

            if config.page:
                if hydrated.page is None:
                    hydrated.page = hydration_models.HydratedParagraphPage()

                if config.page.page_with_visual:
                    if paragraph.page.page_with_visual:
                        # Paragraphs can be found on pages with visual content. In this
                        # case, we want to return the preview of the paragraph page as
                        # an image
                        page_number = paragraph.page.page
                        # TODO: what should I do if I later find there's no page in the DB?
                        hydrated.page.page_preview_ref = page_preview_id(page_number)
                        extra_hydration.field_page = page_number

            if config.table:
                if hydrated.table is None:
                    hydrated.table = hydration_models.HydratedParagraphTable()

                if config.table.table_page_preview:
                    if paragraph.representation.is_a_table:
                        # When a paragraph comes with a table and table hydration is
                        # enabled, we want to return the image representing that table.
                        # Ideally we should hydrate the paragraph reference_file, but
                        # table screenshots are not always perfect so we prefer to use
                        # the page preview. If at some point the table images are good
                        # enough, it'd be better to use those
                        page_number = paragraph.page.page
                        hydrated.table.page_preview_ref = page_preview_id(page_number)
                        extra_hydration.field_table_page = page_number

    return hydrated, extra_hydration


async def related_paragraphs_refs(
    paragraph_id: ParagraphId,
    index: ParagraphIndex,
    *,
    neighbours_before: int | None = None,
    neighbours_after: int | None = None,
    parents: bool = False,
    siblings: bool = False,
    replacements: bool = False,
) -> tuple[hydration_models.RelatedParagraphRefs, list[ParagraphId]]:
    """Compute the related paragraph references for a specific `paragraph_id`
    and return them with the plain list of unique related paragraphs (to
    facilitate work to the caller).

    """
    hydrated = hydration_models.RelatedParagraphRefs()
    related = set()

    if neighbours_before or neighbours_after:
        hydrated.neighbours = hydration_models.RelatedNeighbourParagraphRefs()

        if neighbours_before is not None:
            hydrated.neighbours.before = []
            if neighbours_before > 0:
                for previous_id in index.n_previous(paragraph_id, neighbours_before):
                    hydrated.neighbours.before.insert(0, previous_id)
                    related.add(ParagraphId.from_string(previous_id))

        if neighbours_after is not None:
            hydrated.neighbours.after = []
            if neighbours_after > 0:
                for next_id in index.n_next(paragraph_id, neighbours_after):
                    hydrated.neighbours.after.append(next_id)
                    related.add(ParagraphId.from_string(next_id))

    if parents:
        hydrated.parents = []
        for parent_id in index.parents(paragraph_id):
            hydrated.parents.append(parent_id)
            related.add(ParagraphId.from_string(parent_id))

    if siblings:
        hydrated.siblings = []
        for sibling_id in index.siblings(paragraph_id):
            hydrated.siblings.append(sibling_id)
            related.add(ParagraphId.from_string(sibling_id))

    if replacements:
        hydrated.replacements = []
        for replacement_id in index.replacements(paragraph_id):
            hydrated.replacements.append(replacement_id)
            related.add(ParagraphId.from_string(replacement_id))

    return hydrated, list(related)
