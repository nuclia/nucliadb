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
from typing import cast

from typing_extensions import assert_never

import nucliadb_models
import nucliadb_models.resource
from nucliadb.common import datamanagers
from nucliadb.common.ids import FIELD_TYPE_NAME_TO_STR, FieldId, ParagraphId
from nucliadb.models.internal.augment import (
    Augment,
    Augmented,
    AugmentedField,
    AugmentedParagraph,
    AugmentedResource,
    ConversationAugment,
    ConversationProp,
    DeepResourceAugment,
    FieldAugment,
    FieldProp,
    FileAugment,
    FileProp,
    Metadata,
    Paragraph,
    ParagraphAugment,
    ParagraphProp,
    ParagraphText,
    ResourceAugment,
    ResourceProp,
)
from nucliadb.search.augmentor.metrics import augmentor_observer
from nucliadb.search.augmentor.utils import limited_concurrency
from nucliadb.search.search.hydrator import ResourceHydrationOptions
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import Resource
from nucliadb_utils import const
from nucliadb_utils.utilities import has_feature

from .extracted_text import extracted_texts, nidx_et_cache
from .fields import augment_field
from .paragraphs import augment_paragraph
from .resources import augment_resource, augment_resource_deep


@augmentor_observer.wrap({"type": "augment"})
async def augment(
    kbid: str,
    augmentations: list[Augment],
    *,
    concurrency_control: asyncio.Semaphore | None = None,
) -> Augmented:
    """Process multiple augmentations concurrently and return the augmented content.

    This is a heavy operation that can lead to many I/O operations involving
    maindb blob storage and/or nidx. For improved performance, make sure this is
    called inside the context of `nucliadb.search.search.cache` `request_caches`

    """
    ops = AugmentorOps(kbid)
    await ops.parse(augmentations)
    return await ops.run(concurrency_control=concurrency_control)


async def augment_paragraphs(
    kbid: str,
    given: list[Paragraph],
    select: list[ParagraphProp],
    *,
    concurrency_control: asyncio.Semaphore | None = None,
) -> dict[ParagraphId, AugmentedParagraph]:
    """Augment a list of paragraphs following an augmentation. Paragraphs that
    can't be augmented are not returned.

    """
    augmented = await augment(
        kbid, [ParagraphAugment(given=given, select=select)], concurrency_control=concurrency_control
    )
    return augmented.paragraphs


class AugmentorOps:
    def __init__(self, kbid: str):
        self.kbid = kbid

        # augments
        self.resource_augments: dict[str, list[ResourceProp]] = {}
        self.deep_resource_augments: dict[str, ResourceHydrationOptions] = {}
        self.field_augments: dict[FieldId, list[FieldProp | FileProp | ConversationProp]] = {}
        self.paragraph_augments: dict[ParagraphId, tuple[list[ParagraphProp], Metadata | None]] = {}

        # extracted texts
        self.field_texts: set[FieldId] = set()
        self.paragraph_texts: set[ParagraphId] = set()

    async def parse(self, augmentations: list[Augment]):
        for augmentation in augmentations:
            if augmentation.from_ == "resources":
                self._parse_resource(augmentation)

            elif augmentation.from_ == "resources.deep":
                self._parse_deep_resource(augmentation)

            elif augmentation.from_ == "fields":
                await self._parse_field(augmentation)

            elif augmentation.from_ == "files":
                self._parse_file_field(augmentation)

            elif augmentation.from_ == "conversations":
                self._parse_conversation_field(augmentation)

            elif augmentation.from_ == "paragraphs":
                self._parse_paragraph(augmentation)

            else:  # pragma: no cover
                assert_never(augmentation.from_)

    def _parse_resource(self, augmentation: ResourceAugment) -> None:
        for id in augmentation.given:
            if isinstance(id, str):
                rid = id
            elif isinstance(id, FieldId):
                rid = id.rid
            elif isinstance(id, ParagraphId):
                rid = id.rid
            else:  # pragma: no cover
                assert_never(id)

            self.resource_augments.setdefault(rid, []).extend(augmentation.select)

    def _parse_deep_resource(self, augmentation: DeepResourceAugment) -> None:
        for rid in augmentation.given:
            opts = self.deep_resource_augments.setdefault(rid, ResourceHydrationOptions())
            opts.show.extend(augmentation.show)
            opts.extracted.extend(augmentation.extracted)
            opts.field_type_filter.extend(augmentation.field_type_filter)

    async def _parse_field(self, augmentation: FieldAugment) -> None:
        unfiltered_field_ids: list[FieldId] = []
        for id in augmentation.given:
            if isinstance(id, str):
                # augmenting resource fields
                rid = id
                all_field_ids = await datamanagers.atomic.resources.get_all_field_ids(
                    kbid=self.kbid, rid=rid, for_update=False
                )
                if all_field_ids is None:
                    continue

                unfiltered_field_ids.extend(
                    FieldId.from_pb(rid=rid, field_type=field_id_pb.field_type, key=field_id_pb.field)
                    for field_id_pb in all_field_ids.fields
                )

            elif isinstance(id, FieldId):
                unfiltered_field_ids.append(id)

            elif isinstance(id, ParagraphId):
                unfiltered_field_ids.append(id.field_id)

            else:  # pragma: no cover
                assert_never(id)

        if not augmentation.filter:
            field_ids = unfiltered_field_ids
        else:
            field_ids = []
            for field_id in unfiltered_field_ids:
                for filter in augmentation.filter:
                    if isinstance(filter, nucliadb_models.filters.Field):
                        if filter.type == field_id.type_name and (
                            filter.name is None or filter.name == field_id.key
                        ):
                            field_ids.append(field_id)

                    elif isinstance(filter, nucliadb_models.filters.Generated):
                        # generated fields are always text fields starting with "da-"
                        if field_id.type == FIELD_TYPE_NAME_TO_STR[FieldTypeName.TEXT] and (
                            filter.da_task is None or field_id.key.startswith(f"da-{filter.da_task}-")
                        ):
                            field_ids.append(field_id)

                    else:  # pragma: no cover
                        assert_never(filter)

        for field_id in field_ids:
            self.field_augments.setdefault(field_id, []).extend(augmentation.select)
            self.field_texts.add(field_id)

    def _parse_file_field(self, augmentation: FileAugment) -> None:
        for id in augmentation.given:
            if isinstance(id, FieldId):
                field_id = id
            elif isinstance(id, ParagraphId):
                field_id = id.field_id
            else:  # pragma: no cover
                assert_never(id)

            self.field_augments.setdefault(field_id, []).extend(augmentation.select)
            self.field_texts.add(field_id)

    def _parse_conversation_field(self, augmentation: ConversationAugment) -> None:
        for id in augmentation.given:
            if isinstance(id, FieldId):
                field_id = id
            elif isinstance(id, ParagraphId):
                field_id = id.field_id
            else:  # pragma: no cover
                assert_never(id)

            self.field_augments.setdefault(field_id, []).extend(augmentation.select)

    def _parse_paragraph(self, augmentation: ParagraphAugment) -> None:
        for paragraph in augmentation.given:
            if any((isinstance(prop, ParagraphText) for prop in augmentation.select)):
                self.paragraph_texts.add(paragraph.id)

            select, metadata = self.paragraph_augments.setdefault(paragraph.id, ([], None))
            select.extend(augmentation.select)
            # we keep the first metadata object we see
            metadata = metadata or paragraph.metadata
            self.paragraph_augments[paragraph.id] = (select, metadata)

    async def run(
        self,
        *,
        concurrency_control: asyncio.Semaphore | None = None,
    ) -> Augmented:
        nidx_extracted_texts = None
        if has_feature(
            const.Features.NIDX_AS_EXTRACTED_TEXT_STORAGE,
            context={"kbid": self.kbid, "component": "search"},
        ):
            nidx_extracted_texts = await extracted_texts(
                self.kbid, self.field_texts, self.paragraph_texts
            )

        # we don't care here if nidx has been available or not as the fallback
        # is on the augmentors logic (paragraph and fields)
        token = nidx_et_cache.set(nidx_extracted_texts)
        try:
            augmented = await self._run_augmentations(concurrency_control=concurrency_control)
        finally:
            nidx_et_cache.reset(token)

        return augmented

    async def _run_augmentations(
        self,
        *,
        concurrency_control: asyncio.Semaphore | None = None,
    ) -> Augmented:
        resource_ops = []
        deep_resource_ops = []
        field_ops = []
        paragraph_ops = []

        for rid, resource_select in self.resource_augments.items():
            task = asyncio.create_task(
                limited_concurrency(  # type: ignore[arg-type]
                    augment_resource(self.kbid, rid, resource_select),
                    max_ops=concurrency_control,
                )
            )
            resource_ops.append(task)

        for rid, opts in self.deep_resource_augments.items():
            task = asyncio.create_task(
                limited_concurrency(
                    augment_resource_deep(  # type: ignore[arg-type]
                        self.kbid, rid, opts
                    ),
                    max_ops=concurrency_control,
                )
            )
            deep_resource_ops.append(task)

        for field_id, field_select in self.field_augments.items():
            task = asyncio.create_task(
                limited_concurrency(
                    augment_field(  # type: ignore[arg-type]
                        self.kbid, field_id, field_select
                    ),
                    max_ops=concurrency_control,
                )
            )
            field_ops.append(task)

        for paragraph_id, (paragraph_select, metadata) in self.paragraph_augments.items():
            task = asyncio.create_task(
                limited_concurrency(
                    augment_paragraph(  # type: ignore[arg-type]
                        self.kbid, paragraph_id, paragraph_select, metadata
                    ),
                    max_ops=concurrency_control,
                )
            )
            paragraph_ops.append(task)

        results = await asyncio.gather(*resource_ops, *deep_resource_ops, *field_ops, *paragraph_ops)

        resources = cast(list[AugmentedResource | None], results[: len(resource_ops)])
        del results[: len(resource_ops)]
        resources_deep = cast(list[Resource | None], results[: len(deep_resource_ops)])
        del results[: len(deep_resource_ops)]
        fields = cast(list[AugmentedField | None], results[: len(field_ops)])
        del results[: len(field_ops)]
        paragraphs = cast(list[AugmentedParagraph | None], results[: len(paragraph_ops)])

        return Augmented(
            resources={resource.id: resource for resource in resources if resource is not None},
            resources_deep={
                resource_deep.id: resource_deep
                for resource_deep in resources_deep
                if resource_deep is not None
            },
            fields={field.id: field for field in fields if field is not None},
            paragraphs={paragraph.id: paragraph for paragraph in paragraphs if paragraph is not None},
        )
