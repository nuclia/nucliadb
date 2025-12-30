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
from typing import Any

from typing_extensions import assert_never

import nucliadb_models
from nucliadb.common import datamanagers
from nucliadb.common.ids import FIELD_TYPE_NAME_TO_STR, FieldId, ParagraphId
from nucliadb.models.internal.augment import (
    Augment,
    Augmented,
    AugmentedField,
    AugmentedParagraph,
    AugmentedResource,
)
from nucliadb.search.augmentor.utils import limited_concurrency
from nucliadb.search.search.hydrator import ResourceHydrationOptions
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import Resource

from .fields import augment_field
from .paragraphs import augment_paragraph
from .resources import augment_resource, augment_resource_deep


async def augment(
    kbid: str,
    augmentations: list[Augment],
    # TODO(decoupled-ask): limits
    *,
    concurrency_control: asyncio.Semaphore | None = None,
) -> Augmented:
    """Process multiple augmentations concurrently and return the augmented content.

    This is a heavy operation that can lead to many I/O operations with maindb
    and/or blob storage. For improved performance, make sure this is called
    inside the context of `nucliadb.search.search.cache` `request_caches`

    """
    augments: dict[str, Any] = {
        "resources": {},
        "resources.deep": {},
        "fields": {},
        "paragraphs": {},
    }
    for augmentation in augmentations:
        if augmentation.from_ == "resources":
            for id in augmentation.given:
                if isinstance(id, str):
                    rid = id
                elif isinstance(id, FieldId):
                    rid = id.rid
                elif isinstance(id, ParagraphId):
                    rid = id.rid
                else:  # pragma: no cover
                    assert_never(id)

                augments["resources"].setdefault(rid, []).extend(augmentation.select)

        elif augmentation.from_ == "resources.deep":
            for rid in augmentation.given:
                opts = augments["resources.deep"].setdefault(rid, ResourceHydrationOptions())
                opts.show.extend(augmentation.show)
                opts.extracted.extend(augmentation.extracted)
                opts.field_type_filter.extend(augmentation.field_type_filter)

        elif augmentation.from_ == "fields":
            unfiltered_field_ids: list[FieldId] = []
            for id in augmentation.given:
                if isinstance(id, str):
                    # augmenting resource fields
                    rid = id
                    all_field_ids = await datamanagers.atomic.resources.get_all_field_ids(
                        kbid=kbid, rid=rid, for_update=False
                    )
                    if all_field_ids is None:
                        continue

                    unfiltered_field_ids.extend(
                        FieldId.from_pb(
                            rid=rid, field_type=field_id_pb.field_type, key=field_id_pb.field
                        )
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
                            if filter.type == field_id.type and (
                                filter.name is None or filter.name == field_id.key
                            ):
                                field_ids.append(field_id)

                        elif isinstance(filter, nucliadb_models.filters.Generated):
                            # generated fields are always text fields starting with "da-"
                            if field_id.type == FIELD_TYPE_NAME_TO_STR[FieldTypeName.TEXT] and (
                                filter.da_task is None
                                or field_id.key.startswith(f"da-{filter.da_task}-")
                            ):
                                field_ids.append(field_id)

                        else:  # pragma: no cover
                            assert_never(filter)

            for field_id in field_ids:
                augments["fields"].setdefault(field_id, []).extend(augmentation.select)

        elif augmentation.from_ == "files" or augmentation.from_ == "conversations":
            for id in augmentation.given:
                if isinstance(id, FieldId):
                    field_id = id
                elif isinstance(id, ParagraphId):
                    field_id = id.field_id
                else:  # pragma: no cover
                    assert_never(id)

                augments["fields"].setdefault(field_id, []).extend(augmentation.select)

        elif augmentation.from_ == "paragraphs":
            for paragraph in augmentation.given:
                select, metadata = augments["paragraphs"].setdefault(paragraph.id, ([], None))
                select.extend(augmentation.select)
                # we keep the first metadata object we see
                metadata = metadata or paragraph.metadata
                augments["paragraphs"][paragraph.id] = (select, metadata)

        else:  # pragma: no cover
            assert_never(augmentation.from_)

    ops = {  # type: ignore[var-annotated]
        "resources": [],
        "resources.deep": [],
        "fields": [],
        "paragraphs": [],
    }
    for rid, select in augments["resources"].items():
        task = asyncio.create_task(
            limited_concurrency(
                augment_resource(  # type: ignore[arg-type]
                    kbid, rid, select
                ),
                max_ops=concurrency_control,
            )
        )
        ops["resources"].append(task)

    for rid, opts in augments["resources.deep"].items():
        task = asyncio.create_task(
            limited_concurrency(
                augment_resource_deep(  # type: ignore[arg-type]
                    kbid, rid, opts
                ),
                max_ops=concurrency_control,
            )
        )
        ops["resources.deep"].append(task)

    for field_id, select in augments["fields"].items():
        task = asyncio.create_task(
            limited_concurrency(
                augment_field(  # type: ignore[arg-type]
                    kbid, field_id, select
                ),
                max_ops=concurrency_control,
            )
        )
        ops["fields"].append(task)

    for paragraph_id, (select, metadata) in augments["paragraphs"].items():
        task = asyncio.create_task(
            limited_concurrency(
                augment_paragraph(  # type: ignore[arg-type]
                    kbid, paragraph_id, select, metadata
                ),
                max_ops=concurrency_control,
            )
        )
        ops["paragraphs"].append(task)

    results = await asyncio.gather(
        *ops["resources"], *ops["resources.deep"], *ops["fields"], *ops["paragraphs"]
    )

    resources: list[AugmentedResource] = results[: len(ops["resources"])]
    del results[: len(ops["resources"])]
    resources_deep: list[Resource] = results[: len(ops["resources.deep"])]
    del results[: len(ops["resources.deep"])]
    fields: list[AugmentedField] = results[: len(ops["fields"])]
    del results[: len(ops["fields"])]
    paragraphs: list[AugmentedParagraph] = results[: len(ops["paragraphs"])]

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
