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
import re

from nucliadb.common import ids
from nucliadb.models.internal.augment import (
    Augment,
    FieldIdPattern,
    ParagraphIdPattern,
    ResourceIdPattern,
)
from nucliadb.search.augmentor.models import (
    Augmented,
    AugmentedField,
    AugmentedParagraph,
    AugmentedResource,
)
from nucliadb.search.augmentor.utils import limited_concurrency
from nucliadb.search.search import cache

from .fields import augment_field
from .paragraphs import augment_paragraph
from .resources import augment_resource


async def augment(
    kbid: str,
    augmentations: list[Augment],
    # TODO: limits
    *,
    concurrency_control: asyncio.Semaphore | None = None,
) -> Augmented:
    with cache.request_caches():
        augments = {  # type: ignore[var-annotated]
            "resources": {},
            "fields": {},
            "paragraphs": {},
        }
        for augmentation in augmentations:
            if augmentation.from_ == "resources":
                for id in augmentation.given:
                    if re.match(ResourceIdPattern, id):
                        rid = id
                    elif re.match(FieldIdPattern, id):
                        rid = ids.FieldId.from_string(id).rid
                    elif re.match(ParagraphIdPattern, id):
                        rid = ids.ParagraphId.from_string(id).rid
                    else:  # pragma: no cover
                        raise ValueError(f"unexpected id: {id}")

                    augments["resources"].setdefault(rid, []).extend(augmentation.select)

            elif augmentation.from_ == "fields":
                raise NotImplementedError()

            elif augmentation.from_ == "conversations":
                raise NotImplementedError()

            elif augmentation.from_ == "paragraphs":
                for id in augmentation.given:
                    paragraph_id = ids.ParagraphId.from_string(id)
                    augments["paragraphs"].setdefault(paragraph_id, []).extend(augmentation.select)

            else:  # pragma: no cover
                # This is a trick so mypy generates an error if this branch can be reached,
                # that is, if we are missing some ifs
                _a: int = "a"

        ops = {  # type: ignore[var-annotated]
            "resources": [],
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

        for paragraph_id, select in augments["paragraphs"].items():
            task = asyncio.create_task(
                limited_concurrency(
                    augment_paragraph(  # type: ignore[arg-type]
                        kbid, paragraph_id, select, None
                    ),
                    max_ops=concurrency_control,
                )
            )
            ops["paragraphs"].append(task)

        results = await asyncio.gather(*ops["resources"], *ops["fields"], *ops["paragraphs"])

        resources: list[AugmentedResource] = results[: len(ops["resources"])]
        del results[: len(ops["resources"])]
        fields: list[AugmentedField] = results[: len(ops["fields"])]
        del results[: len(ops["fields"])]
        paragraphs: list[AugmentedParagraph] = results[: len(ops["paragraphs"])]
        del results[: len(ops["paragraphs"])]

        return Augmented(
            resources={resource.id: resource for resource in resources},
            fields={field.id: field for field in fields},
            paragraphs={paragraph.id: paragraph for paragraph in paragraphs},
        )
