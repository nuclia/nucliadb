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
import os

from fastapi import Header, Request
from fastapi.exceptions import HTTPException
from fastapi_versioning import version

from nucliadb.common.ids import ParagraphId
from nucliadb.models.internal.augment import (
    Augment,
    DeepResourceAugment,
    Metadata,
    Paragraph,
    ParagraphAugment,
    ParagraphProp,
    ParagraphText,
)
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.augmentor import augmentor
from nucliadb.search.search.cache import request_caches
from nucliadb_models.augment import (
    AugmentedParagraph,
    AugmentedResource,
    AugmentParagraphs,
    AugmentRequest,
    AugmentResponse,
)
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import NucliaDBClientType
from nucliadb_utils.authentication import requires


class MaliciousStoragePath(Exception):
    """Raised when a path used to access blob storage has a malicious intent
    (e.g., uses ../ to try to access other resources)"""

    ...


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/augment",
    status_code=200,
    description="Augment data on a Knowledge Box",
    include_in_schema=False,
    tags=["Augment"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def _augment_endpoint(
    request: Request,
    kbid: str,
    item: AugmentRequest,
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> AugmentResponse:
    try:
        return await augment_endpoint(kbid, item)
    except MaliciousStoragePath as exc:
        raise HTTPException(
            status_code=422,
            detail=str(exc),
        )


async def augment_endpoint(kbid: str, item: AugmentRequest) -> AugmentResponse:
    augmentations: list[Augment] = []

    if item.resources is not None:
        augmentations.append(
            DeepResourceAugment(
                given=item.resources.given,
                show=item.resources.show,
                extracted=item.resources.extracted,
                field_type_filter=item.resources.field_type_filter,
            )
        )

    if item.paragraphs is not None:
        paragraphs_to_augment, paragraph_selector = parse_paragraph_augment(item.paragraphs)
        augmentations.append(
            ParagraphAugment(
                given=paragraphs_to_augment,
                select=paragraph_selector,
            )
        )

    if len(augmentations) == 0:
        return AugmentResponse(
            resources={},
            paragraphs={},
        )

    with request_caches():
        max_ops = asyncio.Semaphore(50)

        augmented = await augmentor.augment(
            kbid,
            augmentations,
            concurrency_control=max_ops,
        )

        resources = augmented.resources_deep
        paragraphs = augmented.paragraphs

        augmented_resources = {}
        for rid, resource in resources.items():
            if resource is None:
                continue
            augmented_resource = AugmentedResource(id=rid)
            augmented_resource.updated_from(resource)
            augmented_resources[rid] = augmented_resource

        augmented_paragraphs = {}
        for paragraph_id, paragraph in paragraphs.items():
            if paragraph is None:
                continue
            augmented_paragraphs[paragraph_id.full()] = AugmentedParagraph(
                text=paragraph.text,
                # TODO: we need multiple calls to augmentor to fulfill this information
            )

        return AugmentResponse(
            resources=augmented_resources,
            paragraphs=augmented_paragraphs,
        )


def parse_paragraph_augment(item: AugmentParagraphs) -> tuple[list[Paragraph], list[ParagraphProp]]:
    paragraphs_to_augment = []
    for paragraph in item.given:
        try:
            paragraph_id = ParagraphId.from_string(paragraph.id)
        except ValueError:
            # invalid paragraph id, skipping
            continue

        if paragraph.metadata is None:
            metadata = None
        else:
            metadata = Metadata(
                is_an_image=paragraph.metadata.is_an_image,
                is_a_table=paragraph.metadata.is_a_table,
                source_file=paragraph.metadata.source_file,
                page=paragraph.metadata.page,
                in_page_with_visual=paragraph.metadata.in_page_with_visual,
            )

            # metadata provided in the API can't be trusted, we must check for
            # malicious intent
            if metadata.source_file:
                # normalize the path and look for access to parent directories. In a
                # bucket URL, this could mean trying to access another part of the
                # bucket or even another bucket
                normalized = os.path.normpath(metadata.source_file)
                if normalized.startswith("../") or normalized in (".", ".."):
                    raise MaliciousStoragePath(f"Invalid source file path for paragraph {paragraph.id}")

        paragraphs_to_augment.append(Paragraph(id=paragraph_id, metadata=metadata))

    selector: list[ParagraphProp] = []
    if item.text:
        selector.append(ParagraphText())

    return paragraphs_to_augment, selector
