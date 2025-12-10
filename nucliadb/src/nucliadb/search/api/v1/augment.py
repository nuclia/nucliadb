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

import nucliadb_models
from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb.models.internal.augment import (
    Augment,
    DeepResourceAugment,
    FieldAugment,
    FieldClassificationLabels,
    FieldEntities,
    FieldProp,
    FieldText,
    Metadata,
    Paragraph,
    ParagraphAugment,
    ParagraphProp,
    ParagraphText,
    ResourceAugment,
    ResourceClassificationLabels,
    ResourceProp,
    ResourceSummary,
    ResourceTitle,
)
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.augmentor import augmentor
from nucliadb.search.search.cache import request_caches
from nucliadb_models.augment import (
    AugmentedField,
    AugmentedParagraph,
    AugmentedResource,
    AugmentParagraphs,
    AugmentRequest,
    AugmentResources,
    AugmentResponse,
)
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName, NucliaDBRoles
from nucliadb_models.search import NucliaDBClientType, ResourceProperties
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
        show, extracted, resource_select = parse_deep_resource_augment(item.resources)
        if item.resources.field_type_filter is None:
            field_type_filter = list(FieldTypeName)
        else:
            field_type_filter = item.resources.field_type_filter

        if show:
            augmentations.append(
                DeepResourceAugment(
                    given=item.resources.given,
                    show=show,
                    extracted=extracted,
                    field_type_filter=field_type_filter,
                )
            )
        if resource_select:
            augmentations.append(
                ResourceAugment(
                    given=item.resources.given,  # type: ignore[arg-type]
                    select=resource_select,
                )
            )

        if item.resources.fields is not None:
            # Augment resource fields with an optional field filter
            field_select: list[FieldProp] = []
            if item.resources.fields.text:
                field_select.append(FieldText())
            # TODO: add missing test for field classification labels...
            if item.resources.fields.classification_labels:
                field_select.append(FieldClassificationLabels())

            augmentations.append(
                FieldAugment(
                    given=item.resources.given,  # type: ignore[arg-type]
                    select=field_select,  # type: ignore[arg-type]
                    filter=item.resources.fields.filters,
                )
            )

    if item.fields is not None:
        select: list[FieldProp] = []
        if item.fields.text:
            select.append(FieldText())
        if item.fields.entities:
            select.append(FieldEntities())
        if item.fields.classification_labels:
            select.append(FieldClassificationLabels())

        augmentations.append(
            FieldAugment(
                given=[FieldId.from_string(id) for id in item.fields.given],
                select=select,
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
            fields={},
            paragraphs={},
        )

    with request_caches():
        max_ops = asyncio.Semaphore(50)

        augmented = await augmentor.augment(
            kbid,
            augmentations,
            concurrency_control=max_ops,
        )

        augmented_resources = {}
        for rid, resource_deep in augmented.resources_deep.items():
            if resource_deep is None:
                continue
            augmented_resource = AugmentedResource(id=rid)
            augmented_resource.updated_from(resource_deep)
            augmented_resources[rid] = augmented_resource

        for rid, resource in augmented.resources.items():
            if resource is None:
                continue
            augmented_resource = augmented_resources.setdefault(rid, AugmentedResource(id=rid))
            augmented_resource.title = augmented_resource.title or resource.title
            augmented_resource.summary = augmented_resource.summary or resource.summary
            if resource.classification_labels is not None:
                augmented_resource.classification_labels = {
                    labelset: list(labels) for labelset, labels in resource.classification_labels.items()
                }
            # TODO: more properties

        augmented_fields = {}
        for field_id, field in augmented.fields.items():
            if field is None:
                continue

            if field.classification_labels is None:
                classification_labels = None
            else:
                classification_labels = {
                    labelset: list(labels) for labelset, labels in field.classification_labels.items()
                }

            if field.entities is None:
                entities = None
            else:
                entities = {family: list(entity) for family, entity in field.entities.items()}

            augmented_fields[field_id.full()] = AugmentedField(
                text=field.text,
                classification_labels=classification_labels,
                entities=entities,
                # TODO: more parameters
            )

        augmented_paragraphs = {}
        for paragraph_id, paragraph in augmented.paragraphs.items():
            if paragraph is None:
                continue

            augmented_paragraphs[paragraph_id.full()] = AugmentedParagraph(
                text=paragraph.text,
                # TODO: we need multiple calls to augmentor to fulfill this information
            )

        return AugmentResponse(
            resources=augmented_resources,
            fields=augmented_fields,
            paragraphs=augmented_paragraphs,
        )


def parse_deep_resource_augment(
    item: AugmentResources,
) -> tuple[list[ResourceProperties], list[ExtractedDataTypeName], list[ResourceProp]]:
    show = []
    show_extracted = False
    extracted = []
    select = []

    _resource_prop_to_show = {
        nucliadb_models.augment.ResourceProp.BASIC: ResourceProperties.BASIC,
        nucliadb_models.augment.ResourceProp.ORIGIN: ResourceProperties.ORIGIN,
        nucliadb_models.augment.ResourceProp.EXTRA: ResourceProperties.EXTRA,
        nucliadb_models.augment.ResourceProp.RELATIONS: ResourceProperties.RELATIONS,
        nucliadb_models.augment.ResourceProp.VALUES: ResourceProperties.VALUES,
        nucliadb_models.augment.ResourceProp.ERRORS: ResourceProperties.ERRORS,
        nucliadb_models.augment.ResourceProp.SECURITY: ResourceProperties.SECURITY,
    }
    _resource_prop_to_extracted = {
        nucliadb_models.augment.ResourceProp.EXTRACTED_TEXT: ExtractedDataTypeName.TEXT,
        nucliadb_models.augment.ResourceProp.EXTRACTED_METADATA: ExtractedDataTypeName.METADATA,
        nucliadb_models.augment.ResourceProp.EXTRACTED_SHORTENED_METADATA: ExtractedDataTypeName.SHORTENED_METADATA,
        nucliadb_models.augment.ResourceProp.EXTRACTED_LARGE_METADATA: ExtractedDataTypeName.LARGE_METADATA,
        nucliadb_models.augment.ResourceProp.EXTRACTED_VECTOR: ExtractedDataTypeName.VECTOR,
        nucliadb_models.augment.ResourceProp.EXTRACTED_LINK: ExtractedDataTypeName.LINK,
        nucliadb_models.augment.ResourceProp.EXTRACTED_FILE: ExtractedDataTypeName.FILE,
        nucliadb_models.augment.ResourceProp.EXTRACTED_QA: ExtractedDataTypeName.QA,
    }
    _resource_prop_to_prop = {
        nucliadb_models.augment.ResourceProp.TITLE: ResourceTitle(),
        nucliadb_models.augment.ResourceProp.SUMMARY: ResourceSummary(),
        nucliadb_models.augment.ResourceProp.CLASSIFICATION_LABELS: ResourceClassificationLabels(),
    }
    for prop in item.select:
        if prop in _resource_prop_to_show:
            show.append(_resource_prop_to_show[prop])
        elif prop in _resource_prop_to_extracted:
            show_extracted = True
            extracted.append(_resource_prop_to_extracted[prop])
        elif prop in _resource_prop_to_prop:
            select.append(_resource_prop_to_prop[prop])

    if show_extracted:
        show.append(ResourceProperties.EXTRACTED)

    return (
        show,
        extracted,
        select,  # type: ignore
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
