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

from fastapi import Header, Request
from fastapi_versioning import version

from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb.models.internal import augment as internal_augment
from nucliadb.models.internal.augment import (
    Augment,
    Augmented,
    ConversationAnswerOrAfter,
    ConversationAttachments,
    ConversationAugment,
    ConversationProp,
    ConversationSelector,
    ConversationText,
    DeepResourceAugment,
    FieldAugment,
    FieldClassificationLabels,
    FieldEntities,
    FieldProp,
    FieldText,
    FileAugment,
    FileProp,
    FileThumbnail,
    FullSelector,
    MessageSelector,
    Metadata,
    Paragraph,
    ParagraphAugment,
    ParagraphImage,
    ParagraphPage,
    ParagraphPosition,
    ParagraphProp,
    ParagraphTable,
    ParagraphText,
    RelatedParagraphs,
    ResourceAugment,
    ResourceClassificationLabels,
    ResourceProp,
    ResourceSummary,
    ResourceTitle,
    WindowSelector,
)
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.augmentor import augmentor
from nucliadb.search.search.cache import request_caches
from nucliadb_models.augment import (
    AugmentedConversationField,
    AugmentedConversationMessage,
    AugmentedField,
    AugmentedFileField,
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
    return await augment_endpoint(kbid, item)


async def augment_endpoint(kbid: str, item: AugmentRequest) -> AugmentResponse:
    augmentations = parse_first_augments(item)

    if len(augmentations) == 0:
        return AugmentResponse(resources={}, fields={}, paragraphs={})

    with request_caches():
        max_ops = asyncio.Semaphore(50)

        first_augmented = await augmentor.augment(kbid, augmentations, concurrency_control=max_ops)
        response = build_augment_response(item, first_augmented)

        # 2nd round trip to augmentor
        #
        # There are some augmentations that require some augmented content to be
        # able to keep augmenting, as neighbour paragraphs.
        #
        # However, as many data is already cached (when using cache), this
        # second round should be orders of magnitude faster than the first round.
        #
        augmentations = parse_second_augments(item, first_augmented)
        if len(augmentations) > 0:
            second_augmented = await augmentor.augment(kbid, augmentations, concurrency_control=max_ops)
            merge_second_augment(item, response, second_augmented)

    return response


def parse_first_augments(item: AugmentRequest) -> list[Augment]:
    """Parse an augment request and return a list of internal augments to
    fulfill as much as the requested information as it can.

    Notice there are augments that will require a 2nd round trip to the
    augmentor, e.g., neighbouring paragraphs. This makes code a bit more
    convoluted but avoids synchronization between augments, as many paragraphs
    could lead to the same neighbours.

    """
    augmentations: list[Augment] = []

    if item.resources is not None:
        for resource_augment in item.resources:
            show, extracted, resource_select = parse_deep_resource_augment(resource_augment)
            if resource_augment.field_type_filter is None:
                field_type_filter = list(FieldTypeName)
            else:
                field_type_filter = resource_augment.field_type_filter

            if show:
                augmentations.append(
                    DeepResourceAugment(
                        given=resource_augment.given,
                        show=show,
                        extracted=extracted,
                        field_type_filter=field_type_filter,
                    )
                )
            if resource_select:
                augmentations.append(
                    ResourceAugment(
                        given=resource_augment.given,  # type: ignore[arg-type]
                        select=resource_select,
                    )
                )

            if resource_augment.fields is not None:
                # Augment resource fields with an optional field filter
                field_select: list[FieldProp] = []
                if resource_augment.fields.text:
                    field_select.append(FieldText())
                if resource_augment.fields.classification_labels:
                    field_select.append(FieldClassificationLabels())

                augmentations.append(
                    FieldAugment(
                        given=resource_augment.given,  # type: ignore[arg-type]
                        select=field_select,  # type: ignore[arg-type]
                        filter=resource_augment.fields.filters,
                    )
                )

    if item.fields is not None:
        for field_augment in item.fields:
            given = [FieldId.from_string(id) for id in field_augment.given]
            select: list[FieldProp] = []
            if field_augment.text:
                select.append(FieldText())
            if field_augment.entities:
                select.append(FieldEntities())
            if field_augment.classification_labels:
                select.append(FieldClassificationLabels())

            if len(select) > 0:
                augmentations.append(
                    FieldAugment(
                        given=given,
                        select=select,
                    )
                )

            file_select: list[FileProp] = []
            if field_augment.file_thumbnail:
                file_select.append(FileThumbnail())

            if len(file_select) > 0:
                augmentations.append(
                    FileAugment(
                        given=given,  # type: ignore
                        select=file_select,
                    )
                )

            conversation_select: list[ConversationProp] = []
            selector: ConversationSelector

            if field_augment.full_conversation:
                selector = FullSelector()
                conversation_select.append(ConversationText(selector=selector))
                if (
                    field_augment.conversation_text_attachments
                    or field_augment.conversation_image_attachments
                ):
                    conversation_select.append(ConversationAttachments(selector=selector))

            elif field_augment.max_conversation_messages is not None:
                # we want to always get the first conversation and the window
                # requested by the user
                first_selector = MessageSelector(index="first")
                window_selector = WindowSelector(size=field_augment.max_conversation_messages)
                conversation_select.append(ConversationText(selector=first_selector))
                conversation_select.append(ConversationText(selector=window_selector))
                if (
                    field_augment.conversation_text_attachments
                    or field_augment.conversation_image_attachments
                ):
                    conversation_select.append(ConversationAttachments(selector=first_selector))
                    conversation_select.append(ConversationAttachments(selector=window_selector))

            if field_augment.conversation_answer_or_messages_after:
                conversation_select.append(ConversationAnswerOrAfter())

            if len(conversation_select) > 0:
                augmentations.append(
                    ConversationAugment(
                        given=given,  # type: ignore
                        select=conversation_select,
                    )
                )

    if item.paragraphs is not None:
        for paragraph_augment in item.paragraphs:
            paragraphs_to_augment, paragraph_selector = parse_paragraph_augment(paragraph_augment)
            augmentations.append(
                ParagraphAugment(
                    given=paragraphs_to_augment,
                    select=paragraph_selector,
                )
            )

    return augmentations


def parse_deep_resource_augment(
    item: AugmentResources,
) -> tuple[list[ResourceProperties], list[ExtractedDataTypeName], list[ResourceProp]]:
    show = []
    if item.basic:
        show.append(ResourceProperties.BASIC)
    if item.origin:
        show.append(ResourceProperties.ORIGIN)
    if item.extra:
        show.append(ResourceProperties.EXTRA)
    if item.relations:
        show.append(ResourceProperties.RELATIONS)
    if item.values:
        show.append(ResourceProperties.VALUES)
    if item.errors:
        show.append(ResourceProperties.ERRORS)
    if item.security:
        show.append(ResourceProperties.SECURITY)

    extracted = []
    if item.extracted_text:
        extracted.append(ExtractedDataTypeName.TEXT)
    if item.extracted_metadata:
        extracted.append(ExtractedDataTypeName.METADATA)
    if item.extracted_shortened_metadata:
        extracted.append(ExtractedDataTypeName.SHORTENED_METADATA)
    if item.extracted_large_metadata:
        extracted.append(ExtractedDataTypeName.LARGE_METADATA)
    if item.extracted_vector:
        extracted.append(ExtractedDataTypeName.VECTOR)
    if item.extracted_link:
        extracted.append(ExtractedDataTypeName.LINK)
    if item.extracted_file:
        extracted.append(ExtractedDataTypeName.FILE)
    if item.extracted_qa:
        extracted.append(ExtractedDataTypeName.QA)

    if len(extracted) > 0:
        show.append(ResourceProperties.EXTRACTED)

    select: list[ResourceProp] = []
    if item.title:
        select.append(ResourceTitle())
    if item.summary:
        select.append(ResourceSummary())
    if item.classification_labels:
        select.append(ResourceClassificationLabels())

    return (
        show,
        extracted,
        select,
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

        paragraphs_to_augment.append(Paragraph(id=paragraph_id, metadata=metadata))

    selector: list[ParagraphProp] = []
    if item.text:
        selector.append(ParagraphText())
    if item.neighbours_before or item.neighbours_after:
        selector.append(
            RelatedParagraphs(
                neighbours_before=item.neighbours_before or 0,
                neighbours_after=item.neighbours_after or 0,
            )
        )
    if item.source_image:
        selector.append(ParagraphImage())
    if item.table_image:
        selector.append(ParagraphTable(prefer_page_preview=item.table_prefers_page_preview))
    if item.page_preview_image:
        selector.append(ParagraphPage(preview=True))

    return paragraphs_to_augment, selector


def build_augment_response(item: AugmentRequest, augmented: Augmented) -> AugmentResponse:
    response = AugmentResponse(
        resources={},
        fields={},
        paragraphs={},
    )

    # start with deep resources, as they return a Resource object we can merge
    # with the augmented model
    for rid, resource_deep in augmented.resources_deep.items():
        if resource_deep is None:
            continue

        augmented_resource = AugmentedResource(id=rid)
        augmented_resource.updated_from(resource_deep)
        response.resources[rid] = augmented_resource

    # now we can cherry pick properties from the augmented resources and merge
    # them with the deep ones
    for rid, resource in augmented.resources.items():
        if resource is None:
            continue

        augmented_resource = response.resources.setdefault(rid, AugmentedResource(id=rid))

        # merge resource with deep resources without overwriting
        augmented_resource.title = augmented_resource.title or resource.title
        augmented_resource.summary = augmented_resource.summary or resource.summary

        # properties original to the augmented resources (not in deep resource augment)
        if resource.classification_labels is not None:
            augmented_resource.classification_labels = {
                labelset: list(labels) for labelset, labels in resource.classification_labels.items()
            }

    for field_id, field in augmented.fields.items():
        if field is None:
            continue

        # common augments for all fields

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

        if field_id.type in (
            FieldTypeName.TEXT.abbreviation(),
            FieldTypeName.LINK.abbreviation(),
            FieldTypeName.GENERIC.abbreviation(),
        ):
            response.fields[field_id.full()] = AugmentedField(
                text=field.text,  # type: ignore # field is instance of any of the above and has the text property
                classification_labels=classification_labels,
                entities=entities,
            )

        elif field_id.type == FieldTypeName.FILE.abbreviation():
            field = cast(internal_augment.AugmentedFileField, field)
            response.fields[field_id.full()] = AugmentedFileField(
                text=field.text,  # type: ignore # field is instance of any of the above and has the text property
                classification_labels=classification_labels,
                entities=entities,
                thumbnail_image=field.thumbnail_path,
            )

        elif field_id.type == FieldTypeName.CONVERSATION.abbreviation():
            field = cast(internal_augment.AugmentedConversationField, field)
            conversation = AugmentedConversationField(
                classification_labels=classification_labels,
                entities=entities,
            )

            if field.messages is not None:
                conversation.messages = []
                for m in field.messages:
                    if m.attachments is None:
                        attachments = None
                    else:
                        attachments = []
                        for f in m.attachments:
                            attachments.append(f.full())

                    conversation.messages.append(
                        AugmentedConversationMessage(
                            ident=m.ident,
                            text=m.text,
                            attachments=attachments,
                        )
                    )

            response.fields[field_id.full()] = conversation

        else:  # pragma: no cover
            assert False, f"unknown field type: {field_id.type}"

    for paragraph_id, paragraph in augmented.paragraphs.items():
        if paragraph is None:
            continue

        augmented_paragraph = AugmentedParagraph()
        augmented_paragraph.text = paragraph.text
        if paragraph.related is not None:
            augmented_paragraph.neighbours_before = list(
                map(lambda x: x.full(), paragraph.related.neighbours_before)
            )
            augmented_paragraph.neighbours_after = list(
                map(lambda x: x.full(), paragraph.related.neighbours_after)
            )
        augmented_paragraph.source_image = paragraph.source_image_path
        augmented_paragraph.table_image = paragraph.table_image_path
        augmented_paragraph.page_preview_image = paragraph.page_preview_path
        response.paragraphs[paragraph_id.full()] = augmented_paragraph

    return response


def parse_second_augments(item: AugmentRequest, augmented: Augmented) -> list[Augment]:
    """Given an augment request an a first augmentation, return a list of
    augments required to fulfill the requested data.

    """
    augmentations: list[Augment] = []

    for paragraph_augment in item.paragraphs or []:
        if paragraph_augment.neighbours_before or paragraph_augment.neighbours_after:
            neighbours = []
            for paragraph_id, paragraph in augmented.paragraphs.items():
                if paragraph.related is not None:
                    for neighbour_before in paragraph.related.neighbours_before:
                        neighbours.append(Paragraph(id=neighbour_before, metadata=None))
                    for neighbour_after in paragraph.related.neighbours_after:
                        neighbours.append(Paragraph(id=neighbour_after, metadata=None))

            if neighbours:
                augmentations.append(
                    ParagraphAugment(
                        given=neighbours,
                        select=[
                            ParagraphText(),
                            ParagraphPosition(),
                        ],
                    )
                )

    return augmentations


def merge_second_augment(item: AugmentRequest, response: AugmentResponse, augmented: Augmented):
    """Merge in-place augmented data with an existing augment response."""

    if any(
        (
            paragraph_augment.neighbours_before or paragraph_augment.neighbours_after
            for paragraph_augment in item.paragraphs or []
        )
    ):
        # neighbour paragraphs

        new_paragraphs = {}
        for paragraph_id_str, augmented_paragraph in response.paragraphs.items():
            before_refs = []
            for before_id_str in augmented_paragraph.neighbours_before or []:
                before_id = ParagraphId.from_string(before_id_str)

                if before_id not in augmented.paragraphs:
                    continue
                neighbour = augmented.paragraphs[before_id]

                if before_id_str not in response.paragraphs:
                    if not neighbour.text and not neighbour.position:
                        continue
                    # create a new paragraph for the neighbour
                    new_paragraphs[before_id_str] = AugmentedParagraph(
                        text=neighbour.text, position=neighbour.position
                    )

                else:
                    # merge neighbour with existing paragraph
                    if not response.paragraphs[before_id_str].text:
                        response.paragraphs[before_id_str].text = neighbour.text

                before_refs.append(before_id_str)

            after_refs = []
            for after_id_str in augmented_paragraph.neighbours_after or []:
                after_id = ParagraphId.from_string(after_id_str)

                if after_id not in augmented.paragraphs:
                    continue
                neighbour = augmented.paragraphs[after_id]

                if after_id_str not in response.paragraphs:
                    if not neighbour.text and not neighbour.position:
                        continue
                    # create a new paragraph for the neighbour
                    new_paragraphs[after_id_str] = AugmentedParagraph(
                        text=neighbour.text, position=neighbour.position
                    )

                else:
                    # merge neighbour with existing paragraph
                    if not response.paragraphs[after_id_str].text:
                        response.paragraphs[after_id_str].text = neighbour.text

                after_refs.append(after_id_str)

            # update references to contain only the neighbours that existed in
            # the response or we added
            augmented_paragraph.neighbours_before = before_refs
            augmented_paragraph.neighbours_after = after_refs

        response.paragraphs.update(new_paragraphs)
