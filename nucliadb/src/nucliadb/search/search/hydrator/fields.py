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
from typing import cast

from typing_extensions import assert_never

from nucliadb.common.ids import FIELD_TYPE_STR_TO_NAME, FieldId
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.fields.file import File
from nucliadb.ingest.fields.generic import Generic
from nucliadb.ingest.fields.link import Link
from nucliadb.ingest.fields.text import Text
from nucliadb.models.internal.augment import ConversationProp, FieldProp, FieldText, FieldValue
from nucliadb.search.augmentor.fields import (
    db_augment_conversation_field,
    db_augment_file_field,
    db_augment_generic_field,
    db_augment_link_field,
    db_augment_text_field,
)
from nucliadb_models import hydration as hydration_models
from nucliadb_models.common import FieldTypeName


def page_preview_id(page_number: int) -> str:
    """Return the string page number for an specific page"""
    return f"{page_number}"


async def hydrate_field(field: Field, field_id: FieldId, config: hydration_models.FieldHydration):
    field_type = FIELD_TYPE_STR_TO_NAME[field_id.type]

    if field_type == FieldTypeName.TEXT:
        if not config.text is not None:
            return
        field = cast(Text, field)
        return await hydrate_text_field(field, field_id, config.text)

    elif field_type == FieldTypeName.FILE is not None:
        if not config.file:
            return
        field = cast(File, field)
        return await hydrate_file_field(field, field_id, config.file)

    elif field_type == FieldTypeName.LINK is not None:
        if not config.link:
            return
        field = cast(Link, field)
        return await hydrate_link_field(field, field_id, config.link)

    elif field_type == FieldTypeName.CONVERSATION is not None:
        if not config.conversation:
            return
        field = cast(Conversation, field)
        return await hydrate_conversation_field(field, field_id, config.conversation)

    elif field_type == FieldTypeName.GENERIC is not None:
        if not config.generic:
            return
        field = cast(Generic, field)
        return await hydrate_generic_field(field, field_id, config.generic)

    else:  # pragma: no cover
        assert_never(field_type)


async def hydrate_text_field(
    field: Text,
    field_id: FieldId,
    config: hydration_models.TextFieldHydration,
) -> hydration_models.HydratedTextField:
    select: list[FieldProp] = []
    if config.value:
        select.append(FieldValue())
    if config.extracted_text:
        select.append(FieldText())

    augmented = await db_augment_text_field(field, field_id, select)

    hydrated = hydration_models.HydratedTextField(
        id=field_id.full(),
        resource=field_id.rid,
        field_type=FieldTypeName.TEXT,
    )

    if config.value and augmented.value:
        hydrated.value = augmented.value

    if config.extracted_text and augmented.text:
        hydrated.extracted = hydration_models.FieldExtractedData(text=augmented.text)

    return hydrated


async def hydrate_file_field(
    field: File,
    field_id: FieldId,
    config: hydration_models.FileFieldHydration,
) -> hydration_models.HydratedFileField:
    select: list[FieldProp] = []
    if config.value:
        select.append(FieldValue())
    if config.extracted_text:
        select.append(FieldText())

    augmented = await db_augment_file_field(field, field_id, select)

    hydrated = hydration_models.HydratedFileField(
        id=field_id.full(),
        resource=field_id.rid,
        field_type=FieldTypeName.FILE,
    )

    if config.value and augmented.value:
        hydrated.value = augmented.value

    if config.extracted_text and augmented.text:
        hydrated.extracted = hydration_models.FieldExtractedData(text=augmented.text)

    return hydrated


async def hydrate_link_field(
    field: Link,
    field_id: FieldId,
    config: hydration_models.LinkFieldHydration,
) -> hydration_models.HydratedLinkField:
    select: list[FieldProp] = []
    if config.value:
        select.append(FieldValue())
    if config.extracted_text:
        select.append(FieldText())

    augmented = await db_augment_link_field(field, field_id, select)

    hydrated = hydration_models.HydratedLinkField(
        id=field_id.full(),
        resource=field_id.rid,
        field_type=FieldTypeName.LINK,
    )

    if config.value and augmented.value:
        hydrated.value = augmented.value

    if config.extracted_text and augmented.text:
        hydrated.extracted = hydration_models.FieldExtractedData(text=augmented.text)

    return hydrated


async def hydrate_conversation_field(
    field: Conversation,
    field_id: FieldId,
    config: hydration_models.ConversationFieldHydration,
) -> hydration_models.HydratedConversationField:
    select: list[ConversationProp] = []
    if config.value:
        select.append(FieldValue())

    augmented = await db_augment_conversation_field(field, field_id, select)

    hydrated = hydration_models.HydratedConversationField(
        id=field_id.full(),
        resource=field_id.rid,
        field_type=FieldTypeName.CONVERSATION,
    )

    if config.value and augmented.value:
        hydrated.value = augmented.value

    return hydrated


async def hydrate_generic_field(
    field: Generic,
    field_id: FieldId,
    config: hydration_models.GenericFieldHydration,
) -> hydration_models.HydratedGenericField:
    select: list[FieldProp] = []
    if config.value:
        select.append(FieldValue())
    if config.extracted_text:
        select.append(FieldText())

    augmented = await db_augment_generic_field(field, field_id, select)

    hydrated = hydration_models.HydratedGenericField(
        id=field_id.full(),
        resource=field_id.rid,
        field_type=FieldTypeName.GENERIC,
    )

    if config.value and augmented.value:
        hydrated.value = augmented.value

    if config.extracted_text and augmented.text:
        hydrated.extracted = hydration_models.FieldExtractedData(text=augmented.text)

    return hydrated
