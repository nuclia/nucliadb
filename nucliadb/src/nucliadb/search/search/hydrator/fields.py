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


from nucliadb.common.ids import FIELD_TYPE_STR_TO_NAME, FieldId
from nucliadb.common.models_utils import from_proto
from nucliadb.ingest.orm.resource import Resource
from nucliadb.search.search.hydrator import hydrate_field_text
from nucliadb_models import hydration as hydration_models
from nucliadb_models.common import FieldTypeName


def page_preview_id(page_number: int) -> str:
    """Return the string page number for an specific page"""
    return f"{page_number}"


async def hydrate_field(resource: Resource, field_id: FieldId, config: hydration_models.FieldHydration):
    field_type = FIELD_TYPE_STR_TO_NAME[field_id.type]

    if field_type == FieldTypeName.TEXT:
        if not config.text is not None:
            return
        return await hydrate_text_field(resource, field_id, config.text)

    elif field_type == FieldTypeName.FILE is not None:
        if not config.file:
            return
        return await hydrate_file_field(resource, field_id, config.file)

    elif field_type == FieldTypeName.LINK is not None:
        if not config.link:
            return
        return await hydrate_link_field(resource, field_id, config.link)

    elif field_type == FieldTypeName.CONVERSATION is not None:
        if not config.conversation:
            return
        return await hydrate_conversation_field(resource, field_id, config.conversation)

    elif field_type == FieldTypeName.GENERIC is not None:
        if not config.generic:
            return
        return await hydrate_generic_field(resource, field_id, config.generic)

    else:  # pragma: no cover
        # This is a trick so mypy generates an error if this branch can be reached,
        # that is, if we are missing some ifs
        _a: int = "a"


async def hydrate_text_field(
    resource: Resource,
    field_id: FieldId,
    config: hydration_models.TextFieldHydration,
) -> hydration_models.HydratedTextField:
    hydrated = hydration_models.HydratedTextField(
        id=field_id.full(),
        resource=field_id.rid,
        field_type=FieldTypeName.TEXT,
    )

    if config.extracted_text:
        field_text = await hydrate_field_text(resource.kb.kbid, field_id)
        if field_text is not None:
            (_, text) = field_text
            hydrated.extracted = hydration_models.FieldExtractedData(text=text)

    return hydrated


async def hydrate_file_field(
    resource: Resource,
    field_id: FieldId,
    config: hydration_models.FileFieldHydration,
) -> hydration_models.HydratedFileField:
    hydrated = hydration_models.HydratedFileField(
        id=field_id.full(),
        resource=field_id.rid,
        field_type=FieldTypeName.FILE,
    )

    if config.value:
        field = await resource.get_field(field_id.key, field_id.pb_type)
        value = await field.get_value()
        hydrated.value = from_proto.field_file(value)

    if config.extracted_text:
        field_text = await hydrate_field_text(resource.kb.kbid, field_id)
        if field_text is not None:
            (_, text) = field_text
            hydrated.extracted = hydration_models.FieldExtractedData(text=text)

    return hydrated


async def hydrate_link_field(
    resource: Resource,
    field_id: FieldId,
    config: hydration_models.LinkFieldHydration,
) -> hydration_models.HydratedLinkField:
    hydrated = hydration_models.HydratedLinkField(
        id=field_id.full(),
        resource=field_id.rid,
        field_type=FieldTypeName.LINK,
    )

    if config.value:
        field = await resource.get_field(field_id.key, field_id.pb_type)
        value = await field.get_value()
        hydrated.value = from_proto.field_link(value)

    if config.extracted_text:
        field_text = await hydrate_field_text(resource.kb.kbid, field_id)
        if field_text is not None:
            (_, text) = field_text
            hydrated.extracted = hydration_models.FieldExtractedData(text=text)

    return hydrated


async def hydrate_conversation_field(
    resource: Resource,
    field_id: FieldId,
    config: hydration_models.ConversationFieldHydration,
) -> hydration_models.HydratedConversationField:
    hydrated = hydration_models.HydratedConversationField(
        id=field_id.full(),
        resource=field_id.rid,
        field_type=FieldTypeName.CONVERSATION,
    )
    # TODO: implement conversation fields
    return hydrated


async def hydrate_generic_field(
    resource: Resource,
    field_id: FieldId,
    config: hydration_models.GenericFieldHydration,
) -> hydration_models.HydratedGenericField:
    hydrated = hydration_models.HydratedGenericField(
        id=field_id.full(),
        resource=field_id.rid,
        field_type=FieldTypeName.GENERIC,
    )

    if config.value:
        field = await resource.get_field(field_id.key, field_id.pb_type)
        value = await field.get_value()
        hydrated.value = value

    if config.extracted_text:
        field_text = await hydrate_field_text(resource.kb.kbid, field_id)
        if field_text is not None:
            (_, text) = field_text
            hydrated.extracted = hydration_models.FieldExtractedData(text=text)

    return hydrated
