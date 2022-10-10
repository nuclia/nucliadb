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

from enum import Enum
from typing import List, Optional

import nucliadb.models as models
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.fields.file import File
from nucliadb.ingest.fields.link import Link
from nucliadb.ingest.maindb.driver import Transaction
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.resource import Resource as ORMResource
from nucliadb.ingest.utils import get_driver
from nucliadb.models.common import FIELD_TYPES_MAP, FieldTypeName
from nucliadb.models.resource import (
    ConversationFieldData,
    ConversationFieldExtractedData,
    DatetimeFieldData,
    DatetimeFieldExtractedData,
    Error,
    ExtractedDataType,
    FileFieldData,
    FileFieldExtractedData,
    KeywordsetFieldData,
    KeywordsetFieldExtractedData,
    LayoutFieldData,
    LayoutFieldExtractedData,
    LinkFieldData,
    LinkFieldExtractedData,
    Resource,
    ResourceData,
    TextFieldData,
    TextFieldExtractedData,
)
from nucliadb_utils.utilities import get_cache, get_storage


class ResourceProperties(str, Enum):
    BASIC = "basic"
    ORIGIN = "origin"
    RELATIONS = "relations"
    VALUES = "values"
    EXTRACTED = "extracted"
    ERRORS = "errors"


class ResourceFieldProperties(str, Enum):
    VALUE = "value"
    EXTRACTED = "extracted"
    ERROR = "error"


class ExtractedDataTypeName(str, Enum):
    TEXT = "text"
    METADATA = "metadata"
    LARGE_METADATA = "large_metadata"
    VECTOR = "vectors"
    LINK = "link"
    FILE = "file"


async def set_resource_field_extracted_data(
    field: Field,
    field_data: ExtractedDataType,
    field_type_name: FieldTypeName,
    wanted_extracted_data: List[ExtractedDataTypeName],
) -> None:

    if field_data is None:
        return

    if ExtractedDataTypeName.TEXT in wanted_extracted_data:
        data_et = await field.get_extracted_text()
        if data_et is not None:
            field_data.text = models.ExtractedText.from_message(data_et)

    if ExtractedDataTypeName.METADATA in wanted_extracted_data:
        data_fcm = await field.get_field_metadata()

        if data_fcm is not None:
            field_data.metadata = models.FieldComputedMetadata.from_message(data_fcm)

    if ExtractedDataTypeName.LARGE_METADATA in wanted_extracted_data:
        data_lcm = await field.get_large_field_metadata()
        if data_lcm is not None:
            field_data.large_metadata = models.LargeComputedMetadata.from_message(
                data_lcm
            )

    if ExtractedDataTypeName.VECTOR in wanted_extracted_data:
        data_vec = await field.get_vectors()
        if data_vec is not None:
            field_data.vectors = models.VectorObject.from_message(data_vec)

    if (
        isinstance(field, File)
        and isinstance(field_data, FileFieldExtractedData)
        and ExtractedDataTypeName.FILE in wanted_extracted_data
    ):
        data_fed = await field.get_file_extracted_data()
        if data_fed is not None:
            field_data.file = models.FileExtractedData.from_message(data_fed)

    if (
        isinstance(field, Link)
        and isinstance(field_data, LinkFieldExtractedData)
        and ExtractedDataTypeName.LINK in wanted_extracted_data
    ):
        data_led = await field.get_link_extracted_data()
        if data_led is not None:
            field_data.link = models.LinkExtractedData.from_message(data_led)


async def serialize(
    kbid: str,
    rid: Optional[str],
    show: List[ResourceProperties],
    field_type_filter: List[FieldTypeName],
    extracted: List[ExtractedDataTypeName],
    service_name: Optional[str] = None,
    slug: Optional[str] = None,
) -> Optional[Resource]:

    driver = await get_driver()
    txn = await driver.begin()

    orm_resource = await get_orm_resource(
        txn, kbid, rid=rid, slug=slug, service_name=service_name
    )
    if orm_resource is None:
        await txn.abort()
        return None

    resource = Resource(id=orm_resource.uuid)

    include_values = ResourceProperties.VALUES in show

    include_extracted_data = (
        ResourceProperties.EXTRACTED in show and extracted is not []
    )

    if ResourceProperties.BASIC in show:
        await orm_resource.get_basic()

        if orm_resource.basic is not None:

            resource.title = orm_resource.basic.title
            resource.summary = orm_resource.basic.summary
            resource.icon = orm_resource.basic.icon
            resource.layout = orm_resource.basic.layout
            resource.thumbnail = orm_resource.basic.thumbnail
            resource.created = (
                orm_resource.basic.created.ToDatetime()
                if orm_resource.basic.HasField("created")
                else None
            )
            resource.modified = (
                orm_resource.basic.modified.ToDatetime()
                if orm_resource.basic.HasField("modified")
                else None
            )

            resource.metadata = models.Metadata.from_message(
                orm_resource.basic.metadata
            )
            resource.usermetadata = models.UserMetadata.from_message(
                orm_resource.basic.usermetadata
            )
            resource.fieldmetadata = [
                models.UserFieldMetadata.from_message(fm)
                for fm in orm_resource.basic.fieldmetadata
            ]

            resource.last_seqid = orm_resource.basic.last_seqid

    if ResourceProperties.RELATIONS in show:
        await orm_resource.get_relations()
        if orm_resource.relations is not None:
            resource.relations = [
                models.Relation.from_message(relation)
                for relation in orm_resource.relations.relations
            ]

    if ResourceProperties.ORIGIN in show:
        await orm_resource.get_origin()
        if orm_resource.origin is not None:
            resource.origin = models.Origin.from_message(orm_resource.origin)

    if field_type_filter and (include_values or include_extracted_data):
        await orm_resource.get_fields()
        resource.data = ResourceData()
        for (field_type, field_id), field in orm_resource.fields.items():
            field_type_name = FIELD_TYPES_MAP[field_type]
            if field_type_name not in field_type_filter:
                continue

            include_value = ResourceProperties.VALUES in show
            if include_value:
                value = await field.get_value()

            if field_type_name is FieldTypeName.TEXT:
                if resource.data.texts is None:
                    resource.data.texts = {}
                if field.id not in resource.data.texts:
                    resource.data.texts[field.id] = TextFieldData()
                if include_value:
                    resource.data.texts[field.id].value = models.FieldText.from_message(
                        value
                    )
                if include_extracted_data:
                    resource.data.texts[field.id].extracted = TextFieldExtractedData()
                    await set_resource_field_extracted_data(
                        field,
                        resource.data.texts[field.id].extracted,
                        field_type_name,
                        extracted,
                    )

            if field_type_name is FieldTypeName.FILE:
                if resource.data.files is None:
                    resource.data.files = {}
                if field.id not in resource.data.files:
                    resource.data.files[field.id] = FileFieldData()
                if include_value:
                    resource.data.files[field.id].value = models.FieldFile.from_message(
                        value
                    )
                if include_extracted_data:
                    resource.data.files[field.id].extracted = FileFieldExtractedData()
                    await set_resource_field_extracted_data(
                        field,
                        resource.data.files[field.id].extracted,
                        field_type_name,
                        extracted,
                    )

            if field_type_name is FieldTypeName.LINK:
                if resource.data.links is None:
                    resource.data.links = {}
                if field.id not in resource.data.links:
                    resource.data.links[field.id] = LinkFieldData()
                if include_value and value is not None:
                    resource.data.links[field.id].value = models.FieldLink.from_message(
                        value
                    )

                include_errors = ResourceProperties.ERRORS in show
                if include_errors:
                    error = await field.get_error()
                    if error is not None:
                        resource.data.links[field.id].error = Error(
                            body=error.error, code=error.code
                        )

                if include_extracted_data:
                    resource.data.links[field.id].extracted = LinkFieldExtractedData()
                    await set_resource_field_extracted_data(
                        field,
                        resource.data.links[field.id].extracted,
                        field_type_name,
                        extracted,
                    )

            if field_type_name is FieldTypeName.LAYOUT:
                if resource.data.layouts is None:
                    resource.data.layouts = {}
                if field.id not in resource.data.layouts:
                    resource.data.layouts[field.id] = LayoutFieldData()
                if include_value:
                    resource.data.layouts[
                        field.id
                    ].value = models.FieldLayout.from_message(value)
                if include_extracted_data:
                    resource.data.layouts[
                        field.id
                    ].extracted = LayoutFieldExtractedData()
                    await set_resource_field_extracted_data(
                        field,
                        resource.data.layouts[field.id].extracted,
                        field_type_name,
                        extracted,
                    )

            if field_type_name is FieldTypeName.CONVERSATION:
                if resource.data.conversations is None:
                    resource.data.conversations = {}
                if field.id not in resource.data.conversations:
                    resource.data.conversations[field.id] = ConversationFieldData()

                if include_value and isinstance(field, Conversation):
                    value = await field.get_metadata()
                    resource.data.conversations[
                        field.id
                    ].value = models.FieldConversation.from_message(value)
                if include_extracted_data:
                    resource.data.conversations[
                        field.id
                    ].extracted = ConversationFieldExtractedData()
                    await set_resource_field_extracted_data(
                        field,
                        resource.data.conversations[field.id].extracted,
                        field_type_name,
                        extracted,
                    )

            if field_type_name is FieldTypeName.DATETIME:
                if resource.data.datetimes is None:
                    resource.data.datetimes = {}
                if field.id not in resource.data.datetimes:
                    resource.data.datetimes[field.id] = DatetimeFieldData()

                if include_value:
                    resource.data.datetimes[
                        field.id
                    ].value = models.FieldDatetime.from_message(value)
                if include_extracted_data:
                    resource.data.datetimes[
                        field.id
                    ].extracted = DatetimeFieldExtractedData()
                    await set_resource_field_extracted_data(
                        field,
                        resource.data.datetimes[field.id].extracted,
                        field_type_name,
                        extracted,
                    )

            if field_type_name is FieldTypeName.KEYWORDSET:
                if resource.data.keywordsets is None:
                    resource.data.keywordsets = {field.id: KeywordsetFieldData()}
                if field.id not in resource.data.keywordsets:
                    resource.data.keywordsets[field.id] = KeywordsetFieldData()

                if include_value:
                    resource.data.keywordsets[
                        field.id
                    ].value = models.FieldKeywordset.from_message(value)
                if include_extracted_data:
                    resource.data.keywordsets[
                        field.id
                    ].extracted = KeywordsetFieldExtractedData()
                    await set_resource_field_extracted_data(
                        field,
                        resource.data.keywordsets[field.id].extracted,
                        field_type_name,
                        extracted,
                    )
    await txn.abort()
    return resource


async def get_orm_resource(
    txn: Transaction,
    kbid: str,
    rid: Optional[str],
    slug: Optional[str] = None,
    service_name: Optional[str] = None,
) -> Optional[ORMResource]:

    storage = await get_storage(service_name=service_name)
    cache = await get_cache()

    kb = KnowledgeBox(txn, storage, cache, kbid)

    if rid is None:
        if slug is None:
            raise ValueError("Either rid or slug parameters should be used")

        rid = await kb.get_resource_uuid_by_slug(slug)
        if rid is None:
            # Could not find resource uuid from slug
            return None

    orm_resource = await kb.get(rid)
    if orm_resource is None:
        return None

    return orm_resource


async def get_resource_uuid_by_slug(
    kbid: str, slug: str, service_name: Optional[str] = None
) -> Optional[str]:
    storage = await get_storage(service_name=service_name)
    cache = await get_cache()
    driver = await get_driver()
    txn = await driver.begin()
    kb = KnowledgeBox(txn, storage, cache, kbid)
    return await kb.get_resource_uuid_by_slug(slug)
