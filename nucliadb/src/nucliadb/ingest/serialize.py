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

from typing import Optional

import nucliadb_models as models
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.fields.file import File
from nucliadb.ingest.fields.link import Link
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.resource import Resource as ORMResource
from nucliadb_models.common import FIELD_TYPES_MAP, FieldTypeName
from nucliadb_models.resource import (
    ConversationFieldData,
    ConversationFieldExtractedData,
    Error,
    ExtractedDataType,
    ExtractedDataTypeName,
    FileFieldData,
    FileFieldExtractedData,
    GenericFieldData,
    LinkFieldData,
    LinkFieldExtractedData,
    QueueType,
    Resource,
    ResourceData,
    TextFieldData,
    TextFieldExtractedData,
)
from nucliadb_models.search import ResourceProperties
from nucliadb_models.security import ResourceSecurity
from nucliadb_utils.utilities import get_storage


async def set_resource_field_extracted_data(
    field: Field,
    field_data: ExtractedDataType,
    field_type_name: FieldTypeName,
    wanted_extracted_data: list[ExtractedDataTypeName],
) -> None:
    if field_data is None:
        return

    if ExtractedDataTypeName.TEXT in wanted_extracted_data:
        data_et = await field.get_extracted_text()
        if data_et is not None:
            field_data.text = models.ExtractedText.from_message(data_et)

    metadata_wanted = ExtractedDataTypeName.METADATA in wanted_extracted_data
    shortened_metadata_wanted = ExtractedDataTypeName.SHORTENED_METADATA in wanted_extracted_data
    if metadata_wanted or shortened_metadata_wanted:
        data_fcm = await field.get_field_metadata()

        if data_fcm is not None:
            field_data.metadata = models.FieldComputedMetadata.from_message(
                data_fcm, shortened=shortened_metadata_wanted and not metadata_wanted
            )

    if ExtractedDataTypeName.LARGE_METADATA in wanted_extracted_data:
        data_lcm = await field.get_large_field_metadata()
        if data_lcm is not None:
            field_data.large_metadata = models.LargeComputedMetadata.from_message(data_lcm)

    if ExtractedDataTypeName.VECTOR in wanted_extracted_data:
        data_vec = await field.get_vectors()
        if data_vec is not None:
            field_data.vectors = models.VectorObject.from_message(data_vec)

    if ExtractedDataTypeName.QA in wanted_extracted_data:
        qa = await field.get_question_answers()
        if qa is not None:
            field_data.question_answers = models.QuestionAnswers.from_message(qa)

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
    show: list[ResourceProperties],
    field_type_filter: list[FieldTypeName],
    extracted: list[ExtractedDataTypeName],
    service_name: Optional[str] = None,
    slug: Optional[str] = None,
) -> Optional[Resource]:
    driver = get_driver()
    async with driver.transaction(read_only=True) as txn:
        return await managed_serialize(
            txn,
            kbid,
            rid,
            show,
            field_type_filter,
            extracted,
            service_name=service_name,
            slug=slug,
        )


async def managed_serialize(
    txn: Transaction,
    kbid: str,
    rid: Optional[str],
    show: list[ResourceProperties],
    field_type_filter: list[FieldTypeName],
    extracted: list[ExtractedDataTypeName],
    service_name: Optional[str] = None,
    slug: Optional[str] = None,
) -> Optional[Resource]:
    orm_resource = await get_orm_resource(txn, kbid, rid=rid, slug=slug, service_name=service_name)
    if orm_resource is None:
        return None

    resource = Resource(id=orm_resource.uuid)

    include_values = ResourceProperties.VALUES in show

    include_extracted_data = ResourceProperties.EXTRACTED in show and extracted is not []

    if ResourceProperties.BASIC in show:
        await orm_resource.get_basic()

        if orm_resource.basic is not None:
            resource.slug = orm_resource.basic.slug
            resource.title = orm_resource.basic.title
            resource.summary = orm_resource.basic.summary
            resource.icon = orm_resource.basic.icon
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

            resource.metadata = models.Metadata.from_message(orm_resource.basic.metadata)
            resource.usermetadata = models.UserMetadata.from_message(orm_resource.basic.usermetadata)
            resource.fieldmetadata = [
                models.UserFieldMetadata.from_message(fm) for fm in orm_resource.basic.fieldmetadata
            ]
            resource.computedmetadata = models.ComputedMetadata.from_message(
                orm_resource.basic.computedmetadata
            )

            resource.last_seqid = orm_resource.basic.last_seqid

            # 0 on the proto means it was not ever set, as first valid value for this field will allways be 1
            resource.last_account_seq = (
                orm_resource.basic.last_account_seq if orm_resource.basic.last_account_seq != 0 else None
            )
            resource.queue = QueueType[orm_resource.basic.QueueType.Name(orm_resource.basic.queue)]

    if ResourceProperties.RELATIONS in show:
        await orm_resource.get_relations()
        if orm_resource.relations is not None:
            resource.relations = [
                models.Relation.from_message(relation) for relation in orm_resource.relations.relations
            ]

    if ResourceProperties.ORIGIN in show:
        await orm_resource.get_origin()
        if orm_resource.origin is not None:
            resource.origin = models.Origin.from_message(orm_resource.origin)

    if ResourceProperties.EXTRA in show:
        await orm_resource.get_extra()
        if orm_resource.extra is not None:
            resource.extra = models.Extra.from_message(orm_resource.extra)

    include_errors = ResourceProperties.ERRORS in show

    if ResourceProperties.SECURITY in show:
        await orm_resource.get_security()
        resource.security = ResourceSecurity(access_groups=[])
        if orm_resource.security is not None:
            for gid in orm_resource.security.access_groups:
                resource.security.access_groups.append(gid)

    if field_type_filter and (include_values or include_extracted_data):
        await orm_resource.get_fields()
        resource.data = ResourceData()
        for (field_type, field_id), field in orm_resource.fields.items():
            field_type_name = FIELD_TYPES_MAP[field_type]
            if field_type_name not in field_type_filter:
                continue

            include_value = ResourceProperties.VALUES in show
            value = None
            if include_value:
                value = await field.get_value()

            if field_type_name is FieldTypeName.TEXT:
                if resource.data.texts is None:
                    resource.data.texts = {}
                if field.id not in resource.data.texts:
                    resource.data.texts[field.id] = TextFieldData()
                if include_value:
                    serialized_value = (
                        models.FieldText.from_message(value) if value is not None else None
                    )
                    resource.data.texts[field.id].value = serialized_value
                if include_errors:
                    error = await field.get_error()
                    if error is not None:
                        resource.data.texts[field.id].error = Error(body=error.error, code=error.code)
                if include_extracted_data:
                    resource.data.texts[field.id].extracted = TextFieldExtractedData()
                    await set_resource_field_extracted_data(
                        field,
                        resource.data.texts[field.id].extracted,
                        field_type_name,
                        extracted,
                    )
            elif field_type_name is FieldTypeName.FILE:
                if resource.data.files is None:
                    resource.data.files = {}
                if field.id not in resource.data.files:
                    resource.data.files[field.id] = FileFieldData()
                if include_value:
                    if value is not None:
                        resource.data.files[field.id].value = models.FieldFile.from_message(value)
                    else:
                        resource.data.files[field.id].value = None

                if include_errors:
                    error = await field.get_error()
                    if error is not None:
                        resource.data.files[field.id].error = Error(body=error.error, code=error.code)

                if include_extracted_data:
                    resource.data.files[field.id].extracted = FileFieldExtractedData()
                    await set_resource_field_extracted_data(
                        field,
                        resource.data.files[field.id].extracted,
                        field_type_name,
                        extracted,
                    )
            elif field_type_name is FieldTypeName.LINK:
                if resource.data.links is None:
                    resource.data.links = {}
                if field.id not in resource.data.links:
                    resource.data.links[field.id] = LinkFieldData()
                if include_value and value is not None:
                    resource.data.links[field.id].value = models.FieldLink.from_message(value)

                if include_errors:
                    error = await field.get_error()
                    if error is not None:
                        resource.data.links[field.id].error = Error(body=error.error, code=error.code)

                if include_extracted_data:
                    resource.data.links[field.id].extracted = LinkFieldExtractedData()
                    await set_resource_field_extracted_data(
                        field,
                        resource.data.links[field.id].extracted,
                        field_type_name,
                        extracted,
                    )
            elif field_type_name is FieldTypeName.CONVERSATION:
                if resource.data.conversations is None:
                    resource.data.conversations = {}
                if field.id not in resource.data.conversations:
                    resource.data.conversations[field.id] = ConversationFieldData()
                if include_errors:
                    error = await field.get_error()
                    if error is not None:
                        resource.data.conversations[field.id].error = Error(
                            body=error.error, code=error.code
                        )
                if include_value and isinstance(field, Conversation):
                    value = await field.get_metadata()
                    resource.data.conversations[field.id].value = models.FieldConversation.from_message(
                        value
                    )
                if include_extracted_data:
                    resource.data.conversations[field.id].extracted = ConversationFieldExtractedData()
                    await set_resource_field_extracted_data(
                        field,
                        resource.data.conversations[field.id].extracted,
                        field_type_name,
                        extracted,
                    )
            elif field_type_name is FieldTypeName.GENERIC:
                if resource.data.generics is None:
                    resource.data.generics = {}
                if field.id not in resource.data.generics:
                    resource.data.generics[field.id] = GenericFieldData()
                if include_value:
                    resource.data.generics[field.id].value = value
                if include_errors:
                    error = await field.get_error()
                    if error is not None:
                        resource.data.generics[field.id].error = Error(body=error.error, code=error.code)
                if include_extracted_data:
                    resource.data.generics[field.id].extracted = TextFieldExtractedData(
                        text=models.ExtractedText(text=resource.data.generics[field.id].value)
                    )
    return resource


async def get_orm_resource(
    txn: Transaction,
    kbid: str,
    rid: Optional[str],
    slug: Optional[str] = None,
    service_name: Optional[str] = None,
) -> Optional[ORMResource]:
    storage = await get_storage(service_name=service_name)

    kb = KnowledgeBox(txn, storage, kbid)

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
    driver = get_driver()
    async with driver.transaction(read_only=True) as txn:
        kb = KnowledgeBox(txn, storage, kbid)
        return await kb.get_resource_uuid_by_slug(slug)
