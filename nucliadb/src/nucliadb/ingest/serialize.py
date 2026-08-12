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

import nucliadb_models as models
from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.models_utils import from_proto
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.fields.file import File
from nucliadb.ingest.fields.link import Link
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.resource import Resource as ORMResource
from nucliadb_models.common import FieldTypeName
from nucliadb_models.extracted import (
    ExtractedText,
    FieldComputedMetadata,
    FieldQuestionAnswers,
    FileExtractedData,
    LargeComputedMetadata,
    LinkExtractedData,
    RelationEdgeVector,
    RelationNodeVector,
    VectorObject,
)
from nucliadb_models.metadata import Extra, Origin, Relation
from nucliadb_models.resource import (
    ConversationFieldData,
    ConversationFieldExtractedData,
    Error,
    ExtractedDataType,
    ExtractedDataTypeName,
    FileFieldData,
    FileFieldExtractedData,
    GenericFieldData,
    KeyValueFieldData,
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
from nucliadb_protos import writer_pb2
from nucliadb_protos.writer_pb2 import FieldStatus
from nucliadb_utils.utilities import get_storage


async def serialize(
    kbid: str,
    rid: str | None,
    show: list[ResourceProperties],
    field_type_filter: list[FieldTypeName],
    extracted: list[ExtractedDataTypeName],
    vectorset: str | None = None,
    service_name: str | None = None,
    slug: str | None = None,
    max_parallel_field_serializations: int = 16,
) -> Resource | None:
    driver = get_driver()
    async with driver.ro_transaction() as txn:
        return await managed_serialize(
            txn,
            kbid,
            rid,
            show,
            field_type_filter,
            extracted,
            vectorset=vectorset,
            service_name=service_name,
            slug=slug,
            max_parallel_field_serializations=max_parallel_field_serializations,
        )


async def managed_serialize(
    txn: Transaction,
    kbid: str,
    rid: str | None,
    show: list[ResourceProperties],
    field_type_filter: list[FieldTypeName],
    extracted: list[ExtractedDataTypeName],
    vectorset: str | None = None,
    service_name: str | None = None,
    slug: str | None = None,
    max_parallel_field_serializations: int = 16,
) -> Resource | None:
    orm_resource = await get_orm_resource(txn, kbid, rid=rid, slug=slug, service_name=service_name)
    if orm_resource is None:
        return None

    return await serialize_resource(
        orm_resource,
        show,
        field_type_filter,
        extracted,
        vectorset=vectorset,
        max_parallel_field_serializations=max_parallel_field_serializations,
    )


async def get_orm_resource(
    txn: Transaction,
    kbid: str,
    rid: str | None,
    slug: str | None = None,
    service_name: str | None = None,
) -> ORMResource | None:
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


async def serialize_resource(
    orm_resource: ORMResource,
    show: list[ResourceProperties],
    field_type_filter: list[FieldTypeName],
    extracted: list[ExtractedDataTypeName],
    vectorset: str | None = None,
    max_parallel_field_serializations: int = 16,
) -> Resource:
    resource = Resource(id=orm_resource.uuid)

    include_values = ResourceProperties.VALUES in show
    include_extracted_data = ResourceProperties.EXTRACTED in show and extracted != []
    include_errors = ResourceProperties.ERRORS in show

    should_serialize_fields = (
        field_type_filter and (include_values or include_extracted_data)
    ) or include_errors

    fields_task = None
    if should_serialize_fields:
        fields_task = asyncio.create_task(orm_resource.get_fields())

    await serialize_resource_metadata(orm_resource, resource, show)

    if fields_task is not None:
        await fields_task

    if should_serialize_fields:
        resource.data = ResourceData()
        concurrency_control = asyncio.Semaphore(max(max_parallel_field_serializations, 1))

        selected_fields: list[
            tuple[
                Field,
                FieldTypeName,
                TextFieldData
                | FileFieldData
                | LinkFieldData
                | ConversationFieldData
                | GenericFieldData
                | KeyValueFieldData,
            ]
        ] = []

        for (field_type, _), field in orm_resource.fields.items():
            field_type_name = from_proto.field_type_name(field_type)
            if field_type_name not in field_type_filter:
                continue

            field_data = ensure_serialized_field_data(resource.data, field_type_name, field.id)
            selected_fields.append((field, field_type_name, field_data))

        await serialize_fields_data(
            selected_fields,
            include_values=include_values,
            include_errors=include_errors,
            concurrency_control=concurrency_control,
        )

        if include_extracted_data:
            await serialize_fields_extracted_data(
                selected_fields,
                extracted,
                vectorset=vectorset,
                concurrency_control=concurrency_control,
            )
    return resource


async def serialize_resource_metadata(
    orm_resource: ORMResource,
    resource: Resource,
    show: list[ResourceProperties],
) -> None:
    relations_task = None
    requested_resource_columns: list[datamanagers.resources.ResourceColumn] = []
    if ResourceProperties.BASIC in show:
        requested_resource_columns.append("basic")
        if ResourceProperties.RELATIONS in show:
            relations_task = asyncio.create_task(orm_resource.get_user_relations())
    if ResourceProperties.ORIGIN in show:
        requested_resource_columns.append("origin")
    if ResourceProperties.EXTRA in show:
        requested_resource_columns.append("extra")
    if ResourceProperties.SECURITY in show:
        requested_resource_columns.append("security")

    resource_data = None
    if requested_resource_columns:
        resource_data = await orm_resource.get_data(columns=tuple(requested_resource_columns))

    basic = resource_data.basic if resource_data is not None else None
    origin = resource_data.origin if resource_data is not None else None
    extra = resource_data.extra if resource_data is not None else None
    security = resource_data.security if resource_data is not None else None

    if ResourceProperties.BASIC in show and basic is not None:
        resource.slug = basic.slug
        resource.title = basic.title
        resource.summary = basic.summary
        resource.icon = basic.icon
        resource.thumbnail = basic.thumbnail
        resource.hidden = basic.hidden
        resource.created = basic.created.ToDatetime() if basic.HasField("created") else None
        resource.modified = basic.modified.ToDatetime() if basic.HasField("modified") else None

        resource.metadata = from_proto.metadata(basic.metadata)
        resource.usermetadata = from_proto.user_metadata(basic.usermetadata)
        resource.fieldmetadata = [from_proto.user_field_metadata(fm) for fm in basic.fieldmetadata]
        resource.computedmetadata = from_proto.computed_metadata(basic.computedmetadata)

        resource.last_seqid = basic.last_seqid

        # 0 on the proto means it was not ever set, as first valid value for this field will allways be 1
        resource.last_account_seq = basic.last_account_seq if basic.last_account_seq != 0 else None
        resource.queue = QueueType[basic.QueueType.Name(basic.queue)]

        if ResourceProperties.RELATIONS in show and relations_task is not None:
            relations = relations_task.result()
            resource.usermetadata.relations = [from_proto.relation(rel) for rel in relations.relations]

    if ResourceProperties.ORIGIN in show and origin is not None:
        resource.origin = from_proto.origin(origin)
    if ResourceProperties.EXTRA in show and extra is not None:
        resource.extra = from_proto.extra(extra)
    if ResourceProperties.SECURITY in show and security is not None:
        resource.security = from_proto.security(security)


async def serialize_origin(resource: ORMResource) -> Origin | None:
    origin = await resource.get_origin()
    if origin is None:
        return None

    return from_proto.origin(origin)


async def serialize_extra(resource: ORMResource) -> Extra | None:
    extra = await resource.get_extra()
    if extra is None:
        return None
    return from_proto.extra(extra)


async def serialize_user_relations(resource: ORMResource) -> list[Relation]:
    relations = await resource.get_user_relations()
    return [from_proto.relation(rel) for rel in relations.relations]


async def serialize_security(resource: ORMResource) -> ResourceSecurity:
    security = ResourceSecurity(access_groups=[])

    security_pb = await resource.get_security()
    if security_pb is not None:
        for gid in security_pb.access_groups:
            security.access_groups.append(gid)

    return security


def ensure_serialized_field_data(
    resource_data: ResourceData,
    field_type_name: FieldTypeName,
    field_id: str,
) -> (
    TextFieldData
    | FileFieldData
    | LinkFieldData
    | ConversationFieldData
    | GenericFieldData
    | KeyValueFieldData
):
    if field_type_name is FieldTypeName.TEXT:
        if resource_data.texts is None:
            resource_data.texts = {}
        if field_id not in resource_data.texts:
            resource_data.texts[field_id] = TextFieldData()
        return resource_data.texts[field_id]

    if field_type_name is FieldTypeName.FILE:
        if resource_data.files is None:
            resource_data.files = {}
        if field_id not in resource_data.files:
            resource_data.files[field_id] = FileFieldData()
        return resource_data.files[field_id]

    if field_type_name is FieldTypeName.LINK:
        if resource_data.links is None:
            resource_data.links = {}
        if field_id not in resource_data.links:
            resource_data.links[field_id] = LinkFieldData()
        return resource_data.links[field_id]

    if field_type_name is FieldTypeName.CONVERSATION:
        if resource_data.conversations is None:
            resource_data.conversations = {}
        if field_id not in resource_data.conversations:
            resource_data.conversations[field_id] = ConversationFieldData()
        return resource_data.conversations[field_id]

    if field_type_name is FieldTypeName.GENERIC:
        if resource_data.generics is None:
            resource_data.generics = {}
        if field_id not in resource_data.generics:
            resource_data.generics[field_id] = GenericFieldData()
        return resource_data.generics[field_id]

    if field_type_name is FieldTypeName.KEY_VALUE:
        if resource_data.key_values is None:
            resource_data.key_values = {}
        if field_id not in resource_data.key_values:
            resource_data.key_values[field_id] = KeyValueFieldData()
        return resource_data.key_values[field_id]
    else:
        assert_never(field_type_name)


async def serialize_field_data(
    field: Field,
    field_type_name: FieldTypeName,
    serialized: (
        TextFieldData
        | FileFieldData
        | LinkFieldData
        | ConversationFieldData
        | GenericFieldData
        | KeyValueFieldData
    ),
    *,
    include_value: bool,
    include_errors: bool,
) -> None:
    value, status = await fetch_field_values(
        field,
        field_type_name,
        include_value=include_value,
        include_errors=include_errors,
    )

    if field_type_name is FieldTypeName.TEXT and isinstance(serialized, TextFieldData):
        if include_value:
            serialized.value = from_proto.field_text(value) if value is not None else None
    elif field_type_name is FieldTypeName.FILE and isinstance(serialized, FileFieldData):
        if include_value:
            serialized.value = from_proto.field_file(value) if value is not None else None
    elif field_type_name is FieldTypeName.LINK and isinstance(serialized, LinkFieldData):
        if include_value and value is not None:
            serialized.value = from_proto.field_link(value)
    elif field_type_name is FieldTypeName.CONVERSATION and isinstance(serialized, ConversationFieldData):
        if include_value and value is not None:
            serialized.value = from_proto.field_conversation(value)
    elif field_type_name is FieldTypeName.GENERIC and isinstance(serialized, GenericFieldData):
        if include_value:
            serialized.value = value
    elif field_type_name is FieldTypeName.KEY_VALUE and isinstance(serialized, KeyValueFieldData):
        if include_value and value is not None:
            serialized.value = from_proto.field_key_value(value)

    if include_errors:
        set_serialized_field_errors_from_status(serialized, status)


async def fetch_field_values(
    field: Field,
    field_type_name: FieldTypeName,
    *,
    include_value: bool,
    include_errors: bool,
) -> tuple[Any | None, FieldStatus | None]:

    value_task = None
    status_task = None

    if include_value:
        if field_type_name is FieldTypeName.CONVERSATION and isinstance(field, Conversation):
            value_task = asyncio.create_task(field.get_metadata())
        else:
            value_task = asyncio.create_task(field.get_value())

    if include_errors:
        status_task = asyncio.create_task(field.get_status())

    await asyncio.gather(*[task for task in [value_task, status_task] if task is not None])
    value = value_task.result() if value_task is not None else None
    status = status_task.result() if status_task is not None else None
    return value, status


async def serialize_fields_data(
    selected_fields: list[
        tuple[
            Field,
            FieldTypeName,
            TextFieldData
            | FileFieldData
            | LinkFieldData
            | ConversationFieldData
            | GenericFieldData
            | KeyValueFieldData,
        ]
    ],
    *,
    include_values: bool,
    include_errors: bool,
    concurrency_control: asyncio.Semaphore,
) -> None:
    from nucliadb.search.augmentor.utils import limited_concurrency

    await asyncio.gather(
        *[
            limited_concurrency(
                serialize_field_data(
                    field,
                    field_type_name,
                    field_data,
                    include_value=include_values,
                    include_errors=include_errors,
                ),
                max_ops=concurrency_control,
            )
            for field, field_type_name, field_data in selected_fields
        ]
    )


async def serialize_fields_extracted_data(
    selected_fields: list[
        tuple[
            Field,
            FieldTypeName,
            TextFieldData
            | FileFieldData
            | LinkFieldData
            | ConversationFieldData
            | GenericFieldData
            | KeyValueFieldData,
        ]
    ],
    extracted: list[ExtractedDataTypeName],
    *,
    vectorset: str | None,
    concurrency_control: asyncio.Semaphore,
) -> None:
    from nucliadb.search.augmentor.utils import limited_concurrency

    await asyncio.gather(
        *[
            limited_concurrency(
                serialize_field_extracted_data(
                    field,
                    field_type_name,
                    field_data,
                    extracted,
                    vectorset=vectorset,
                ),
                max_ops=concurrency_control,
            )
            for field, field_type_name, field_data in selected_fields
        ]
    )


async def serialize_field_extracted_data(
    field: Field,
    field_type_name: FieldTypeName,
    serialized: (
        TextFieldData
        | FileFieldData
        | LinkFieldData
        | ConversationFieldData
        | GenericFieldData
        | KeyValueFieldData
    ),
    extracted: list[ExtractedDataTypeName],
    *,
    vectorset: str | None = None,
) -> None:
    if field_type_name is FieldTypeName.TEXT and isinstance(serialized, TextFieldData):
        serialized.extracted = TextFieldExtractedData()
        await set_resource_field_extracted_data(
            field,
            serialized.extracted,
            field_type_name,
            extracted,
            vectorset=vectorset,
        )
    elif field_type_name is FieldTypeName.FILE and isinstance(serialized, FileFieldData):
        serialized.extracted = FileFieldExtractedData()
        await set_resource_field_extracted_data(
            field,
            serialized.extracted,
            field_type_name,
            extracted,
            vectorset=vectorset,
        )
    elif field_type_name is FieldTypeName.LINK and isinstance(serialized, LinkFieldData):
        serialized.extracted = LinkFieldExtractedData()
        await set_resource_field_extracted_data(
            field,
            serialized.extracted,
            field_type_name,
            extracted,
            vectorset=vectorset,
        )
    elif field_type_name is FieldTypeName.CONVERSATION and isinstance(serialized, ConversationFieldData):
        serialized.extracted = ConversationFieldExtractedData()
        await set_resource_field_extracted_data(
            field,
            serialized.extracted,
            field_type_name,
            extracted,
            vectorset=vectorset,
        )
    elif field_type_name is FieldTypeName.GENERIC and isinstance(serialized, GenericFieldData):
        serialized.extracted = TextFieldExtractedData(text=models.ExtractedText(text=serialized.value))


async def serialize_field_errors(
    field: Field,
    serialized: (
        TextFieldData
        | FileFieldData
        | LinkFieldData
        | ConversationFieldData
        | GenericFieldData
        | KeyValueFieldData
    ),
):
    status = await field.get_status()
    set_serialized_field_errors_from_status(serialized, status)


def set_serialized_field_errors_from_status(
    serialized: (
        TextFieldData
        | FileFieldData
        | LinkFieldData
        | ConversationFieldData
        | GenericFieldData
        | KeyValueFieldData
    ),
    status: FieldStatus | None,
) -> None:
    if status is None:
        status = FieldStatus()
    serialized.status = status.Status.Name(status.status)
    if status.errors:
        serialized.errors = []
        for error in status.errors:
            serialized.errors.append(
                Error(
                    body=error.source_error.error,
                    code=error.source_error.code,
                    code_str=writer_pb2.Error.ErrorCode.Name(error.source_error.code),
                    created=error.created.ToDatetime(),
                    severity=writer_pb2.Error.Severity.Name(error.source_error.severity),
                )
            )
        serialized.error = serialized.errors[-1]


async def set_resource_field_extracted_data(
    field: Field,
    field_data: ExtractedDataType,
    field_type_name: FieldTypeName,
    wanted_extracted_data: list[ExtractedDataTypeName],
    vectorset: str | None = None,
) -> None:
    if field_data is None:
        return

    text_task = None
    metadata_task = None
    large_metadata_task = None
    vector_task = None
    qa_task = None
    file_task = None
    link_task = None
    relation_node_vectors_task = None
    relation_edge_vectors_task = None

    if ExtractedDataTypeName.TEXT in wanted_extracted_data:
        text_task = asyncio.create_task(serialize_extracted_text(field))

    metadata_wanted = ExtractedDataTypeName.METADATA in wanted_extracted_data
    shortened_metadata_wanted = ExtractedDataTypeName.SHORTENED_METADATA in wanted_extracted_data
    if metadata_wanted or shortened_metadata_wanted:
        metadata_task = asyncio.create_task(
            serialize_extracted_metadata(
                field, shortened=shortened_metadata_wanted and not metadata_wanted
            )
        )

    if ExtractedDataTypeName.LARGE_METADATA in wanted_extracted_data:
        large_metadata_task = asyncio.create_task(serialize_extracted_large_metadata(field))

    if ExtractedDataTypeName.VECTOR in wanted_extracted_data:
        vector_task = asyncio.create_task(serialize_extracted_vectors(field, vectorset=vectorset))

    if ExtractedDataTypeName.QA in wanted_extracted_data:
        qa_task = asyncio.create_task(serialize_extracted_question_answers(field))

    if (
        isinstance(field, File)
        and isinstance(field_data, FileFieldExtractedData)
        and ExtractedDataTypeName.FILE in wanted_extracted_data
    ):
        file_task = asyncio.create_task(serialize_file_extracted_data(field))

    if (
        isinstance(field, Link)
        and isinstance(field_data, LinkFieldExtractedData)
        and ExtractedDataTypeName.LINK in wanted_extracted_data
    ):
        link_task = asyncio.create_task(serialize_link_extracted_data(field))

    if ExtractedDataTypeName.RELATION_VECTORS in wanted_extracted_data:
        relation_node_vectors_task = asyncio.create_task(serialize_relation_node_vectors(field))
        relation_edge_vectors_task = asyncio.create_task(serialize_relation_edge_vectors(field))

    await asyncio.gather(
        *[
            task
            for task in [
                text_task,
                metadata_task,
                large_metadata_task,
                vector_task,
                qa_task,
                file_task,
                link_task,
                relation_node_vectors_task,
                relation_edge_vectors_task,
            ]
            if task is not None
        ]
    )

    if text_task is not None:
        field_data.text = text_task.result()
    if metadata_task is not None:
        field_data.metadata = metadata_task.result()
    if large_metadata_task is not None:
        field_data.large_metadata = large_metadata_task.result()
    if vector_task is not None:
        field_data.vectors = vector_task.result()
    if qa_task is not None:
        field_data.question_answers = qa_task.result()
    if file_task is not None and isinstance(field_data, FileFieldExtractedData):
        field_data.file = file_task.result()
    if link_task is not None and isinstance(field_data, LinkFieldExtractedData):
        field_data.link = link_task.result()
    if relation_node_vectors_task is not None:
        field_data.relation_node_vectors = relation_node_vectors_task.result()
    if relation_edge_vectors_task is not None:
        field_data.relation_edge_vectors = relation_edge_vectors_task.result()


async def serialize_extracted_text(field: Field) -> ExtractedText | None:
    data_et = await field.get_extracted_text()
    if data_et is None:
        return None
    return from_proto.extracted_text(data_et)


async def serialize_extracted_metadata(field: Field, *, shortened: bool) -> FieldComputedMetadata | None:
    data_fcm = await field.get_field_metadata()
    if data_fcm is None:
        return None
    return from_proto.field_computed_metadata(data_fcm, shortened)


async def serialize_extracted_large_metadata(field: Field) -> LargeComputedMetadata | None:
    data_lcm = await field.get_large_field_metadata()
    if data_lcm is None:
        return None
    return from_proto.large_computed_metadata(data_lcm)


async def serialize_extracted_vectors(field: Field, vectorset: str | None = None) -> VectorObject | None:
    vectorset_id = None
    vs = None
    async with datamanagers.with_ro_transaction() as txn:
        if vectorset is None:
            # Get the first vectorset for this field's KB, if any
            async for vectorset_id, vs in datamanagers.vectorsets.iter(txn=txn, kbid=field.kbid):
                break
        else:
            vectorset_id = vectorset
            vs = await datamanagers.vectorsets.get(txn, kbid=field.kbid, vectorset_id=vectorset)
    if vs is None or vectorset_id is None:
        return None
    data_vec = await field.get_vectors(vectorset_id, vs.storage_key_kind)
    if data_vec is None:
        return None
    return from_proto.vector_object(data_vec)


async def serialize_relation_node_vectors(field: Field) -> dict[str, list[RelationNodeVector]]:
    vectors: dict[str, list[RelationNodeVector]] = {}
    async with datamanagers.with_ro_transaction() as txn:
        for vs in await datamanagers.graph_vectorsets.node.get_all(txn, kbid=field.kbid):
            data_vec = await field.get_relation_node_vectors(vs.vectorset_id)
            if data_vec is None:
                vectors[vs.vectorset_id] = []
            else:
                vectors[vs.vectorset_id] = [from_proto.relation_node_vector(v) for v in data_vec.vectors]
    return vectors


async def serialize_relation_edge_vectors(field: Field) -> dict[str, list[RelationEdgeVector]]:
    vectors: dict[str, list[RelationEdgeVector]] = {}
    async with datamanagers.with_ro_transaction() as txn:
        for vs in await datamanagers.graph_vectorsets.edge.get_all(txn, kbid=field.kbid):
            data_vec = await field.get_relation_edge_vectors(vs.vectorset_id)
            if data_vec is None:
                vectors[vs.vectorset_id] = []
            else:
                vectors[vs.vectorset_id] = [from_proto.relation_edge_vector(v) for v in data_vec.vectors]
    return vectors


async def serialize_extracted_question_answers(field: Field) -> FieldQuestionAnswers | None:
    qa = await field.get_question_answers()
    if qa is None:
        return None
    return from_proto.field_question_answers(qa)


async def serialize_file_extracted_data(field: File) -> FileExtractedData | None:
    data_fed = await field.get_file_extracted_data()
    if data_fed is None:
        return None
    return from_proto.file_extracted_data(data_fed)


async def serialize_link_extracted_data(field: Link) -> LinkExtractedData | None:
    data_led = await field.get_link_extracted_data()
    if data_led is None:
        return None
    return from_proto.link_extracted_data(data_led)
