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
import dataclasses
from datetime import datetime
from typing import Optional, Union

from google.protobuf.json_format import MessageToDict

import nucliadb_models as models
from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.models_utils import from_proto, to_proto
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.orm.resource import Resource as ORMResource
from nucliadb.models.internal import processing as processing_models
from nucliadb.models.internal.processing import ClassificationLabel, PushConversation, PushPayload
from nucliadb.writer import SERVICE_NAME
from nucliadb.writer.utilities import get_processing
from nucliadb_models.common import FieldTypeName
from nucliadb_models.content_types import GENERIC_MIME_TYPE
from nucliadb_models.writer import (
    CreateResourcePayload,
    UpdateResourcePayload,
)
from nucliadb_protos import resources_pb2
from nucliadb_protos.writer_pb2 import BrokerMessage, FieldIDStatus, FieldStatus
from nucliadb_utils.storages.storage import StorageField
from nucliadb_utils.utilities import get_storage


@dataclasses.dataclass
class ResourceClassifications:
    resource_level: set[ClassificationLabel] = dataclasses.field(default_factory=set)
    field_level: dict[tuple[resources_pb2.FieldType.ValueType, str], set[ClassificationLabel]] = (
        dataclasses.field(default_factory=dict)
    )

    def for_field(
        self, field_key: str, field_type: resources_pb2.FieldType.ValueType
    ) -> list[ClassificationLabel]:
        """
        Returns a list of unique classification labels for a given field, including those inherited from the resource.
        """
        field_id = (field_type, field_key)
        resource_level = self.resource_level
        field_level = self.field_level.get(field_id, set())
        return list(resource_level.union(field_level))


async def extract_file_field_from_pb(
    field_pb: resources_pb2.FieldFile, classif_labels: list[ClassificationLabel]
) -> str:
    processing = get_processing()
    if field_pb.file.source == resources_pb2.CloudFile.Source.EXTERNAL:
        file_field = models.FileField(
            language=field_pb.language,
            password=field_pb.password,
            file=models.File(payload=None, uri=field_pb.file.uri),
            extract_strategy=field_pb.extract_strategy,
            split_strategy=field_pb.split_strategy,
        )
        return processing.convert_external_filefield_to_str(file_field, classif_labels)
    else:
        storage = await get_storage(service_name=SERVICE_NAME)
        return await processing.convert_internal_filefield_to_str(field_pb, storage, classif_labels)


async def extract_file_field(
    field_id: str,
    resource: ORMResource,
    toprocess: PushPayload,
    resource_classifications: ResourceClassifications,
    password: Optional[str] = None,
):
    field_type = resources_pb2.FieldType.FILE
    field = await resource.get_field(field_id, field_type)
    field_pb = await field.get_value()
    if field_pb is None:
        raise KeyError(f"Field {field_id} does not exists")

    if password is not None:
        field_pb.password = password

    classif_labels = resource_classifications.for_field(field_id, resources_pb2.FieldType.FILE)
    toprocess.filefield[field_id] = await extract_file_field_from_pb(field_pb, classif_labels)


async def extract_fields(resource: ORMResource, toprocess: PushPayload):
    processing = get_processing()
    storage = await get_storage(service_name=SERVICE_NAME)
    await resource.get_fields()

    resource_classifications = await atomic_get_stored_resource_classifications(
        kbid=toprocess.kbid,
        rid=toprocess.uuid,
    )
    for (field_type, field_id), field in resource.fields.items():
        field_type_name = from_proto.field_type_name(field_type)

        if field_type_name not in {
            FieldTypeName.TEXT,
            FieldTypeName.FILE,
            FieldTypeName.CONVERSATION,
            FieldTypeName.LINK,
        }:
            continue

        field_pb = await field.get_value()
        classif_labels = resource_classifications.for_field(field_id, field_type)
        if field_type_name is FieldTypeName.FILE:
            toprocess.filefield[field_id] = await extract_file_field_from_pb(field_pb, classif_labels)

        if field_type_name is FieldTypeName.LINK:
            parsed_link = MessageToDict(
                field_pb,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=True,
            )
            parsed_link["link"] = parsed_link.pop("uri", None)
            toprocess.linkfield[field_id] = processing_models.LinkUpload(**parsed_link)
            toprocess.linkfield[field_id].classification_labels = classif_labels

        if field_type_name is FieldTypeName.TEXT:
            parsed_text = MessageToDict(
                field_pb,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=True,
            )
            parsed_text["format"] = processing_models.PushTextFormat[parsed_text["format"]]
            toprocess.textfield[field_id] = processing_models.Text(**parsed_text)
            toprocess.textfield[field_id].classification_labels = classif_labels

        if field_type_name is FieldTypeName.CONVERSATION and isinstance(field, Conversation):
            metadata = await field.get_metadata()
            if metadata.pages == 0:
                continue

            full_conversation = PushConversation(messages=[])
            for page in range(0, metadata.pages):
                conversation_pb = await field.get_value(page + 1)
                if conversation_pb is None:
                    continue

                for message in conversation_pb.messages:
                    parsed_message = MessageToDict(
                        message,
                        preserving_proto_field_name=True,
                        always_print_fields_with_no_presence=True,
                    )
                    parsed_message["content"]["attachments"] = [
                        await processing.convert_internal_cf_to_str(cf, storage)
                        for cf in message.content.attachments
                    ]
                    if "attachments_fields" in parsed_message["content"]:
                        # Not defined on the push payload
                        del parsed_message["content"]["attachments_fields"]
                    parsed_message["content"]["format"] = resources_pb2.MessageContent.Format.Value(
                        parsed_message["content"]["format"]
                    )
                    full_conversation.messages.append(processing_models.PushMessage(**parsed_message))
            toprocess.conversationfield[field_id] = full_conversation
            toprocess.conversationfield[field_id].classification_labels = classif_labels


async def parse_fields(
    writer: BrokerMessage,
    toprocess: PushPayload,
    item: Union[CreateResourcePayload, UpdateResourcePayload],
    kbid: str,
    uuid: str,
    x_skip_store: bool,
    resource_classifications: ResourceClassifications,
):
    for key, file_field in item.files.items():
        await parse_file_field(
            key,
            file_field,
            writer,
            toprocess,
            kbid,
            uuid,
            resource_classifications,
            skip_store=x_skip_store,
        )

    for key, link_field in item.links.items():
        parse_link_field(
            key,
            link_field,
            writer,
            toprocess,
            resource_classifications,
        )

    for key, text_field in item.texts.items():
        parse_text_field(
            key,
            text_field,
            writer,
            toprocess,
            resource_classifications,
        )

    for key, conversation_field in item.conversations.items():
        await parse_conversation_field(
            key,
            conversation_field,
            writer,
            toprocess,
            kbid,
            uuid,
            resource_classifications,
        )


def parse_text_field(
    key: str,
    text_field: models.TextField,
    writer: BrokerMessage,
    toprocess: PushPayload,
    resource_classifications: ResourceClassifications,
) -> None:
    classif_labels = resource_classifications.for_field(key, resources_pb2.FieldType.TEXT)
    if text_field.extract_strategy is not None:
        writer.texts[key].extract_strategy = text_field.extract_strategy
    if text_field.split_strategy is not None:
        writer.texts[key].split_strategy = text_field.split_strategy
    writer.texts[key].body = text_field.body
    writer.texts[key].format = resources_pb2.FieldText.Format.Value(text_field.format.value)
    etw = resources_pb2.ExtractedTextWrapper()
    etw.field.field = key
    etw.field.field_type = resources_pb2.FieldType.TEXT
    etw.body.text = text_field.body
    writer.extracted_text.append(etw)
    toprocess.textfield[key] = processing_models.Text(
        body=text_field.body,
        format=getattr(processing_models.PushTextFormat, text_field.format.value),
        extract_strategy=text_field.extract_strategy,
        split_strategy=text_field.split_strategy,
        classification_labels=classif_labels,
    )
    writer.field_statuses.append(
        FieldIDStatus(
            id=resources_pb2.FieldID(field_type=resources_pb2.FieldType.TEXT, field=key),
            status=FieldStatus.Status.PENDING,
        )
    )


async def parse_file_field(
    key: str,
    file_field: models.FileField,
    writer: BrokerMessage,
    toprocess: PushPayload,
    kbid: str,
    uuid: str,
    resource_classifications: ResourceClassifications,
    skip_store: bool = False,
):
    if file_field.file.is_external:
        parse_external_file_field(key, file_field, writer, toprocess, resource_classifications)
    else:
        await parse_internal_file_field(
            key,
            file_field,
            writer,
            toprocess,
            kbid,
            uuid,
            resource_classifications,
            skip_store=skip_store,
        )

    writer.field_statuses.append(
        FieldIDStatus(
            id=resources_pb2.FieldID(field_type=resources_pb2.FieldType.FILE, field=key),
            status=FieldStatus.Status.PENDING,
        )
    )


async def parse_internal_file_field(
    key: str,
    file_field: models.FileField,
    writer: BrokerMessage,
    toprocess: PushPayload,
    kbid: str,
    uuid: str,
    resource_classifications: ResourceClassifications,
    skip_store: bool = False,
) -> None:
    classif_labels = resource_classifications.for_field(key, resources_pb2.FieldType.FILE)
    writer.files[key].added.FromDatetime(datetime.now())
    if file_field.language:
        writer.files[key].language = file_field.language
    if file_field.extract_strategy is not None:
        writer.files[key].extract_strategy = file_field.extract_strategy
    if file_field.split_strategy is not None:
        writer.files[key].split_strategy = file_field.split_strategy

    processing = get_processing()
    if skip_store:
        # Does not store file on nuclia's blob storage. Only sends it to process
        toprocess.filefield[key] = await processing.convert_filefield_to_str(file_field, classif_labels)

    else:
        # Store file on nuclia's blob storage
        storage = await get_storage(service_name=SERVICE_NAME)
        sf: StorageField = storage.file_field(kbid, uuid, field=key)
        writer.files[key].file.CopyFrom(
            await storage.upload_b64file_to_cloudfile(
                sf,
                file_field.file.payload.encode(),  # type: ignore
                file_field.file.filename,  # type: ignore
                file_field.file.content_type,
                file_field.file.md5,
            )
        )
        # Send the pointer of the new blob to processing
        toprocess.filefield[key] = await processing.convert_internal_filefield_to_str(
            writer.files[key], storage, classif_labels
        )


def parse_external_file_field(
    key: str,
    file_field: models.FileField,
    writer: BrokerMessage,
    toprocess: PushPayload,
    resource_classifications: ResourceClassifications,
) -> None:
    classif_labels = resource_classifications.for_field(key, resources_pb2.FieldType.FILE)
    writer.files[key].added.FromDatetime(datetime.now())
    if file_field.language:
        writer.files[key].language = file_field.language
    if file_field.extract_strategy is not None:
        writer.files[key].extract_strategy = file_field.extract_strategy
    if file_field.split_strategy is not None:
        writer.files[key].split_strategy = file_field.split_strategy
    uri = file_field.file.uri
    writer.files[key].url = uri  # type: ignore
    writer.files[key].file.uri = uri  # type: ignore
    writer.files[key].file.source = resources_pb2.CloudFile.Source.EXTERNAL
    writer.files[key].file.content_type = file_field.file.content_type
    if file_field.file.content_type and writer.basic.icon == GENERIC_MIME_TYPE:
        writer.basic.icon = file_field.file.content_type
    processing = get_processing()
    toprocess.filefield[key] = processing.convert_external_filefield_to_str(file_field, classif_labels)


def parse_link_field(
    key: str,
    link_field: models.LinkField,
    writer: BrokerMessage,
    toprocess: PushPayload,
    resource_classifications: ResourceClassifications,
) -> None:
    classif_labels = resource_classifications.for_field(key, resources_pb2.FieldType.LINK)
    writer.links[key].added.FromDatetime(datetime.now())

    writer.links[key].uri = link_field.uri
    if link_field.language:
        writer.links[key].language = link_field.language

    if link_field.headers is not None:
        for header, value in link_field.headers.items():
            writer.links[key].headers[header] = value

    if link_field.cookies is not None:
        for cookie, value in link_field.cookies.items():
            writer.links[key].headers[cookie] = value

    if link_field.localstorage is not None:
        for local, value in link_field.localstorage.items():
            writer.links[key].localstorage[local] = value

    if link_field.css_selector is not None:
        writer.links[key].css_selector = link_field.css_selector

    if link_field.xpath is not None:
        writer.links[key].xpath = link_field.xpath

    if link_field.extract_strategy is not None:
        writer.links[key].extract_strategy = link_field.extract_strategy

    if link_field.split_strategy is not None:
        writer.links[key].split_strategy = link_field.split_strategy

    toprocess.linkfield[key] = processing_models.LinkUpload(
        link=link_field.uri,
        headers=link_field.headers or {},
        cookies=link_field.cookies or {},
        localstorage=link_field.localstorage or {},
        css_selector=link_field.css_selector,
        xpath=link_field.xpath,
        extract_strategy=link_field.extract_strategy,
        split_strategy=link_field.split_strategy,
        classification_labels=classif_labels,
    )
    writer.field_statuses.append(
        FieldIDStatus(
            id=resources_pb2.FieldID(field_type=resources_pb2.FieldType.LINK, field=key),
            status=FieldStatus.Status.PENDING,
        )
    )


async def parse_conversation_field(
    key: str,
    conversation_field: models.InputConversationField,
    writer: BrokerMessage,
    toprocess: PushPayload,
    kbid: str,
    uuid: str,
    resource_classifications: ResourceClassifications,
) -> None:
    classif_labels = resource_classifications.for_field(key, resources_pb2.FieldType.CONVERSATION)
    storage = await get_storage(service_name=SERVICE_NAME)
    processing = get_processing()
    field_value = resources_pb2.Conversation()
    convs = processing_models.PushConversation()
    for message in conversation_field.messages:
        cm = resources_pb2.Message()
        if message.timestamp:
            cm.timestamp.FromDatetime(message.timestamp)
        if message.who:
            cm.who = message.who
        for to in message.to:
            cm.to.append(to)
        cm.ident = message.ident
        if message.type_ is not None:
            cm.type = resources_pb2.Message.MessageType.Value(message.type_.value)

        processing_message_content = processing_models.PushMessageContent(
            text=message.content.text,
            format=getattr(processing_models.PushMessageFormat, message.content.format.value),
        )

        cm.content.text = message.content.text
        cm.content.format = resources_pb2.MessageContent.Format.Value(message.content.format.value)
        cm.content.attachments_fields.extend(
            [
                resources_pb2.FieldRef(
                    field_type=to_proto.field_type_name(attachment.field_type),
                    field_id=attachment.field_id,
                    split=attachment.split if attachment.split is not None else "",
                )
                for attachment in message.content.attachments_fields
            ]
        )

        for idx, file in enumerate(message.content.attachments):
            sf_conv_field: StorageField = storage.conversation_field_attachment(
                kbid, uuid, field=key, ident=message.ident, attachment_index=idx
            )
            cf_conv_field = await storage.upload_b64file_to_cloudfile(
                sf_conv_field,
                file.payload.encode(),
                file.filename,
                file.content_type,
                file.md5,
            )
            cm.content.attachments.append(cf_conv_field)

            processing_message_content.attachments.append(
                await processing.convert_internal_cf_to_str(cf_conv_field, storage)
            )

        processing_message = processing_models.PushMessage(
            timestamp=message.timestamp,
            content=processing_message_content,
            ident=message.ident,
        )
        if message.who:
            processing_message.who = message.who
        for to in message.to:
            processing_message.to.append(to)
        convs.messages.append(processing_message)
        field_value.messages.append(cm)
    convs.classification_labels = classif_labels
    if conversation_field.extract_strategy:
        field_value.extract_strategy = conversation_field.extract_strategy
        convs.extract_strategy = conversation_field.extract_strategy
    if conversation_field.split_strategy:
        field_value.split_strategy = conversation_field.split_strategy
        convs.split_strategy = conversation_field.split_strategy
    toprocess.conversationfield[key] = convs
    writer.conversations[key].CopyFrom(field_value)
    writer.field_statuses.append(
        FieldIDStatus(
            id=resources_pb2.FieldID(field_type=resources_pb2.FieldType.CONVERSATION, field=key),
            status=FieldStatus.Status.PENDING,
        )
    )


async def atomic_get_stored_resource_classifications(
    kbid: str,
    rid: str,
) -> ResourceClassifications:
    async with datamanagers.with_ro_transaction() as txn:
        return await get_stored_resource_classifications(txn, kbid=kbid, rid=rid)


async def get_stored_resource_classifications(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
) -> ResourceClassifications:
    rc = ResourceClassifications()
    basic = await datamanagers.resources.get_basic(txn, kbid=kbid, rid=rid)
    if basic is None:
        # Resource not found
        return rc

    # User resource-level classifications
    for u_classif in basic.usermetadata.classifications:
        classif = ClassificationLabel(labelset=u_classif.labelset, label=u_classif.label)
        rc.resource_level.add(classif)

    # Processor-computed field-level classifications. These are not user-defined and are immutable.
    for field_classif in basic.computedmetadata.field_classifications:
        fid = (field_classif.field.field_type, field_classif.field.field)
        for f_classif in field_classif.classifications:
            classif = ClassificationLabel(labelset=f_classif.labelset, label=f_classif.label)
            rc.field_level.setdefault(fid, set()).add(classif)
    return rc
