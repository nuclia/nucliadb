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
from datetime import datetime
from typing import Optional, Union

from google.protobuf.json_format import MessageToDict

import nucliadb_models as models
from nucliadb.common.models_utils import from_proto, to_proto
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.orm.resource import Resource as ORMResource
from nucliadb.ingest.processing import PushPayload
from nucliadb.writer import SERVICE_NAME
from nucliadb.writer.utilities import get_processing
from nucliadb_models.common import FieldTypeName
from nucliadb_models.content_types import GENERIC_MIME_TYPE
from nucliadb_models.conversation import PushConversation
from nucliadb_models.writer import (
    CreateResourcePayload,
    UpdateResourcePayload,
)
from nucliadb_protos import resources_pb2
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_utils.storages.storage import StorageField
from nucliadb_utils.utilities import get_storage


async def extract_file_field_from_pb(field_pb: resources_pb2.FieldFile) -> str:
    processing = get_processing()

    if field_pb.file.source == resources_pb2.CloudFile.Source.EXTERNAL:
        file_field = models.FileField(
            language=field_pb.language,
            password=field_pb.password,
            file=models.File(payload=None, uri=field_pb.file.uri),
        )
        return processing.convert_external_filefield_to_str(file_field)
    else:
        storage = await get_storage(service_name=SERVICE_NAME)
        return await processing.convert_internal_filefield_to_str(field_pb, storage)


async def extract_file_field(
    field_id: str,
    resource: ORMResource,
    toprocess: PushPayload,
    password: Optional[str] = None,
):
    field_type = resources_pb2.FieldType.FILE
    field = await resource.get_field(field_id, field_type)
    field_pb = await field.get_value()
    if field_pb is None:
        raise KeyError(f"Field {field_id} does not exists")

    if password is not None:
        field_pb.password = password

    toprocess.filefield[field_id] = await extract_file_field_from_pb(field_pb)


async def extract_fields(resource: ORMResource, toprocess: PushPayload):
    processing = get_processing()
    storage = await get_storage(service_name=SERVICE_NAME)
    await resource.get_fields()
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

        if field_type_name is FieldTypeName.FILE:
            toprocess.filefield[field_id] = await extract_file_field_from_pb(field_pb)

        if field_type_name is FieldTypeName.LINK:
            parsed_link = MessageToDict(
                field_pb,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
            parsed_link["link"] = parsed_link.pop("uri", None)
            toprocess.linkfield[field_id] = models.LinkUpload(**parsed_link)

        if field_type_name is FieldTypeName.TEXT:
            parsed_text = MessageToDict(
                field_pb,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
            parsed_text["format"] = models.PushTextFormat[parsed_text["format"]]
            toprocess.textfield[field_id] = models.Text(**parsed_text)

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
                        including_default_value_fields=True,
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
                    full_conversation.messages.append(models.PushMessage(**parsed_message))
            toprocess.conversationfield[field_id] = full_conversation


async def parse_fields(
    writer: BrokerMessage,
    toprocess: PushPayload,
    item: Union[CreateResourcePayload, UpdateResourcePayload],
    kbid: str,
    uuid: str,
    x_skip_store: bool,
):
    for key, file_field in item.files.items():
        await parse_file_field(key, file_field, writer, toprocess, kbid, uuid, skip_store=x_skip_store)

    for key, link_field in item.links.items():
        parse_link_field(key, link_field, writer, toprocess)

    for key, text_field in item.texts.items():
        parse_text_field(key, text_field, writer, toprocess)

    for key, conversation_field in item.conversations.items():
        await parse_conversation_field(key, conversation_field, writer, toprocess, kbid, uuid)


def parse_text_field(
    key: str,
    text_field: models.TextField,
    writer: BrokerMessage,
    toprocess: PushPayload,
) -> None:
    writer.texts[key].body = text_field.body
    writer.texts[key].format = resources_pb2.FieldText.Format.Value(text_field.format.value)
    etw = resources_pb2.ExtractedTextWrapper()
    etw.field.field = key
    etw.field.field_type = resources_pb2.FieldType.TEXT
    etw.body.text = text_field.body
    writer.extracted_text.append(etw)
    toprocess.textfield[key] = models.Text(
        body=text_field.body,
        format=getattr(models.PushTextFormat, text_field.format.value),
    )


async def parse_file_field(
    key: str,
    file_field: models.FileField,
    writer: BrokerMessage,
    toprocess: PushPayload,
    kbid: str,
    uuid: str,
    skip_store: bool = False,
):
    if file_field.file.is_external:
        parse_external_file_field(key, file_field, writer, toprocess)
    else:
        await parse_internal_file_field(
            key, file_field, writer, toprocess, kbid, uuid, skip_store=skip_store
        )


async def parse_internal_file_field(
    key: str,
    file_field: models.FileField,
    writer: BrokerMessage,
    toprocess: PushPayload,
    kbid: str,
    uuid: str,
    skip_store: bool = False,
) -> None:
    writer.files[key].added.FromDatetime(datetime.now())
    if file_field.language:
        writer.files[key].language = file_field.language
    if file_field.extract_strategy is not None:
        writer.files[key].extract_strategy = file_field.extract_strategy

    processing = get_processing()

    if skip_store:
        # Does not store file on nuclia's blob storage. Only sends it to process
        toprocess.filefield[key] = await processing.convert_filefield_to_str(file_field)

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
            writer.files[key], storage
        )


def parse_external_file_field(
    key: str,
    file_field: models.FileField,
    writer: BrokerMessage,
    toprocess: PushPayload,
) -> None:
    writer.files[key].added.FromDatetime(datetime.now())
    if file_field.language:
        writer.files[key].language = file_field.language
    if file_field.extract_strategy is not None:
        writer.files[key].extract_strategy = file_field.extract_strategy
    uri = file_field.file.uri
    writer.files[key].url = uri  # type: ignore
    writer.files[key].file.uri = uri  # type: ignore
    writer.files[key].file.source = resources_pb2.CloudFile.Source.EXTERNAL
    writer.files[key].file.content_type = file_field.file.content_type
    if file_field.file.content_type and writer.basic.icon == GENERIC_MIME_TYPE:
        writer.basic.icon = file_field.file.content_type

    processing = get_processing()
    toprocess.filefield[key] = processing.convert_external_filefield_to_str(file_field)


def parse_link_field(
    key: str,
    link_field: models.LinkField,
    writer: BrokerMessage,
    toprocess: PushPayload,
) -> None:
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

    toprocess.linkfield[key] = models.LinkUpload(
        link=link_field.uri,
        headers=link_field.headers or {},
        cookies=link_field.cookies or {},
        localstorage=link_field.localstorage or {},
        css_selector=link_field.css_selector,
        xpath=link_field.xpath,
        extract_strategy=link_field.extract_strategy,
    )


async def parse_conversation_field(
    key: str,
    conversation_field: models.InputConversationField,
    writer: BrokerMessage,
    toprocess: PushPayload,
    kbid: str,
    uuid: str,
) -> None:
    storage = await get_storage(service_name=SERVICE_NAME)
    processing = get_processing()
    field_value = resources_pb2.Conversation()
    convs = models.PushConversation()
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

        processing_message_content = models.PushMessageContent(
            text=message.content.text,
            format=getattr(models.PushMessageFormat, message.content.format.value),
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

        for count, file in enumerate(message.content.attachments):
            sf_conv_field: StorageField = storage.conversation_field(
                kbid, uuid, field=key, ident=message.ident, count=count
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

        processing_message = models.PushMessage(
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

    toprocess.conversationfield[key] = convs
    writer.conversations[key].CopyFrom(field_value)
