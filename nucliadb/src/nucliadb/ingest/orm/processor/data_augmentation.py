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

import logging
from dataclasses import dataclass, field
from typing import Optional

from nucliadb.ingest.orm.resource import Resource
from nucliadb.ingest.processing import ProcessingEngine
from nucliadb.models.internal.processing import PushPayload, PushTextFormat, Source, Text
from nucliadb_protos import resources_pb2, writer_pb2
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_utils.utilities import Utility, get_partitioning, get_utility

logger = logging.getLogger("ingest-processor")


@dataclass
class GeneratedFields:
    texts: list[str] = field(default_factory=list)
    links: list[str] = field(default_factory=list)
    files: list[str] = field(default_factory=list)
    conversations: list[str] = field(default_factory=list)

    def is_not_empty(self) -> bool:
        return (len(self.texts) + len(self.links) + len(self.files) + len(self.conversations)) > 0


async def get_generated_fields(bm: writer_pb2.BrokerMessage, resource: Resource) -> GeneratedFields:
    """Processing can send messages with generated fields. Those can be
    generated with a data augmentation task and, as learning can't queue it to
    process, nucliadb is responsible to send the generated field to process (and
    ingest the processed thing later).

    Given a broker message and a resource, this function returns the list of
    generated fields, that can be empty. It skips fields with errors.

    """
    generated_fields = GeneratedFields()

    # only messages from processor can add generated fields
    if bm.source != writer_pb2.BrokerMessage.MessageSource.PROCESSOR:
        return generated_fields

    # search all fields
    for field_id, text in bm.texts.items():
        errors = [e for e in bm.errors if e.field_type == FieldType.TEXT and e.field == field_id]
        has_error = len(errors) > 0
        if text.generated_by.WhichOneof("author") == "data_augmentation" and not has_error:
            generated_fields.texts.append(field_id)

    return generated_fields


async def send_generated_fields_to_process(
    kbid: str,
    resource: Resource,
    generated_fields: GeneratedFields,
    bm: writer_pb2.BrokerMessage,
):
    partitioning = get_partitioning()
    partition = partitioning.generate_partition(kbid, resource.uuid)

    processing: ProcessingEngine = get_utility(Utility.PROCESSING)

    payload = _generate_processing_payload_for_fields(kbid, resource.uuid, generated_fields, bm)
    if payload is not None:
        processing_info = await processing.send_to_process(payload, partition)
        logger.info(
            "Sent generated fields to process",
            extra={"processing_info": processing_info},
        )


def _generate_processing_payload_for_fields(
    kbid: str,
    rid: str,
    fields: GeneratedFields,
    bm: writer_pb2.BrokerMessage,
) -> Optional[PushPayload]:
    partitioning = get_partitioning()
    partition = partitioning.generate_partition(kbid, rid)

    payload = PushPayload(kbid=kbid, uuid=rid, userid="nucliadb-ingest", partition=partition)

    payload.kbid = bm.kbid
    payload.uuid = rid
    payload.source = Source.INGEST
    payload.slug = bm.slug

    # populate generated fields

    for text in fields.texts:
        payload.textfield[text] = _bm_text_field_to_processing(bm.texts[text])

    for file in fields.files:
        logger.warning(
            "Ingest received a broker message from processor with a new file field! Skipping",
            extra={"kbid": kbid, "rid": rid, "field_id": file},
        )
        pass

    for link in fields.links:
        logger.warning(
            "Ingest received a broker message from processor with a new link field!",
            extra={"kbid": kbid, "rid": rid, "field_id": link},
        )
        pass

    for conversation in fields.conversations:
        logger.warning(
            "Ingest received a broker message from processor with a new conversation field! Skipping",
            extra={"kbid": kbid, "rid": rid, "field_id": conversation},
        )
        pass

    if len(fields.texts) > 0:
        return payload
    else:
        # we don't want to send weird empty messages to processing
        return None


def _bm_text_field_to_processing(text_field: resources_pb2.FieldText) -> Text:
    return Text(body=text_field.body, format=PushTextFormat(text_field.format))
