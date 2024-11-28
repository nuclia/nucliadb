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

from nucliadb.ingest.orm.resource import Resource
from nucliadb.ingest.processing import ProcessingEngine, PushPayload
from nucliadb_protos import (
    writer_pb2,
)
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_utils.utilities import Utility, get_partitioning, get_utility

logger = logging.getLogger("ingest-processor")


@dataclass
class GeneratedFields:
    texts: list[str] = field(default_factory=list)
    links: list[str] = field(default_factory=list)
    files: list[str] = field(default_factory=list)
    conversations: list[str] = field(default_factory=list)

    def __len__(self):
        return len(self.texts) + len(self.links) + len(self.files) + len(self.conversations)


async def has_generated_fields(bm: writer_pb2.BrokerMessage, resource: Resource) -> bool:
    generated_fields = await _get_generated_fields(bm, resource)
    return len(generated_fields) > 0


async def _get_generated_fields(bm: writer_pb2.BrokerMessage, resource: Resource) -> GeneratedFields:
    """Processing can send messages with generated fields. Those can be
    generated with a data augmentation task and, as learning can't queue it to
    process, nucliadb is responsible to send the generated field to process (and
    ingest the processed thing later).

    Given a broker message and a resource, this function returns the list of
    generated fields, that can be empty.

    """
    generated_fields = GeneratedFields()

    # only messages from processor can add generated fields
    if bm.source != writer_pb2.BrokerMessage.MessageSource.PROCESSOR:
        return generated_fields

    # search all fields

    for field_id in bm.texts:
        if not resource.has_field(FieldType.TEXT, field_id):
            generated_fields.texts.append(field_id)

    for field_id in bm.links:
        if not resource.has_field(FieldType.LINK, field_id):
            generated_fields.links.append(field_id)

    for field_id in bm.files:
        if not resource.has_field(FieldType.FILE, field_id):
            generated_fields.files.append(field_id)

    for field_id in bm.conversations:
        if not resource.has_field(FieldType.CONVERSATION, field_id):
            generated_fields.conversations.append(field_id)

    return generated_fields


async def send_generated_fields_to_process(
    kbid: str,
    resource: Resource,
    bm: writer_pb2.BrokerMessage,
):
    generated_fields = await _get_generated_fields(bm, resource)

    partitioning = get_partitioning()
    partition = partitioning.generate_partition(kbid, resource.uuid)

    processing: ProcessingEngine = get_utility(Utility.PROCESSING)

    payload = _generate_processing_payload_for_fields(kbid, resource.uuid, generated_fields, bm)
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
) -> PushPayload:
    partitioning = get_partitioning()
    partition = partitioning.generate_partition(kbid, rid)

    payload = PushPayload(kbid=kbid, uuid=rid, userid="nucliadb-ingest", partition=partition)

    # TODO: implement

    return payload
