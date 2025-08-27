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
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.resource import Resource
from nucliadb_protos import audit_pb2, writer_pb2
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_utils.storages.storage import Storage


async def collect_audit_fields(
    driver: Driver, storage: Storage, message: writer_pb2.BrokerMessage
) -> list[audit_pb2.AuditField]:
    if message.type == writer_pb2.BrokerMessage.MessageType.DELETE:
        # If we are fully deleting a resource we won't iterate the delete_fields (if any).
        # Make no sense as we already collected all resource fields as deleted
        return []

    audit_storage_fields: list[audit_pb2.AuditField] = []
    async with driver.ro_transaction() as txn:
        kb = KnowledgeBox(txn, storage, message.kbid)
        resource = Resource(txn, storage, kb, message.uuid)
        field_keys = await resource.get_fields_ids()

        for field_id, field_type in iterate_auditable_fields(field_keys, message):
            auditfield = audit_pb2.AuditField()
            auditfield.field_type = field_type
            auditfield.field_id = field_id
            if field_type is writer_pb2.FieldType.FILE:
                auditfield.filename = message.files[field_id].file.filename
            # The field did exist, so we are overwriting it, with a modified file
            # in case of a file
            auditfield.action = audit_pb2.AuditField.FieldAction.MODIFIED
            if field_type is writer_pb2.FieldType.FILE:
                auditfield.size = message.files[field_id].file.size

            audit_storage_fields.append(auditfield)

        for fieldid in message.delete_fields or []:
            field = await resource.get_field(fieldid.field, writer_pb2.FieldType.FILE, load=True)
            audit_field = audit_pb2.AuditField()
            audit_field.action = audit_pb2.AuditField.FieldAction.DELETED
            audit_field.field_id = fieldid.field
            audit_field.field_type = fieldid.field_type
            if fieldid.field_type is writer_pb2.FieldType.FILE:
                val = await field.get_value()
                audit_field.size = 0
                if val is not None:
                    audit_field.filename = val.file.filename
            audit_storage_fields.append(audit_field)

    return audit_storage_fields


def iterate_auditable_fields(
    resource_keys: list[tuple[FieldType.ValueType, str]],
    message: writer_pb2.BrokerMessage,
):
    """
    Generator that emits the combined list of field ids from both
    the existing resource and message that needs to be considered
    in the audit of fields.
    """
    yielded = set()

    # Include all fields present in the message we are processing
    for field_id in message.files.keys():
        key = (field_id, writer_pb2.FieldType.FILE)
        yield key
        yielded.add(key)

    for field_id in message.conversations.keys():
        key = (field_id, writer_pb2.FieldType.CONVERSATION)
        yield key
        yielded.add(key)

    for field_id in message.texts.keys():
        key = (field_id, writer_pb2.FieldType.TEXT)
        yield key
        yielded.add(key)

    for field_id in message.links.keys():
        key = (field_id, writer_pb2.FieldType.LINK)
        yield key
        yielded.add(key)

    for field_type, field_id in resource_keys:
        if field_type is writer_pb2.FieldType.GENERIC:
            continue

        if not (
            field_id in message.files or message.type is writer_pb2.BrokerMessage.MessageType.DELETE
        ):
            continue

        # Avoid duplicates
        if (field_type, field_id) in yielded:
            continue

        yield (field_id, field_type)
