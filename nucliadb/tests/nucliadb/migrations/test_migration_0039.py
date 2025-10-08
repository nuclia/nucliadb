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
import uuid
from unittest.mock import Mock

from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.fields.conversation import CONVERSATION_SPLITS_METADATA, Conversation
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.migrator.models import Migration
from nucliadb_protos import resources_pb2
from tests.nucliadb.migrations import get_migration

migration: Migration = get_migration(39)


async def test_migration_0039(maindb_driver: Driver):
    execution_context = Mock()
    execution_context.kv_driver = maindb_driver
    storage = Mock()
    execution_context.blob_storage = storage

    kbid = str(uuid.uuid4())
    rid = str(uuid.uuid4())
    field_id = "faq"

    # Create a fake conversation field with only one message
    async with maindb_driver.rw_transaction() as txn:
        kb_obj = KnowledgeBox(txn, storage, kbid)
        r_obj = await kb_obj.add_resource(rid, "slug")
        conv_pb = resources_pb2.Conversation()
        conv_pb.messages.append(resources_pb2.Message(ident="foo"))
        await r_obj.set_field(resources_pb2.FieldType.CONVERSATION, field_id, conv_pb)
        await txn.commit()

    # Delete the key simulating as it wasn't there
    async with maindb_driver.rw_transaction() as txn:
        splits_metadata_key = CONVERSATION_SPLITS_METADATA.format(
            kbid=kbid, uuid=rid, type="c", field=field_id
        )
        await txn.delete(splits_metadata_key)
        await txn.commit()

    await migration.module.migrate_kb(execution_context, kbid)

    # Make sure that the splits metadata has been populated correctly
    async with maindb_driver.ro_transaction() as txn:
        kb_obj = KnowledgeBox(txn, storage, kbid)
        resource_obj = await kb_obj.get(rid)
        assert resource_obj
        conv: Conversation = await resource_obj.get_field(
            field_id, resources_pb2.FieldType.CONVERSATION, load=False
        )
        splits_metadata = await conv.get_splits_metadata()
        assert splits_metadata.metadata["foo"] is not None
