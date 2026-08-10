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
from unittest.mock import Mock, patch

from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.resource import Resource
from nucliadb.migrator.models import Migration
from nucliadb_protos import resources_pb2
from tests.nucliadb.migrations import get_migration

migration: Migration = get_migration(51)


async def test_migration_0051(maindb_driver: Driver):
    """Reproduce the replace_field bug in the DB, run the migration, verify repair."""
    execution_context = Mock()
    execution_context.kv_driver = maindb_driver
    storage = Mock()
    storage.needs_move = Mock(return_value=False)
    execution_context.blob_storage = storage

    kbid = str(uuid.uuid4())
    rid = Resource.new_unique_rid()
    field_id = "chat"

    with patch("nucliadb.ingest.orm.resource.get_storage"):
        async with maindb_driver.rw_transaction() as txn:
            await datamanagers.kb.set_slug(txn, kbid=kbid, slug=f"slug-{kbid}")
            kb_obj = KnowledgeBox(txn, storage, kbid)
            await datamanagers.resources.set_slug(txn, kbid=kbid, rid=rid, slug=f"slug-{rid}")
            created_resource = await kb_obj.add_resource(rid, f"slug-{rid}")

            # Initial write: 2 messages on page 1.
            initial = resources_pb2.Conversation()
            initial.messages.extend(
                [
                    resources_pb2.Message(ident="m1"),
                    resources_pb2.Message(ident="m2"),
                ]
            )
            await created_resource.set_field(resources_pb2.FieldType.CONVERSATION, field_id, initial)
            await txn.commit()

        # Simulate the buggy replace_field behaviour directly in the DB:
        #   - delete the conversation pages from kb_conversations
        #   - leave the stale FieldConversation metadata in kb_fields (pages=1)
        #   - insert replacement messages at page 2 instead of page 1
        async with maindb_driver.rw_transaction() as txn:
            await datamanagers.conversations.delete_field(txn, kbid=kbid, rid=rid, field_id=field_id)
            await datamanagers.fields.set(
                txn,
                kbid=kbid,
                rid=rid,
                field_type="c",
                field_id=field_id,
                value=resources_pb2.FieldConversation(pages=2, total=2, size=200),
            )

            replacement_conv = resources_pb2.Conversation()
            replacement_conv.messages.extend(
                [
                    resources_pb2.Message(ident="r1"),
                    resources_pb2.Message(ident="r2"),
                ]
            )
            replacement_splits = resources_pb2.SplitsMetadata()
            replacement_splits.metadata["r1"].page = 2
            replacement_splits.metadata["r2"].page = 2

            await datamanagers.conversations.set_page(
                txn, kbid=kbid, rid=rid, field_id=field_id, page=2, value=replacement_conv
            )
            await datamanagers.conversations.set_splits_metadata(
                txn, kbid=kbid, rid=rid, field_id=field_id, splits_metadata=replacement_splits
            )

            # Stale metadata: pages=2, total=2 (as the bug would leave it)
            stale_meta = resources_pb2.FieldConversation()
            stale_meta.pages = 2
            stale_meta.total = 2
            stale_meta.size = 200
            await datamanagers.conversations.set_metadata(
                txn, kbid=kbid, rid=rid, field_id=field_id, metadata=stale_meta
            )
            await txn.commit()

        # Confirm the broken state before running the migration
        async with maindb_driver.ro_transaction() as txn:
            meta = await datamanagers.conversations.get_metadata(
                txn, kbid=kbid, rid=rid, field_id=field_id
            )
            assert meta is not None and meta.pages == 2
            assert (
                await datamanagers.conversations.get_page(
                    txn, kbid=kbid, rid=rid, field_id=field_id, page=1
                )
                is None
            ), "page 1 should be absent before migration"
            assert (
                await datamanagers.conversations.get_page(
                    txn, kbid=kbid, rid=rid, field_id=field_id, page=2
                )
                is not None
            )
            splits_meta = await datamanagers.conversations.get_splits_metadata(
                txn, kbid=kbid, rid=rid, field_id=field_id
            )
            assert splits_meta is not None
            assert splits_meta.metadata["r1"].page == 2
            assert splits_meta.metadata["r2"].page == 2

        # Run the migration
        await migration.module.migrate_kb(execution_context, kbid)

        # Verify the repair
        async with maindb_driver.ro_transaction() as txn:
            kb_obj = KnowledgeBox(txn, storage, kbid)
            r_obj = await kb_obj.get(rid)
            assert r_obj is not None
            conv: Conversation = await r_obj.get_field(
                field_id, resources_pb2.FieldType.CONVERSATION, load=False
            )

            meta = await conv.get_metadata()
            assert meta is not None
            assert meta.pages == 1, f"expected pages=1 after migration, got {meta.pages}"
            assert meta.total == 2

            page1 = await conv.get_value(page=1)
            assert page1 is not None, "page 1 must exist after migration"
            assert [m.ident for m in page1.messages] == ["r1", "r2"]

            page2 = await conv.get_value(page=2)
            assert page2 is None, "page 2 must be gone after migration"

            splits = await conv.get_splits_metadata()
            assert splits.metadata["r1"].page == 1
            assert splits.metadata["r2"].page == 1


async def test_migration_0051_multipage(maindb_driver: Driver):
    """Same bug but the replacement content itself spanned multiple pages."""
    execution_context = Mock()
    execution_context.kv_driver = maindb_driver
    storage = Mock()
    storage.needs_move = Mock(return_value=False)
    execution_context.blob_storage = storage

    kbid = str(uuid.uuid4())
    rid = Resource.new_unique_rid()
    field_id = "chat"

    with patch("nucliadb.ingest.orm.resource.get_storage"):
        async with maindb_driver.rw_transaction() as txn:
            await datamanagers.kb.set_slug(txn, kbid=kbid, slug=f"slug-{kbid}")
            kb_obj = KnowledgeBox(txn, storage, kbid)
            await datamanagers.resources.set_slug(txn, kbid=kbid, rid=rid, slug=f"slug-{rid}")
            await kb_obj.add_resource(rid, f"slug-{rid}")
            await txn.commit()

        # Directly write the broken state: replacement content sits on pages 2 and 3
        # (as if the original field had 1 page and set_value bumped pages to 2 then 3).
        async with maindb_driver.rw_transaction() as txn:
            await datamanagers.fields.set(
                txn,
                kbid=kbid,
                rid=rid,
                field_type="c",
                field_id=field_id,
                value=resources_pb2.FieldConversation(pages=3, total=5, size=3),
            )

            page2 = resources_pb2.Conversation()
            page2.messages.extend([resources_pb2.Message(ident=f"p2m{i}") for i in range(3)])
            page3 = resources_pb2.Conversation()
            page3.messages.extend([resources_pb2.Message(ident=f"p3m{i}") for i in range(2)])

            await datamanagers.conversations.set_page(
                txn, kbid=kbid, rid=rid, field_id=field_id, page=2, value=page2
            )
            await datamanagers.conversations.set_page(
                txn, kbid=kbid, rid=rid, field_id=field_id, page=3, value=page3
            )

            splits = resources_pb2.SplitsMetadata()
            for i in range(3):
                splits.metadata[f"p2m{i}"].page = 2
            for i in range(2):
                splits.metadata[f"p3m{i}"].page = 3
            await datamanagers.conversations.set_splits_metadata(
                txn, kbid=kbid, rid=rid, field_id=field_id, splits_metadata=splits
            )

            stale_meta = resources_pb2.FieldConversation(pages=3, total=5, size=3)
            await datamanagers.conversations.set_metadata(
                txn, kbid=kbid, rid=rid, field_id=field_id, metadata=stale_meta
            )
            await txn.commit()

        await migration.module.migrate_kb(execution_context, kbid)

        async with maindb_driver.ro_transaction() as txn:
            kb_obj = KnowledgeBox(txn, storage, kbid)
            r_obj = await kb_obj.get(rid)
            assert r_obj is not None
            conv: Conversation = await r_obj.get_field(
                field_id, resources_pb2.FieldType.CONVERSATION, load=False
            )

            meta = await conv.get_metadata()
            assert meta is not None
            assert meta.pages == 2, f"expected pages=2 after migration, got {meta.pages}"
            assert meta.total == 5

            page1 = await conv.get_value(page=1)
            assert page1 is not None
            assert [m.ident for m in page1.messages] == [f"p2m{i}" for i in range(3)]

            page2_result = await conv.get_value(page=2)
            assert page2_result is not None
            assert [m.ident for m in page2_result.messages] == [f"p3m{i}" for i in range(2)]

            assert await conv.get_value(page=3) is None

            result_splits = await conv.get_splits_metadata()
            for i in range(3):
                assert result_splits.metadata[f"p2m{i}"].page == 1
            for i in range(2):
                assert result_splits.metadata[f"p3m{i}"].page == 2
