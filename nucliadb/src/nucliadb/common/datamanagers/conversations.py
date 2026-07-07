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
from nucliadb.common.datamanagers import conversations_v2
from nucliadb.common.datamanagers.utils import datamanagers_v2_read, datamanagers_v2_write
from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos.resources_pb2 import Conversation as PBConversation
from nucliadb_protos.resources_pb2 import FieldConversation, SplitsMetadata

KB_CONVERSATION_PAGE = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}/{page}"
KB_CONVERSATION_SPLITS_METADATA = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}/splits_metadata"
KB_CONVERSATION_METADATA = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}"


async def get_page(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    page: int,
) -> PBConversation | None:
    if page <= 0:
        raise ValueError("Conversation pages start at index 1")

    if datamanagers_v2_read(kbid):
        return await conversations_v2.get_page(txn, kbid=kbid, rid=rid, field_id=field_id, page=page)

    key = KB_CONVERSATION_PAGE.format(kbid=kbid, uuid=rid, type=field_type, field=field_id, page=page)
    payload = await txn.get(key)
    if payload is None:
        return None
    pb = PBConversation()
    pb.ParseFromString(payload)
    return pb


async def set_page(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    page: int,
    value: PBConversation,
) -> None:
    key = KB_CONVERSATION_PAGE.format(kbid=kbid, uuid=rid, type=field_type, field=field_id, page=page)
    await txn.set(key, value.SerializeToString())

    if datamanagers_v2_write(kbid):
        await conversations_v2.set_page(
            txn, kbid=kbid, rid=rid, field_id=field_id, page=page, value=value
        )


async def get_metadata(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
) -> FieldConversation | None:
    if datamanagers_v2_read(kbid):
        return await conversations_v2.get_metadata(txn, kbid=kbid, rid=rid, field_id=field_id)

    key = KB_CONVERSATION_METADATA.format(kbid=kbid, uuid=rid, type=field_type, field=field_id)
    payload = await txn.get(key)
    if payload is None:
        return None
    pb = FieldConversation()
    pb.ParseFromString(payload)
    return pb


async def set_metadata(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    metadata: FieldConversation,
) -> None:
    key = KB_CONVERSATION_METADATA.format(kbid=kbid, uuid=rid, type=field_type, field=field_id)
    await txn.set(key, metadata.SerializeToString())

    if datamanagers_v2_write(kbid):
        await conversations_v2.set_metadata(
            txn, kbid=kbid, rid=rid, field_id=field_id, metadata=metadata
        )


async def delete_field(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
) -> None:
    base_key = KB_CONVERSATION_METADATA.format(kbid=kbid, uuid=rid, type=field_type, field=field_id)
    await txn.delete_by_prefix(base_key)

    if datamanagers_v2_write(kbid):
        await conversations_v2.delete_field(txn, kbid=kbid, rid=rid, field_id=field_id)


async def get_splits_metadata(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
) -> SplitsMetadata | None:
    if datamanagers_v2_read(kbid):
        return await conversations_v2.get_splits_metadata(txn, kbid=kbid, rid=rid, field_id=field_id)

    key = KB_CONVERSATION_SPLITS_METADATA.format(kbid=kbid, uuid=rid, type=field_type, field=field_id)
    payload = await txn.get(key)
    if payload is None:
        return None
    pb = SplitsMetadata()
    pb.ParseFromString(payload)
    return pb


async def set_splits_metadata(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    splits_metadata: SplitsMetadata,
) -> None:
    key = KB_CONVERSATION_SPLITS_METADATA.format(kbid=kbid, uuid=rid, type=field_type, field=field_id)
    await txn.set(key, splits_metadata.SerializeToString())

    if datamanagers_v2_write(kbid):
        await conversations_v2.set_splits_metadata(
            txn, kbid=kbid, rid=rid, field_id=field_id, splits_metadata=splits_metadata
        )
