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
from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos.resources_pb2 import Conversation as PBConversation
from nucliadb_protos.resources_pb2 import FieldConversation, SplitsMetadata


async def get_page(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    page: int,
) -> PBConversation | None:
    return await conversations_v2.get_page(txn, kbid=kbid, rid=rid, field_id=field_id, page=page)


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

    await conversations_v2.set_page(txn, kbid=kbid, rid=rid, field_id=field_id, page=page, value=value)


async def get_metadata(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
) -> FieldConversation | None:
    return await conversations_v2.get_metadata(txn, kbid=kbid, rid=rid, field_id=field_id)


async def set_metadata(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    metadata: FieldConversation,
) -> None:

    await conversations_v2.set_metadata(txn, kbid=kbid, rid=rid, field_id=field_id, metadata=metadata)


async def delete_field(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
) -> None:

    await conversations_v2.delete_field(txn, kbid=kbid, rid=rid, field_id=field_id)


async def get_splits_metadata(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
) -> SplitsMetadata | None:

    return await conversations_v2.get_splits_metadata(txn, kbid=kbid, rid=rid, field_id=field_id)


async def set_splits_metadata(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    splits_metadata: SplitsMetadata,
) -> None:
    await conversations_v2.set_splits_metadata(
        txn, kbid=kbid, rid=rid, field_id=field_id, splits_metadata=splits_metadata
    )
