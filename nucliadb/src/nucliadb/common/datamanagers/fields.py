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

from typing import Optional

from google.protobuf.message import Message

from nucliadb.common.datamanagers.utils import get_kv_pb
from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import writer_pb2

KB_RESOURCE_FIELD = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}"
KB_RESOURCE_FIELD_ERROR = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}/error"


async def get_raw(
    txn: Transaction, *, kbid: str, rid: str, field_type: str, field_id: str
) -> Optional[bytes]:
    key = KB_RESOURCE_FIELD.format(kbid=kbid, uuid=rid, type=field_type, field=field_id)
    return await txn.get(key)


async def set(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    value: Message,
):
    key = KB_RESOURCE_FIELD.format(kbid=kbid, uuid=rid, type=field_type, field=field_id)
    await txn.set(key, value.SerializeToString())


async def delete(txn: Transaction, *, kbid: str, rid: str, field_type: str, field_id: str):
    base_key = KB_RESOURCE_FIELD.format(kbid=kbid, uuid=rid, type=field_type, field=field_id)
    # Make sure we explicitly delete the field and any nested key
    keys_to_delete = []
    async for key in txn.keys(base_key):
        keys_to_delete.append(key)

    for key in keys_to_delete:
        await txn.delete(key)


# Error


async def get_error(
    txn: Transaction, *, kbid: str, rid: str, field_type: str, field_id: str
) -> Optional[writer_pb2.Error]:
    key = KB_RESOURCE_FIELD_ERROR.format(kbid=kbid, uuid=rid, type=field_type, field=field_id)
    return await get_kv_pb(txn, key, writer_pb2.Error)


async def set_error(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    error: writer_pb2.Error,
):
    key = KB_RESOURCE_FIELD_ERROR.format(kbid=kbid, uuid=rid, type=field_type, field=field_id)
    await txn.set(key, error.SerializeToString())
