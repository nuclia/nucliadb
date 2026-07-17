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


from typing import Sequence

from google.protobuf.message import Message

from nucliadb.common.datamanagers import fields_v2
from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import writer_pb2


async def get_raw(
    txn: Transaction, *, kbid: str, rid: str, field_type: str, field_id: str
) -> bytes | None:
    return await fields_v2.get_raw(txn, kbid=kbid, rid=rid, field_type=field_type, field_id=field_id)


async def set(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    value: Message,
):
    await fields_v2.set(txn, kbid=kbid, rid=rid, field_type=field_type, field_id=field_id, value=value)


async def delete(txn: Transaction, *, kbid: str, rid: str, field_type: str, field_id: str):
    await fields_v2.delete(txn, kbid=kbid, rid=rid, field_type=field_type, field_id=field_id)


# Status


async def get_status(
    txn: Transaction, *, kbid: str, rid: str, field_type: str, field_id: str
) -> writer_pb2.FieldStatus | None:
    return await fields_v2.get_status(txn, kbid=kbid, rid=rid, field_type=field_type, field_id=field_id)


async def get_statuses(
    txn: Transaction, *, kbid: str, rid: str, fields: Sequence[writer_pb2.FieldID]
) -> list[writer_pb2.FieldStatus]:
    return await fields_v2.get_statuses(txn, kbid=kbid, rid=rid, fields=fields)


async def set_status(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    status: writer_pb2.FieldStatus,
):
    await fields_v2.set_status(
        txn, kbid=kbid, rid=rid, field_type=field_type, field_id=field_id, status=status
    )
