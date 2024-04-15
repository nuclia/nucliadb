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

from nucliadb.common.datamanagers.utils import get_kv_pb
from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import knowledgebox_pb2

KB_VECTORSET = "/kbs/{kbid}/vectorsets"


async def get_vectorsets(
    txn: Transaction, *, kbid: str
) -> Optional[knowledgebox_pb2.VectorSets]:
    key = KB_VECTORSET.format(kbid=kbid)
    return await get_kv_pb(txn, key, knowledgebox_pb2.VectorSets)


async def set_vectorset(
    txn: Transaction, *, kbid: str, vectorset_id: str, vs: knowledgebox_pb2.VectorSet
):
    vectorsets = await get_vectorsets(txn, kbid=kbid)
    if vectorsets is None:
        vectorsets = knowledgebox_pb2.VectorSets()

    vectorsets.vectorsets[vectorset_id].CopyFrom(vs)
    key = KB_VECTORSET.format(kbid=kbid)
    await txn.set(key, vectorsets.SerializeToString())


async def del_vectorset(txn: Transaction, *, kbid: str, vectorset_id: str):
    vectorsets = await get_vectorsets(txn, kbid=kbid)
    if vectorsets is None:
        return

    if vectorset_id not in vectorsets.vectorsets:
        return

    del vectorsets.vectorsets[vectorset_id]

    key = KB_VECTORSET.format(kbid=kbid)
    await txn.set(key, vectorsets.SerializeToString())
