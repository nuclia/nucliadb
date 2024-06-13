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

KB_SYNONYMS = "/kbs/{kbid}/synonyms"


async def get(txn: Transaction, *, kbid: str) -> Optional[knowledgebox_pb2.Synonyms]:
    key = KB_SYNONYMS.format(kbid=kbid)
    return await get_kv_pb(txn, key, knowledgebox_pb2.Synonyms)


async def set(txn: Transaction, *, kbid: str, synonyms: knowledgebox_pb2.Synonyms):
    key = KB_SYNONYMS.format(kbid=kbid)
    await txn.set(key, synonyms.SerializeToString())


async def delete(txn: Transaction, *, kbid: str):
    key = KB_SYNONYMS.format(kbid=kbid)
    await txn.delete(key)
