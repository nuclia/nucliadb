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
from typing import AsyncIterator, Optional

from nucliadb.common.datamanagers.utils import get_kv_pb
from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import knowledgebox_pb2

KB_VECTORSETS = "/kbs/{kbid}/vectorsets"


class BrokenInvariant(Exception):
    pass


async def initialize(txn: Transaction, *, kbid: str):
    key = KB_VECTORSETS.format(kbid=kbid)
    await txn.set(key, knowledgebox_pb2.KnowledgeBoxVectorSetsConfig().SerializeToString())


async def get(
    txn: Transaction, *, kbid: str, vectorset_id: str
) -> Optional[knowledgebox_pb2.VectorSetConfig]:
    kb_vectorsets = await _get_or_default(txn, kbid=kbid, for_update=False)
    index = _find_vectorset(kb_vectorsets, vectorset_id)
    if index is None:
        return None
    return kb_vectorsets.vectorsets[index]


async def exists(txn, *, kbid: str, vectorset_id: str) -> bool:
    kb_vectorsets = await _get_or_default(txn, kbid=kbid, for_update=False)
    return _find_vectorset(kb_vectorsets, vectorset_id) is not None


async def iter(
    txn: Transaction, *, kbid: str
) -> AsyncIterator[tuple[str, knowledgebox_pb2.VectorSetConfig]]:
    kb_vectorsets = await _get_or_default(txn, kbid=kbid, for_update=False)
    for config in kb_vectorsets.vectorsets:
        yield config.vectorset_id, config


async def count(txn: Transaction, *, kbid: str) -> int:
    kb_vectorsets = await _get_or_default(txn, kbid=kbid, for_update=False)
    return len(kb_vectorsets.vectorsets)


async def set(txn: Transaction, *, kbid: str, config: knowledgebox_pb2.VectorSetConfig):
    """Create or update a vectorset configuration"""
    kb_vectorsets = await _get_or_default(txn, kbid=kbid, for_update=True)
    index = _find_vectorset(kb_vectorsets, config.vectorset_id)
    if index is None:
        # adding a new vectorset
        kb_vectorsets.vectorsets.append(config)
    else:
        # updating a vectorset
        kb_vectorsets.vectorsets[index].CopyFrom(config)

    key = KB_VECTORSETS.format(kbid=kbid)
    await txn.set(key, kb_vectorsets.SerializeToString())


async def delete(
    txn: Transaction, *, kbid: str, vectorset_id: str
) -> Optional[knowledgebox_pb2.VectorSetConfig]:
    kb_vectorsets = await _get_or_default(txn, kbid=kbid, for_update=True)
    index = _find_vectorset(kb_vectorsets, vectorset_id)
    if index is None:
        # already deleted
        return None

    deleted = kb_vectorsets.vectorsets[index]
    del kb_vectorsets.vectorsets[index]
    key = KB_VECTORSETS.format(kbid=kbid)
    await txn.set(key, kb_vectorsets.SerializeToString())
    return deleted


# XXX At some point in the vectorset epic, we should make this key mandatory and
# fail instead of providing a default
async def _get_or_default(
    txn: Transaction,
    *,
    kbid: str,
    for_update: bool = True,
) -> knowledgebox_pb2.KnowledgeBoxVectorSetsConfig:
    key = KB_VECTORSETS.format(kbid=kbid)
    stored = await get_kv_pb(
        txn, key, knowledgebox_pb2.KnowledgeBoxVectorSetsConfig, for_update=for_update
    )
    return stored or knowledgebox_pb2.KnowledgeBoxVectorSetsConfig()


def _find_vectorset(
    kb_vectorsets: knowledgebox_pb2.KnowledgeBoxVectorSetsConfig, vectorset_id: str
) -> Optional[int]:
    """Return the position of the vectorset in `vectorsets` or `None` if not found."""
    for idx, vectorset in enumerate(kb_vectorsets.vectorsets):
        if vectorset.vectorset_id == vectorset_id:
            return idx
    return None
