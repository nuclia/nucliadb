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

import async_lru

from nucliadb.common import datamanagers
from nucliadb.common.external_index_providers.base import ExternalIndexManager
from nucliadb_protos.knowledgebox_pb2 import (
    StoredExternalIndexProviderMetadata,
)


async def get_external_index_manager(
    kbid: str, for_rollover: bool = False
) -> ExternalIndexManager | None:
    """
    Returns an ExternalIndexManager for the given kbid.
    If for_rollover is True, the ExternalIndexManager returned will include the rollover indexes (if any).
    """
    return None


@async_lru.alru_cache(maxsize=None)
async def get_external_index_metadata(kbid: str) -> StoredExternalIndexProviderMetadata | None:
    return await datamanagers.atomic.kb.get_external_index_provider_metadata(kbid=kbid)


@async_lru.alru_cache(maxsize=None)
async def get_default_vectorset_id(kbid: str) -> str | None:
    """
    While we are transitioning to the new vectorset system, we need to take into account
    that KBs that have only one semantic model will have the `vectorset_id` field on BrokerMessage.field_vectors
    set to empty string -- that is the `default` vectorset concept.
    """
    async with datamanagers.with_ro_transaction() as txn:
        vss = []
        async for vs_id, vs_config in datamanagers.vectorsets.iter(txn, kbid=kbid):
            vss.append((vs_id, vs_config))
        if len(vss) == 0:
            # If there is nothing in the vectorsets key on maindb, we use the "__default__" vectorset as id.
            return "__default__"
        if len(vss) == 1:
            # If there is only one vectorset, return it as the default
            return vss[0][0]
        else:
            # If there are multiple vectorsets, we don't have a default
            # and we assume the index messages are explicit about the vectorset
            return None


async def get_rollover_external_index_metadata(
    kbid: str,
) -> StoredExternalIndexProviderMetadata | None:
    async with datamanagers.with_ro_transaction() as txn:
        return await datamanagers.rollover.get_kb_rollover_external_index_metadata(txn, kbid=kbid)
