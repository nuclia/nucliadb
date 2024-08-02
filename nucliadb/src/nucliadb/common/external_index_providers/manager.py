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

import async_lru

from nucliadb.common import datamanagers
from nucliadb.common.external_index_providers.base import ExternalIndexManager
from nucliadb.common.external_index_providers.pinecone import PineconeIndexManager
from nucliadb.common.external_index_providers.settings import settings
from nucliadb_protos.knowledgebox_pb2 import (
    ExternalIndexProviderType,
    StoredExternalIndexProviderMetadata,
)
from nucliadb_utils.utilities import get_endecryptor


async def get_external_index_manager(kbid: str) -> Optional[ExternalIndexManager]:
    """
    Returns an ExternalIndexManager for the given kbid
    """
    metadata = await get_external_index_metadata(kbid)
    if metadata is None or metadata.type != ExternalIndexProviderType.PINECONE:
        return None
    api_key = decrypt_api_key(metadata.pinecone_config.encrypted_api_key)
    default_vectorset = await get_default_vectorset_id(kbid)
    return PineconeIndexManager(
        kbid=kbid,
        api_key=api_key,
        indexes=dict(metadata.pinecone_config.indexes),
        upsert_parallelism=settings.pinecone_upsert_parallelism,
        delete_parallelism=settings.pinecone_delete_parallelism,
        upsert_timeout=settings.pinecone_upsert_timeout,
        delete_timeout=settings.pinecone_delete_timeout,
        default_vectorset=default_vectorset,
    )


@async_lru.alru_cache(maxsize=None)
async def get_external_index_metadata(kbid: str) -> Optional[StoredExternalIndexProviderMetadata]:
    return await datamanagers.atomic.kb.get_external_index_provider_metadata(kbid=kbid)


@async_lru.alru_cache(maxsize=None)
async def get_default_vectorset_id(kbid: str) -> str:
    async with datamanagers.with_ro_transaction() as txn:
        vs = await datamanagers.vectorsets.get_default_vectorset(txn, kbid=kbid)
        return vs.vectorset_id


def decrypt_api_key(encrypted_api_key: str) -> str:
    endecryptor = get_endecryptor()
    return endecryptor.decrypt(encrypted_api_key)
