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
import logging
from typing import AsyncIterator, Optional

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import knowledgebox_pb2

from . import cluster

KB_UUID = "/kbs/{kbid}/config"
KB_SLUGS_BASE = "/kbslugs/"
KB_SLUGS = KB_SLUGS_BASE + "{slug}"

logger = logging.getLogger(__name__)


async def get_kbs(txn: Transaction, *, prefix: str = "") -> AsyncIterator[tuple[str, str]]:
    async for key in txn.keys(KB_SLUGS.format(slug=prefix)):
        slug = key.replace(KB_SLUGS_BASE, "")
        uuid = await get_kb_uuid(txn, slug=slug)
        if uuid is None:
            logger.error(f"KB with slug ({slug}) but without uuid?")
            continue
        yield (uuid, slug)


async def exists_kb(txn: Transaction, *, kbid: str) -> bool:
    return await get_config(txn, kbid=kbid, for_update=False) is not None


async def get_kb_uuid(txn: Transaction, *, slug: str) -> Optional[str]:
    uuid = await txn.get(KB_SLUGS.format(slug=slug), for_update=False)
    if uuid is not None:
        return uuid.decode()
    else:
        return None


async def set_kbid_for_slug(txn: Transaction, *, slug: str, kbid: str):
    key = KB_SLUGS.format(slug=slug)
    await txn.set(key, kbid.encode())


async def delete_kb_slug(txn: Transaction, *, slug: str):
    key = KB_SLUGS.format(slug=slug)
    await txn.delete(key)


async def get_config(
    txn: Transaction, *, kbid: str, for_update: bool = False
) -> Optional[knowledgebox_pb2.KnowledgeBoxConfig]:
    key = KB_UUID.format(kbid=kbid)
    payload = await txn.get(key, for_update=for_update)
    if payload is None:
        return None
    response = knowledgebox_pb2.KnowledgeBoxConfig()
    response.ParseFromString(payload)
    return response


async def set_config(txn: Transaction, *, kbid: str, config: knowledgebox_pb2.KnowledgeBoxConfig):
    key = KB_UUID.format(kbid=kbid)
    await txn.set(key, config.SerializeToString())


async def delete_config(txn: Transaction, *, kbid: str) -> None:
    key = KB_UUID.format(kbid=kbid)
    await txn.delete(key)


async def get_model_metadata(txn: Transaction, *, kbid: str) -> knowledgebox_pb2.SemanticModelMetadata:
    shards_obj = await cluster.get_kb_shards(txn, kbid=kbid, for_update=False)
    if shards_obj is None:
        raise KnowledgeBoxNotFound(kbid)
    if shards_obj.HasField("model"):
        return shards_obj.model
    else:
        # B/c code for old KBs that do not have the `model` attribute set in the Shards object.
        # Cleanup this code after a migration is done unifying all fields under `model` (on-prem and cloud).
        return knowledgebox_pb2.SemanticModelMetadata(similarity_function=shards_obj.similarity)


# DEPRECATED: this function should be removed once the "default" vectorset
# concept is removed and processing sends us all messages with a vectorset_id
async def get_matryoshka_vector_dimension(
    txn: Transaction,
    *,
    kbid: str,
    vectorset_id: Optional[str] = None,
) -> Optional[int]:
    """Return vector dimension for matryoshka models"""
    from . import vectorsets

    async for _, vs in vectorsets.iter(txn, kbid=kbid):
        if len(vs.matryoshka_dimensions) > 0 and vs.vectorset_index_config.vector_dimension:
            if vs.vectorset_index_config.vector_dimension in vs.matryoshka_dimensions:
                return vs.vectorset_index_config.vector_dimension
            else:
                logger.error(
                    "KB has an invalid matryoshka dimension!",
                    extra={
                        "kbid": kbid,
                        "vector_dimension": vs.vectorset_index_config.vector_dimension,
                        "matryoshka_dimensions": vs.matryoshka_dimensions,
                    },
                )
        return None
    else:
        # fallback for KBs that don't have vectorset
        model_metadata = await get_model_metadata(txn, kbid=kbid)
        dimension = None
        if len(model_metadata.matryoshka_dimensions) > 0 and model_metadata.vector_dimension:
            if model_metadata.vector_dimension in model_metadata.matryoshka_dimensions:
                dimension = model_metadata.vector_dimension
            else:
                logger.error(
                    "KB has an invalid matryoshka dimension!",
                    extra={
                        "kbid": kbid,
                        "vector_dimension": model_metadata.vector_dimension,
                        "matryoshka_dimensions": model_metadata.matryoshka_dimensions,
                    },
                )
        return dimension


async def get_external_index_provider_metadata(
    txn: Transaction, *, kbid: str
) -> Optional[knowledgebox_pb2.StoredExternalIndexProviderMetadata]:
    kb_config = await get_config(txn, kbid=kbid)
    if kb_config is None:
        return None
    return kb_config.external_index_provider


async def set_external_index_provider_metadata(
    txn: Transaction, *, kbid: str, metadata: knowledgebox_pb2.StoredExternalIndexProviderMetadata
):
    kb_config = await get_config(txn, kbid=kbid)
    if kb_config is None:
        raise KnowledgeBoxNotFound(kbid)
    kb_config.external_index_provider.CopyFrom(metadata)
    await set_config(txn, kbid=kbid, config=kb_config)
