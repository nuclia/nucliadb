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

import backoff
from fastapi import HTTPException

from nucliadb import learning_proxy
from nucliadb.common import datamanagers
from nucliadb.common.cluster.exceptions import NodeError
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.writer import logger
from nucliadb_protos import knowledgebox_pb2
from nucliadb_telemetry import errors
from nucliadb_utils.utilities import get_storage


async def add(kbid: str, embedding_id: str) -> None:
    await add_semantic_model_to_learning_config(kbid, semantic_model=embedding_id)
    await add_vectorset_to_index(kbid, vectorset_id=embedding_id)

    # TODO: Start a task to recompute the embeddings for this kb
    return


async def delete(kbid: str, embedding_id: str) -> None:
    await delete_semantic_model_from_learning_config(kbid, semantic_model=embedding_id)
    await delete_vectorset_from_index(kbid, vectorset_id=embedding_id)

    # TODO: Cancel any ongoing task for this embedding on this kb
    # TODO: Finally, spawn a task to clean up the storage
    return


async def add_semantic_model_to_learning_config(kbid: str, semantic_model: str):
    config = await learning_proxy.get_configuration(kbid)
    if config is None:
        raise HTTPException(status_code=404, detail="Learning configuration not found")
    if semantic_model not in config.semantic_models:
        config.semantic_models.append(semantic_model)
        await learning_proxy.set_configuration(kbid, config.model_dump())


async def delete_semantic_model_from_learning_config(kbid: str, semantic_model: str):
    config = await learning_proxy.get_configuration(kbid)
    if config is None:
        raise HTTPException(status_code=404, detail="Learning configuration not found")
    if semantic_model in config.semantic_models:
        config.semantic_models.remove(semantic_model)
        await learning_proxy.set_configuration(kbid, config.model_dump())


async def add_vectorset_to_index(kbid: str, vectorset_id: str):
    async with datamanagers.with_transaction() as txn:
        kbobj = KnowledgeBox(txn, await get_storage(), kbid)
        vectorset_config = knowledgebox_pb2.VectorSetConfig(
            vectorset_id=vectorset_id,
            # TODO: Where do I get vector index config from? (e.g. dimension, similarity, etc.)
        )
        await kbobj.create_vectorset(vectorset_config)
        await txn.commit()


@backoff.on_exception(backoff.expo, NodeError, jitter=backoff.random_jitter, max_tries=3)
async def delete_vectorset_from_index(kbid: str, vectorset_id: str):
    try:
        async with datamanagers.with_transaction() as txn:
            kbobj = KnowledgeBox(txn, await get_storage(), kbid)
            await kbobj.delete_vectorset(vectorset_id)
            await txn.commit()
    except Exception as ex:
        errors.capture_exception(ex)
        logger.exception("Could not delete vectorset from index", extra={"kbid": kbid})
