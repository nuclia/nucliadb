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

"""Migration #25 (Fixed migration 24)

Vectorsets are coming and we need to be ready at nucliadb. Vector index config
shouldn't be stored anymore in the `Shards` protobuffer, we need to migrate to
the new vectorsets config.

This migration asks learning_config for each KB configuration and saves the
model name as the vectorset_id. Creates a vectorset configuration for each model
and deprecates the vectors index config from the `Shards` protobuffer.

This migration should work for onprem and hosted deployments, as
learning_proxy handles which API is used (internal or external)

"""

import logging

from nucliadb import learning_proxy
from nucliadb.common import datamanagers
from nucliadb.migrator.context import ExecutionContext
from nucliadb_protos import knowledgebox_pb2

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    async with context.kv_driver.transaction(read_only=True) as txn:
        vectorsets_count = len([vs async for vs in datamanagers.vectorsets.iter(txn, kbid=kbid)])
    if vectorsets_count > 0:
        logger.info("Skipping KB with vectorsets already populated", extra={"kbid": kbid})
        return

    learning_config = await learning_proxy.get_configuration(kbid)
    if learning_config is None:
        logger.warning(f"KB has no learning config", extra={"kbid": kbid})
        return None

    vectorset_id = learning_config.semantic_model
    learning_model_metadata = learning_config.into_semantic_model_metadata()
    learning_similarity = learning_model_metadata.similarity_function
    learning_vector_dimension = learning_model_metadata.vector_dimension
    learning_matryoshka_dimensions = learning_model_metadata.matryoshka_dimensions
    learning_normalize_vectors = len(learning_matryoshka_dimensions) > 0

    async with context.kv_driver.transaction(read_only=True) as txn:
        semantic_model = await datamanagers.kb.get_model_metadata(txn, kbid=kbid)

        maindb_similarity = semantic_model.similarity_function

        maindb_vector_dimension = None
        if semantic_model.vector_dimension:
            maindb_vector_dimension = semantic_model.vector_dimension

        maindb_matryoshka_dimensions: list[int] = []
        if len(semantic_model.matryoshka_dimensions) > 0:
            maindb_matryoshka_dimensions.extend(semantic_model.matryoshka_dimensions)

        maindb_normalize_vectors = len(maindb_matryoshka_dimensions) > 0

    if (
        maindb_similarity != learning_similarity
        or (maindb_vector_dimension is not None and maindb_vector_dimension != learning_vector_dimension)
        or set(maindb_matryoshka_dimensions) != set(learning_matryoshka_dimensions)
        or maindb_normalize_vectors != learning_normalize_vectors
    ):
        logger.error(
            "KB has mismatched data between nucliadb and learning_config! Please, review manually",
            extra={"kbid": kbid},
        )
        return None

    default_vectorset = knowledgebox_pb2.VectorSetConfig(
        vectorset_id=vectorset_id,
        vectorset_index_config=knowledgebox_pb2.VectorIndexConfig(
            vector_dimension=maindb_vector_dimension,
            similarity=maindb_similarity,
            vector_type=knowledgebox_pb2.VectorType.DENSE_F32,  # we only support this for now
            normalize_vectors=maindb_normalize_vectors,
        ),
        matryoshka_dimensions=maindb_matryoshka_dimensions,
    )

    async with context.kv_driver.transaction() as txn:
        # Populate KB vectorsets with data from learning. We are skipping KBs
        # with this key already set, so we can set here safely
        await datamanagers.vectorsets.set(txn, kbid=kbid, config=default_vectorset)
        await txn.commit()
