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

from nucliadb import learning_proxy
from nucliadb.common import datamanagers
from nucliadb.ingest.orm.exceptions import VectorSetConflict
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.writer import logger
from nucliadb_protos import knowledgebox_pb2
from nucliadb_telemetry import errors
from nucliadb_utils.utilities import get_storage


async def add(kbid: str, vectorset_id: str) -> None:
    # First off, add the vectorset to the learning configuration if it's not already there
    lconfig = await learning_proxy.get_configuration(kbid)
    assert lconfig is not None
    semantic_models = lconfig.model_dump()["semantic_models"]
    if vectorset_id not in semantic_models:
        semantic_models.append(vectorset_id)
        await learning_proxy.update_configuration(kbid, {"semantic_models": semantic_models})
        lconfig = await learning_proxy.get_configuration(kbid)
        assert lconfig is not None

    # Then, add the vectorset to the index if it's not already there
    async with datamanagers.with_rw_transaction() as txn:
        kbobj = KnowledgeBox(txn, await get_storage(), kbid)
        vectorset_config = get_vectorset_config(lconfig, vectorset_id)
        try:
            await kbobj.create_vectorset(vectorset_config)
            await txn.commit()
        except VectorSetConflict:
            # Vectorset already exists, nothing to do
            return


async def delete(kbid: str, vectorset_id: str) -> None:
    lconfig = await learning_proxy.get_configuration(kbid)
    if lconfig is not None:
        semantic_models = lconfig.model_dump()["semantic_models"]
        if vectorset_id in semantic_models:
            semantic_models.remove(vectorset_id)
            await learning_proxy.update_configuration(kbid, {"semantic_models": semantic_models})
    try:
        async with datamanagers.with_rw_transaction() as txn:
            kbobj = KnowledgeBox(txn, await get_storage(), kbid)
            await kbobj.delete_vectorset(vectorset_id=vectorset_id)
            await txn.commit()
    except Exception as ex:
        errors.capture_exception(ex)
        logger.exception(
            "Could not delete vectorset from index", extra={"kbid": kbid, "vectorset_id": vectorset_id}
        )


def get_vectorset_config(
    learning_config: learning_proxy.LearningConfiguration, vectorset_id: str
) -> knowledgebox_pb2.VectorSetConfig:
    """
    Create a VectorSetConfig from a LearningConfiguration for a given vectorset_id
    """
    vectorset_config = knowledgebox_pb2.VectorSetConfig(vectorset_id=vectorset_id)
    vectorset_index_config = knowledgebox_pb2.VectorIndexConfig(
        vector_type=knowledgebox_pb2.VectorType.DENSE_F32,
    )
    model_config = learning_config.semantic_model_configs[vectorset_id]

    # Parse similarity function
    parsed_similarity = learning_proxy.SimilarityFunction(model_config.similarity)
    if parsed_similarity == learning_proxy.SimilarityFunction.COSINE.value:
        vectorset_index_config.similarity = knowledgebox_pb2.VectorSimilarity.COSINE
    elif parsed_similarity == learning_proxy.SimilarityFunction.DOT.value:
        vectorset_index_config.similarity = knowledgebox_pb2.VectorSimilarity.DOT
    else:
        raise ValueError(
            f"Unknown similarity function {model_config.similarity}, parsed as {parsed_similarity}"
        )

    # Parse vector dimension
    vectorset_index_config.vector_dimension = model_config.size

    # Parse matryoshka dimensions
    if len(model_config.matryoshka_dims) > 0:
        vectorset_index_config.normalize_vectors = True
        vectorset_config.matryoshka_dimensions.extend(model_config.matryoshka_dims)
    else:
        vectorset_index_config.normalize_vectors = False
    vectorset_config.vectorset_index_config.CopyFrom(vectorset_index_config)
    return vectorset_config
