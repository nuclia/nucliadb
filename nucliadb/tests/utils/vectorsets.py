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
from copy import deepcopy
from unittest.mock import patch

from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.learning_proxy import (
    LearningConfiguration,
    SemanticConfig,
    SimilarityFunction,
)
from nucliadb_protos import utils_pb2


async def add_vectorset(
    nucliadb_writer: AsyncClient,
    kbid: str,
    vectorset_id: str,
    *,
    similarity: SimilarityFunction = SimilarityFunction.COSINE,
    vector_dimension: int = 768,
    threshold: float = 0.4,
    matryoshka_dims: list[int] | None = None,
):
    # This function is tightly coupled with the vectorset API implementation. It
    # mocks the interactions with learning config to get, update and get again
    # the configuration while adding a vectorset

    matryoshka_dims = matryoshka_dims or []

    learning_config = LearningConfiguration(
        semantic_model="DEPRECATED",
        semantic_vector_similarity="DEPRECATED",
        semantic_vector_size=-1,
    )
    async with datamanagers.with_ro_transaction() as txn:
        async for vid, vs in datamanagers.vectorsets.iter(txn, kbid=kbid):
            learning_config.semantic_models.append(vid)

            if vs.vectorset_index_config.similarity == utils_pb2.VectorSimilarity.COSINE:
                vs_similarity = SimilarityFunction.COSINE
            else:
                vs_similarity = SimilarityFunction.DOT

            learning_config.semantic_model_configs[vid] = SemanticConfig(
                similarity=vs_similarity,
                size=vs.vectorset_index_config.vector_dimension,
                threshold=0.5,  # XXX: we don't know it, so we set a value here
            )

    first_get_config = deepcopy(learning_config)

    learning_config.semantic_models.append(vectorset_id)
    learning_config.semantic_model_configs[vectorset_id] = SemanticConfig(
        similarity=similarity,
        size=vector_dimension,
        threshold=threshold,
        matryoshka_dims=matryoshka_dims,
    )

    updated_get_config = learning_config

    with (
        patch(
            "nucliadb.writer.api.v1.vectorsets.learning_proxy.get_configuration",
            side_effect=[
                first_get_config,
                updated_get_config,
            ],
        ),
        patch(
            "nucliadb.writer.api.v1.vectorsets.learning_proxy.update_configuration",
        ),
    ):
        resp = await nucliadb_writer.post(f"/kb/{kbid}/vectorsets/{vectorset_id}")
        return resp
