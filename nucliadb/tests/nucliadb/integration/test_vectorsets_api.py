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
from unittest.mock import patch

from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.learning_proxy import (
    LearningConfiguration,
    ProxiedLearningConfigError,
    SemanticConfig,
    SimilarityFunction,
)
from nucliadb_protos import utils_pb2
from nucliadb_protos.nodewriter_pb2 import VectorType

MODULE = "nucliadb.writer.vectorsets"


async def test_add_delete_vectorsets(
    nucliadb_manager: AsyncClient,
    knowledgebox,
):
    kbid = knowledgebox
    vectorset_id = "en-2024-04-24"
    existing_lconfig = LearningConfiguration(
        semantic_model="multilingual",
        semantic_threshold=0.5,
        semantic_vector_similarity=SimilarityFunction.COSINE.name,
        semantic_models=["multilingual"],
        semantic_model_configs={
            "multilingual": SemanticConfig(
                similarity=SimilarityFunction.COSINE,
                size=1024,
                threshold=0.5,
            )
        },
    )
    updated_lconfig = LearningConfiguration(
        semantic_model="multilingual",
        semantic_threshold=0.5,
        semantic_vector_similarity=SimilarityFunction.COSINE.name,
        semantic_models=["multilingual", vectorset_id],
        semantic_model_configs={
            "multilingual": SemanticConfig(
                similarity=SimilarityFunction.COSINE,
                size=1024,
                threshold=0.5,
            ),
            vectorset_id: SemanticConfig(
                similarity=SimilarityFunction.COSINE,
                size=1024,
                threshold=0.5,
                matryoshka_dims=[1024, 512, 256, 128],
            ),
        },
    )

    with patch(
        f"{MODULE}.learning_proxy.get_configuration",
        side_effect=[
            existing_lconfig,  # Initial configuration
            updated_lconfig,  # Configuration after adding the vectorset
            updated_lconfig,  # Configuration right before deleting the vectorset
        ],
    ) as get_configuration:
        with patch(f"{MODULE}.learning_proxy.update_configuration", return_value=None):
            # Add the vectorset
            resp = await nucliadb_manager.post(f"/kb/{kbid}/vectorsets/{vectorset_id}")
            assert resp.status_code == 200, resp.text

            # Check that the vectorset has been created with the correct configuration
            async with datamanagers.with_ro_transaction() as txn:
                vs = await datamanagers.vectorsets.get(txn, kbid=kbid, vectorset_id=vectorset_id)
                assert vs is not None
                assert vs.vectorset_id == vectorset_id
                assert vs.vectorset_index_config.vector_type == VectorType.DENSE_F32
                assert vs.vectorset_index_config.similarity == utils_pb2.VectorSimilarity.COSINE
                assert vs.vectorset_index_config.vector_dimension == 1024
                assert vs.matryoshka_dimensions == [1024, 512, 256, 128]

            # Mock the learning_proxy to return the updated configuration on get_configuration
            get_configuration.return_value = updated_lconfig

            # Delete the vectorset
            resp = await nucliadb_manager.delete(f"/kb/{kbid}/vectorsets/{vectorset_id}")
            assert resp.status_code == 200, resp.text

            # Check that the vectorset has been deleted
            async with datamanagers.with_ro_transaction() as txn:
                vs = await datamanagers.vectorsets.get(txn, kbid=kbid, vectorset_id=vectorset_id)
                assert vs is None


async def test_learning_config_errors_are_proxied_correctly(
    nucliadb_manager: AsyncClient,
    knowledgebox,
):
    kbid = knowledgebox
    with patch(
        f"{MODULE}.learning_proxy.get_configuration",
        side_effect=ProxiedLearningConfigError(
            status_code=500, content=b"Learning Internal Server Error", content_type="text/plain"
        ),
    ):
        resp = await nucliadb_manager.post(f"/kb/{kbid}/vectorsets/foo")
        assert resp.status_code == 500
        assert resp.text == "Learning Internal Server Error"

        resp = await nucliadb_manager.delete(f"/kb/{kbid}/vectorsets/foo")
        assert resp.status_code == 500
        assert resp.text == "Learning Internal Server Error"
