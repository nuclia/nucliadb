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

from nucliadb.learning_proxy import LearningConfiguration, SemanticConfig, SimilarityFunction
from nucliadb.writer.api.v1.vectorsets import get_vectorset_config
from nucliadb_protos import knowledgebox_pb2


def test_get_vectorset_config():
    lconfig = LearningConfiguration(
        semantic_model="multilingual",
        semantic_vector_size=1024,
        semantic_threshold=0.5,
        semantic_vector_similarity=SimilarityFunction.COSINE,
        semantic_models=["multilingual", "en-2024-04-24"],
        semantic_model_configs={
            "multilingual": SemanticConfig(
                similarity=SimilarityFunction.COSINE,
                size=1024,
                threshold=0.5,
                matryoshka_dims=[1024, 512, 256],
            ),
            "en-2024-04-24": SemanticConfig(
                similarity=SimilarityFunction.DOT,
                size=768,
                threshold=0.7,
            ),
        },
    )
    vs_config = get_vectorset_config(lconfig, "multilingual")
    assert vs_config.vectorset_id == "multilingual"
    assert vs_config.vectorset_index_config.vector_dimension == 1024
    assert vs_config.vectorset_index_config.vector_type == knowledgebox_pb2.VectorType.DENSE_F32
    assert vs_config.vectorset_index_config.similarity == knowledgebox_pb2.VectorSimilarity.COSINE
    assert vs_config.matryoshka_dimensions == [1024, 512, 256]
    assert vs_config.vectorset_index_config.normalize_vectors is True

    vs_config = get_vectorset_config(lconfig, "en-2024-04-24")
    assert vs_config.vectorset_id == "en-2024-04-24"
    assert vs_config.vectorset_index_config.vector_dimension == 768
    assert vs_config.vectorset_index_config.vector_type == knowledgebox_pb2.VectorType.DENSE_F32
    assert vs_config.vectorset_index_config.similarity == knowledgebox_pb2.VectorSimilarity.DOT
    assert vs_config.matryoshka_dimensions == []
    assert vs_config.vectorset_index_config.normalize_vectors is False
