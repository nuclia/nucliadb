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
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient

from nucliadb.learning_proxy import LearningConfiguration, SemanticConfig, SimilarityFunction
from nucliadb.writer.api.v1.router import KB_PREFIX, KBS_PREFIX


@pytest.mark.deploy_modes("component")
async def test_knowledgebox_lifecycle(nucliadb_writer_manager: AsyncClient):
    resp = await nucliadb_writer_manager.post(
        f"/{KBS_PREFIX}",
        json={
            "slug": "kbid1",
            "title": "My Knowledge Box",
            "description": "My lovely knowledgebox",
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["slug"] == "kbid1"
    kbid = data["uuid"]

    resp = await nucliadb_writer_manager.patch(
        f"/{KB_PREFIX}/{kbid}",
        json={
            "slug": "kbid2",
            "description": "My lovely knowledgebox2",
        },
    )
    assert resp.status_code == 200


@pytest.mark.deploy_modes("component")
async def test_create_knowledgebox_with_learning_config(nucliadb_writer_manager: AsyncClient):
    with (
        patch("nucliadb.writer.api.v1.knowledgebox.KnowledgeBox", new=AsyncMock()) as kb,
        patch("nucliadb.writer.api.v1.knowledgebox.learning_proxy", new=AsyncMock()) as learning_proxy,
    ):
        kb.create.return_value = ("kbid", "slug")
        learning_config = LearningConfiguration(
            semantic_model="multilingual",
            semantic_threshold=-1,
            semantic_vector_size=10,
            semantic_vector_similarity="cosine",
            semantic_matryoshka_dims=[10, 20, 30],
            semantic_model_configs={
                "multilingual": SemanticConfig(
                    size=10,
                    threshold=-1,
                    similarity=SimilarityFunction.COSINE,
                    matryoshka_dims=[10, 20, 30],
                )
            },
        )
        learning_proxy.set_configuration.return_value = learning_config

        resp = await nucliadb_writer_manager.post(
            f"/{KBS_PREFIX}",
            json={
                "slug": "slug",
                "learning_config": {
                    "semantic_model": "multilingual",
                    "semantic_threshold": -1,
                    "semantic_vector_size": 10,
                    "semantic_vector_similarity": "cosine",
                },
            },
        )
        assert resp.status_code == 201
        assert kb.create.call_count == 1
        assert kb.new_unique_kbid.call_count == 1
        assert kb.create.call_args.kwargs["slug"] == "slug"
        assert (
            kb.create.call_args.kwargs["semantic_models"]["multilingual"]
            == learning_config.into_semantic_model_metadata()
        )


@pytest.mark.deploy_modes("component")
async def test_create_knowledgebox_with_learning_config_with_matryoshka_dimensions(
    nucliadb_writer_manager: AsyncClient,
):
    with (
        patch("nucliadb.writer.api.v1.knowledgebox.KnowledgeBox", new=AsyncMock()) as kb,
        patch("nucliadb.writer.api.v1.knowledgebox.learning_proxy", new=AsyncMock()) as learning_proxy,
    ):
        kb.create.return_value = ("kbid", "slug")
        learning_config = LearningConfiguration(
            semantic_model="multilingual",
            semantic_threshold=-1,
            semantic_vector_size=10,
            semantic_vector_similarity="cosine",
            semantic_matryoshka_dims=[10, 20, 30],
            semantic_model_configs={
                "multilingual": SemanticConfig(
                    size=10,
                    threshold=-1,
                    similarity=SimilarityFunction.COSINE,
                    matryoshka_dims=[10, 20, 30],
                )
            },
        )
        learning_proxy.set_configuration.return_value = learning_config

        resp = await nucliadb_writer_manager.post(
            f"/{KBS_PREFIX}",
            json={
                "slug": "slug",
                "learning_config": {
                    "semantic_model": "multilingual",
                    "semantic_threshold": -1,
                    "semantic_vector_size": 10,
                    "semantic_vector_similarity": "cosine",
                    "semantic_matryoshka_dims": [10, 20, 30],
                },
            },
        )
        assert resp.status_code == 201
        assert kb.create.call_count == 1
        assert kb.new_unique_kbid.call_count == 1
        assert kb.create.call_args.kwargs["slug"] == "slug"
        assert (
            kb.create.call_args.kwargs["semantic_models"]["multilingual"]
            == learning_config.into_semantic_model_metadata()
        )
