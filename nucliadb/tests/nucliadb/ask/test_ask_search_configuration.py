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

from unittest.mock import patch

import pytest
from fastapi import HTTPException
from httpx import AsyncClient

from nucliadb_models.search import FullResourceStrategy


@pytest.mark.deploy_modes("standalone")
async def test_search_configuration_ask(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/search_configurations/find_config",
        json={"kind": "find", "config": {"top_k": 1, "features": ["semantic"]}},
    )
    assert resp.status_code == 201

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/search_configurations/ask_config",
        json={
            "kind": "ask",
            "config": {"top_k": 1, "rag_strategies": [{"name": "full_resource", "count": 2}]},
        },
    )
    assert resp.status_code == 201

    async def run_ask(params):
        with patch("nucliadb.search.api.v1.ask.ask") as mock:
            mock.side_effect = HTTPException(status_code=500)
            await nucliadb_reader.post(
                f"/kb/{kbid}/ask",
                json={**params, "query": "whatever"},
            )
            mock.assert_called_once()
            return mock.call_args[1]["ask_request"]

    # Default ask request (sanity check)
    request = await run_ask({})
    assert request.top_k == 20

    # Using search configuration
    request = await run_ask({"search_configuration": "ask_config"})
    assert request.top_k == 1
    assert request.rag_strategies == [
        FullResourceStrategy(
            name="full_resource", count=2, include_remaining_text_blocks=False, apply_to=None
        )
    ]

    # Using search configuration and overrides
    request = await run_ask({"search_configuration": "ask_config", "top_k": 12})
    assert request.top_k == 12
    assert request.rag_strategies == [
        FullResourceStrategy(
            name="full_resource", count=2, include_remaining_text_blocks=False, apply_to=None
        )
    ]

    request = await run_ask({"search_configuration": "ask_config", "rag_strategies": []})
    assert request.top_k == 1
    assert request.rag_strategies == []

    # Using invalid search configuration
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/ask",
        json={"query": "whatever", "search_configuration": "invalid"},
    )
    assert resp.status_code == 400

    # Using find search configuration
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/ask",
        json={"query": "whatever", "search_configuration": "find_config"},
    )
    assert resp.status_code == 400
