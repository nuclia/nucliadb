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

import datetime
from unittest.mock import patch

import pytest
from httpx import AsyncClient

from nucliadb_models.search import FullResourceStrategy


@pytest.mark.deploy_modes("standalone")
async def test_search_configuration_create(
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    # Create a config works
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/search_configurations/one_thing",
        json={"kind": "find", "config": {"top_k": 1}},
    )
    assert resp.status_code == 201

    # Duplicates not allowed
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/search_configurations/one_thing",
        json={"kind": "find", "config": {"top_k": 1}},
    )
    assert resp.status_code == 409


@pytest.mark.deploy_modes("standalone")
async def test_search_configuration_update(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    # Cannot update a config that doesn't exist
    resp = await nucliadb_writer.patch(
        f"/kb/{kbid}/search_configurations/one_thing",
        json={"kind": "find", "config": {"top_k": 1}},
    )
    assert resp.status_code == 404

    # Can update an existing config
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/search_configurations/one_thing",
        json={"kind": "find", "config": {"top_k": 1}},
    )
    assert resp.status_code == 201

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/search_configurations/one_thing",
    )
    assert resp.status_code == 200
    assert resp.json() == {"kind": "find", "config": {"top_k": 1}}

    resp = await nucliadb_writer.patch(
        f"/kb/{kbid}/search_configurations/one_thing",
        json={"kind": "find", "config": {"top_k": 22}},
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/search_configurations/one_thing",
    )
    assert resp.status_code == 200
    assert resp.json() == {"kind": "find", "config": {"top_k": 22}}


@pytest.mark.deploy_modes("standalone")
async def test_search_configuration_delete(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/search_configurations/one_thing",
        json={"kind": "find", "config": {"top_k": 1}},
    )
    assert resp.status_code == 201

    # Cannot delete non-existing config
    resp = await nucliadb_writer.delete(
        f"/kb/{kbid}/search_configurations/another_thing",
    )
    assert resp.status_code == 404

    # Deletes existing config
    resp = await nucliadb_writer.delete(
        f"/kb/{kbid}/search_configurations/one_thing",
    )
    assert resp.status_code == 204

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/search_configurations/one_thing",
    )
    assert resp.status_code == 404


@pytest.mark.deploy_modes("standalone")
async def test_search_configuration_list(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/search_configurations/one_thing",
        json={"kind": "find", "config": {"top_k": 1}},
    )
    assert resp.status_code == 201

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/search_configurations/ask_pretty",
        json={"kind": "ask", "config": {"prompt": "This is my prompt now"}},
    )
    assert resp.status_code == 201

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/search_configurations",
    )
    assert resp.status_code == 200

    assert resp.json() == {
        "ask_pretty": {"kind": "ask", "config": {"prompt": "This is my prompt now"}},
        "one_thing": {"kind": "find", "config": {"top_k": 1}},
    }


@pytest.mark.deploy_modes("standalone")
async def test_search_configuration_find(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/search_configurations/one_thing",
        json={"kind": "find", "config": {"top_k": 1, "features": ["semantic"]}},
    )
    assert resp.status_code == 201

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/search_configurations/ask_config",
        json={"kind": "ask", "config": {"top_k": 1, "features": ["semantic"]}},
    )
    assert resp.status_code == 201

    async def run_find(params):
        with patch("nucliadb.search.api.v1.find.find") as mock:
            await nucliadb_reader.post(
                f"/kb/{kbid}/find",
                json=params,
            )
            mock.assert_called_once()
            return mock.call_args[0][1]

    # Default find request (sanity check)
    request = await run_find({})
    assert request.top_k == 20

    # Using search configuration
    request = await run_find({"search_configuration": "one_thing"})
    assert request.top_k == 1
    assert request.features == ["semantic"]

    # Using search configuration and overrides
    request = await run_find({"search_configuration": "one_thing", "top_k": 12})
    assert request.top_k == 12
    assert request.features == ["semantic"]

    request = await run_find({"search_configuration": "one_thing", "features": ["keyword"]})
    assert request.top_k == 1
    assert request.features == ["keyword"]

    # Using invalid search configuration
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={"search_configuration": "invalid"},
    )
    assert resp.status_code == 400

    # Using ask search configuration
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={"search_configuration": "ask_config"},
    )
    assert resp.status_code == 400


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
            mock.side_effect = Exception()
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


@pytest.mark.deploy_modes("standalone")
async def test_search_configuration_merge(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/search_configurations/one_thing",
        json={"kind": "find", "config": {"top_k": 1, "features": ["semantic"]}},
    )
    assert resp.status_code == 201

    async def run_find(params):
        with patch("nucliadb.search.api.v1.find.find") as mock:
            await nucliadb_reader.post(
                f"/kb/{kbid}/find",
                json=params,
            )
            mock.assert_called_once()
            return mock.call_args[0][1]

    request = await run_find(
        {
            "search_configuration": "one_thing",
            "filter_expression": {"field": {"prop": "keyword", "word": "patata"}},
        }
    )
    assert request.top_k == 1
    assert request.filter_expression.field.model_dump() == {"prop": "keyword", "word": "patata"}

    request = await run_find(
        {
            "search_configuration": "one_thing",
            "filter_expression": {
                "field": {
                    "and": [
                        {"prop": "keyword", "word": "patata"},
                        {"prop": "created", "since": "2024-01-01"},
                    ]
                }
            },
        }
    )
    assert request.top_k == 1
    assert request.filter_expression.field.model_dump() == {
        "and": [
            {"prop": "keyword", "word": "patata"},
            {"prop": "created", "since": datetime.datetime(2024, 1, 1, 0, 0), "until": None},
        ]
    }
