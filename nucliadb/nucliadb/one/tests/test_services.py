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
from typing import Callable

import pytest
from httpx import AsyncClient

from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb_models.resource import NucliaDBRoles


@pytest.mark.asyncio
async def test_widget_service(
    nucliadb_api: Callable[..., AsyncClient], knowledgebox_one
) -> None:
    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        widget = {
            "id": "widget1",
            "description": "description",
            "mode": "button",
            "features": {
                "useFilters": True,
                "suggestEntities": True,
                "suggestSentences": True,
                "suggestParagraphs": True,
                "suggestLabels": True,
                "editLabels": True,
                "entityAnnotation": True,
            },
            "filters": ["filter1"],
            "topEntities": ["entity1"],
            "style": {"top-border": "0px"},
        }
        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_one}/widget/widget1", json=widget
        )
        assert resp.status_code == 200

    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(f"/{KB_PREFIX}/{knowledgebox_one}/widgets")
        # default 'dashboard' widget and the one created here
        assert len(resp.json()["widgets"]) == 2

        resp = await client.get(f"/{KB_PREFIX}/{knowledgebox_one}/widget/widget1")
        assert resp.status_code == 200
        assert resp.json() == widget

    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.delete(f"/{KB_PREFIX}/{knowledgebox_one}/widget/widget1")
        assert resp.status_code == 200

    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(f"/{KB_PREFIX}/{knowledgebox_one}/widgets")
        assert len(resp.json()["widgets"]) == 1


@pytest.mark.asyncio
async def test_disable_vectors_service(
    nucliadb_api: Callable[..., AsyncClient], knowledgebox_one
) -> None:
    async with nucliadb_api(roles=[NucliaDBRoles.MANAGER]) as client:
        data = {"disable_vectors": True}
        resp = await client.patch(f"/{KB_PREFIX}/{knowledgebox_one}", json=data)
        assert resp.status_code == 200

    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(f"/{KB_PREFIX}/{knowledgebox_one}")
        assert resp.json()["config"]["disable_vectors"]


@pytest.mark.asyncio
async def test_entities_service(
    nucliadb_api: Callable[..., AsyncClient], knowledgebox_one
) -> None:
    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        entitygroup = {
            "title": "Kitchen",
            "custom": True,
            "entities": {
                "cupboard": {"value": "Cupboard"},
                "fork": {"value": "Fork"},
                "fridge": {"value": "Fridge"},
                "knife": {"value": "Knife"},
                "sink": {"value": "Sink"},
                "spoon": {"value": "Spoon"},
            },
        }
        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_one}/entitiesgroup/group1", json=entitygroup
        )
        assert resp.status_code == 200

    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(f"/{KB_PREFIX}/{knowledgebox_one}/entitiesgroups")
        groups = resp.json()["groups"]
        assert len(groups) == 1
        assert groups["group1"]["custom"] is True

        resp = await client.get(f"/{KB_PREFIX}/{knowledgebox_one}/entitiesgroup/group1")
        assert resp.status_code == 200
        assert resp.json()["custom"] is True

    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.delete(
            f"/{KB_PREFIX}/{knowledgebox_one}/entitiesgroup/group1"
        )
        assert resp.status_code == 200

    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(f"/{KB_PREFIX}/{knowledgebox_one}/entitiesgroups")
        assert len(resp.json()["groups"]) == 0


@pytest.mark.asyncio
async def test_labelsets_service(
    nucliadb_api: Callable[..., AsyncClient], knowledgebox_one
) -> None:
    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        widget = {
            "title": "labelset1",
            "labels": [{"title": "Label 1", "related": "related 1", "text": "My Text"}],
        }
        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_one}/labelset/labelset1", json=widget
        )
        assert resp.status_code == 200

    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(f"/{KB_PREFIX}/{knowledgebox_one}/labelsets")
        assert len(resp.json()["labelsets"]) == 1

        resp = await client.get(f"/{KB_PREFIX}/{knowledgebox_one}/labelset/labelset1")
        assert resp.status_code == 200

    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.delete(
            f"/{KB_PREFIX}/{knowledgebox_one}/labelset/labelset1"
        )
        assert resp.status_code == 200

    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(f"/{KB_PREFIX}/{knowledgebox_one}/labelsets")
        assert len(resp.json()["labelsets"]) == 0
