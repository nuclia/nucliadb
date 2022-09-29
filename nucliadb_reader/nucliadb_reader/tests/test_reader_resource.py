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

from nucliadb_ingest.orm.resource import Resource
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_reader.api.v1.router import KB_PREFIX

ID = ("id",)
BASIC = (
    "title",
    "summary",
    "icon",
    "layout",
    "thumbnail",
    "metadata",
    "usermetadata",
    "created",
    "modified",
    "fieldmetadata",
    "last_seqid",
)
RELATIONS = ("relations",)
ORIGIN = ("origin",)
DATA = ("data",)
VALUES = ("values",)
EXTRACTED = ("extracted",)


@pytest.mark.asyncio
async def test_get_resource_inexistent(
    reader_api: Callable[..., AsyncClient], knowledgebox: str
) -> None:
    kbid = knowledgebox
    rid = "000000000000001"

    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/resource/{rid}",
        )
        assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_resource_default_options(
    reader_api: Callable[..., AsyncClient], test_resource: Resource
) -> None:
    rsc = test_resource
    kbid = rsc.kb.kbid
    rid = rsc.uuid

    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/resource/{rid}",
        )
        assert resp.status_code == 200

        resource = resp.json()

        # DEBUG
        # import json  # noqa
        # print(json.dumps(data, indent=4))

        expected_root_fields = set(ID + BASIC)
        assert set(resource.keys()) == expected_root_fields

        assert "data" not in resource


@pytest.mark.asyncio
async def test_get_resource_all(
    reader_api: Callable[..., AsyncClient], test_resource: Resource
) -> None:
    rsc = test_resource
    kbid = rsc.kb.kbid
    rid = rsc.uuid

    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/resource/{rid}",
            params={
                "show": ["basic", "origin", "relations", "values", "extracted"],
                "field_type": [
                    "text",
                    "link",
                    "file",
                    "layout",
                    "keywordset",
                    "datetime",
                    "conversation",
                ],
                "extracted": [
                    "metadata",
                    "vectors",
                    "large_metadata",
                    "text",
                    "link",
                    "file",
                ],
            },
        )
        assert resp.status_code == 200

        resource = resp.json()

        # DEBUG
        # import json  # noqa
        # print(json.dumps(data, indent=4))

        expected_root_fields = set(ID + BASIC + RELATIONS + ORIGIN + DATA)
        assert set(resource.keys()) == expected_root_fields

        data = resource["data"]
        assert set(data.keys()) == {
            "files",
            "texts",
            "links",
            "layouts",
            "keywordsets",
            "datetimes",
            "conversations",
        }
        texts = data["texts"]
        assert set(texts.keys()) == {"text1"}
        assert set(texts["text1"]["extracted"].keys()) == {
            "metadata",
            "vectors",
            "large_metadata",
            "text",
        }
        layouts = data["layouts"]
        assert set(layouts.keys()) == {"layout1"}
        assert set(layouts["layout1"]["extracted"].keys()) == {
            "metadata",
            "vectors",
            "large_metadata",
            "text",
        }

        links = data["links"]
        assert set(links.keys()) == {"link1"}
        assert set(links["link1"]["extracted"].keys()) == {
            "metadata",
            "vectors",
            "large_metadata",
            "text",
            "link",
        }


@pytest.mark.asyncio
async def test_get_resource_filter_root_fields(reader_api, test_resource):
    rsc = test_resource
    kbid = rsc.kb.kbid
    rid = rsc.uuid

    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/resource/{rid}",
            params={"show": ["basic", "values"]},
        )
        assert resp.status_code == 200

        resource = resp.json()

        expected_root_fields = set(ID + BASIC + DATA)
        assert set(resource.keys()) == expected_root_fields

        data = resource["data"]

        assert set(data.keys()) == {
            "files",
            "texts",
            "links",
            "layouts",
            "keywordsets",
            "datetimes",
            "conversations",
        }

        assert set(data["files"]["file1"].keys()) == {"value"}
        assert set(data["texts"]["text1"].keys()) == {"value"}
        assert set(data["links"]["link1"].keys()) == {"value"}
        assert set(data["layouts"]["layout1"].keys()) == {"value"}
        assert set(data["keywordsets"]["keywordset1"].keys()) == {"value"}
        assert set(data["datetimes"]["datetime1"].keys()) == {"value"}
        assert set(data["conversations"]["conv1"].keys()) == {"value"}


@pytest.mark.asyncio
async def test_get_resource_filter_field_types(reader_api, test_resource):
    rsc = test_resource
    kbid = rsc.kb.kbid
    rid = rsc.uuid

    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/resource/{rid}",
            params={"show": ["values", "extracted"], "field_type": ["text", "link"]},
        )
        assert resp.status_code == 200

        resource = resp.json()

        expected_root_fields = set(ID + DATA)
        assert set(resource.keys()) == expected_root_fields

        data = resource["data"]

        assert set(data.keys()) == {"texts", "links"}
        assert set(data["texts"]["text1"].keys()) == {"value", "extracted"}
        assert set(data["links"]["link1"].keys()) == {"value", "extracted"}


@pytest.mark.asyncio
async def test_get_resource_filter_field_types_and_extracted(reader_api, test_resource):
    rsc = test_resource
    kbid = rsc.kb.kbid
    rid = rsc.uuid

    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/resource/{rid}",
            params={
                "show": ["extracted"],
                "field_type": ["text"],
                "extracted": ["metadata", "vectors"],
            },
        )

        assert resp.status_code == 200
        resource = resp.json()

        expected_root_fields = set(ID + DATA)
        assert set(resource.keys()) == expected_root_fields

        data = resource["data"]

        assert set(data.keys()) == {"texts"}
        assert set(data["texts"]["text1"].keys()) == {"extracted"}
        assert set(data["texts"]["text1"]["extracted"].keys()) == {
            "metadata",
            "vectors",
        }
