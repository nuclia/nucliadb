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

import pytest
from httpx import AsyncClient

from nucliadb.ingest.orm.resource import Resource
from nucliadb.reader.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RSLUG_PREFIX

BASE = ("field_id", "field_type")
VALUE = ("value",)
EXTRACTED = ("extracted",)


@pytest.mark.deploy_modes("component")
async def test_get_resource_field_default_options(
    nucliadb_reader: AsyncClient, test_resource: Resource
) -> None:
    rsc = test_resource
    kbid = rsc.kbid
    rid = rsc.uuid
    field_type = "text"
    field_id = "text1"

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/{field_type}/{field_id}",
    )
    assert resp.status_code == 200

    data = resp.json()

    # DEBUG
    # import json  # noqa
    # print(json.dumps(data, indent=4))

    expected_root_fields = set(BASE + VALUE)
    assert set(data.keys()) == expected_root_fields


@pytest.mark.deploy_modes("component")
async def test_get_resource_field_all(nucliadb_reader: AsyncClient, test_resource: Resource) -> None:
    rsc = test_resource
    kbid = rsc.kbid
    rid = rsc.uuid
    field_type = "text"
    field_id = "text1"

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/{field_type}/{field_id}",
        params={
            "show": ["value", "extracted"],
            "field_type": [
                "text",
                "link",
                "file",
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

    data = resp.json()

    # DEBUG
    # import json  # noqa
    # print(json.dumps(data, indent=4))

    expected_root_fields = set(BASE + VALUE + EXTRACTED)
    assert set(data.keys()) == expected_root_fields
    assert "body" in data["value"]
    assert set(data["extracted"].keys()) == {
        "metadata",
        "vectors",
        "large_metadata",
        "text",
    }


@pytest.mark.deploy_modes("component")
async def test_get_resource_field_filter_root_fields(nucliadb_reader: AsyncClient, test_resource):
    rsc = test_resource
    kbid = rsc.kbid
    rid = rsc.uuid
    field_type = "text"
    field_id = "text1"

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/{field_type}/{field_id}",
        params={"show": ["value"]},
    )

    assert resp.status_code == 200

    data = resp.json()

    expected_root_fields = set(BASE + VALUE)
    assert set(data.keys()) == expected_root_fields


@pytest.mark.deploy_modes("component")
async def test_get_resource_field_filter_extracted(nucliadb_reader: AsyncClient, test_resource):
    rsc = test_resource
    kbid = rsc.kbid
    rid = rsc.uuid
    field_type = "text"
    field_id = "text1"

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/{field_type}/{field_id}",
        params={
            "show": ["extracted"],
            "extracted": ["metadata", "vectors"],
        },
    )

    assert resp.status_code == 200

    data = resp.json()

    expected_root_fields = set(BASE + EXTRACTED)
    assert set(data.keys()) == expected_root_fields

    assert set(data["extracted"].keys()) == {"metadata", "vectors"}


@pytest.mark.deploy_modes("component")
async def test_get_resource_field_conversation(
    nucliadb_reader: AsyncClient, test_resource: Resource
) -> None:
    rsc = test_resource
    kbid = rsc.kbid
    rid = rsc.uuid
    field_type = "conversation"
    field_id = "conv1"

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}/{field_type}/{field_id}?page=last",
    )
    assert resp.status_code == 200
    data = resp.json()

    # DEBUG
    # import json  # noqa
    # print(json.dumps(data, indent=4))

    expected_root_fields = set(BASE + VALUE)
    assert set(data.keys()) == expected_root_fields
    assert "messages" in data["value"]


@pytest.mark.deploy_modes("component")
async def test_resource_endpoints_by_slug(nucliadb_reader: AsyncClient, test_resource):
    rsc = test_resource
    kbid = rsc.kbid
    rslug = rsc.basic.slug

    non_existent_slug = "foobar"

    # Regular GET

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RSLUG_PREFIX}/{rslug}",
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RSLUG_PREFIX}/{non_existent_slug}",
    )
    assert resp.status_code == 404

    # Field endpoint

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RSLUG_PREFIX}/{rslug}/text/text1",
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{kbid}/{RSLUG_PREFIX}/{non_existent_slug}/text/text1",
    )
    assert resp.status_code == 404
