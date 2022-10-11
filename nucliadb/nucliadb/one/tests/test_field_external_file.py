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

from nucliadb.models.resource import NucliaDBRoles
from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RESOURCES_PREFIX
from nucliadb_utils.settings import nuclia_settings

EXTERNAL_FILE_URL = "http://mysite.com/files/myfile.pdf"

TEST_EXTERNAL_FILE_FIELD_PAYLOAD = {
    "file": {
        "uri": EXTERNAL_FILE_URL,
        "extra_headers": {"Authorization": "Bearer foo1234"},
    }
}


@pytest.fixture(scope="function")
def nuclia_jwt_key():
    nuclia_settings.nuclia_jwt_key = "foobarkey"
    yield


@pytest.mark.asyncio
async def test_external_file_field(nuclia_jwt_key, nucliadb_api, knowledgebox_one):
    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        # Create a resource
        kb_path = f"/{KB_PREFIX}/{knowledgebox_one}"
        resp = await client.post(
            f"{kb_path}/{RESOURCES_PREFIX}",
            headers={"X-SYNCHRONOUS": "True"},
            json={
                "slug": "resource1",
                "title": "Resource 1",
                "files": {"field1": TEST_EXTERNAL_FILE_FIELD_PAYLOAD},
            },
        )
        assert resp.status_code == 201
        resource = resp.json().get("uuid")

        # Create the external file field to the resource
        resp = await client.put(
            f"{kb_path}/{RESOURCE_PREFIX}/{resource}/file/field2",
            headers={"X-SYNCHRONOUS": "True"},
            json=TEST_EXTERNAL_FILE_FIELD_PAYLOAD,
        )
        assert resp.status_code == 201

    # Check that the uri was saved and that the source type is set to external
    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(f"{kb_path}/{RESOURCE_PREFIX}/{resource}?show=values")
        assert resp.status_code == 200
        data = resp.json()["data"]

        for field in ("field1", "field2"):
            assert data["files"][field]["value"]["file"]["uri"] == EXTERNAL_FILE_URL
            assert "extra_headers" not in data["files"][field]["value"]["file"]
            assert data["files"][field]["value"]["external"] is True

    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        # Try the patch
        resp = await client.patch(
            f"{kb_path}/{RESOURCE_PREFIX}/{resource}",
            headers={"X-SYNCHRONOUS": "True"},
            json={"files": {"field3": TEST_EXTERNAL_FILE_FIELD_PAYLOAD}},
        )
        assert resp.status_code == 200

    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(f"{kb_path}/{RESOURCE_PREFIX}/{resource}?show=values")
        assert resp.status_code == 200
        data = resp.json()["data"]

        assert data["files"]["field3"]["value"]["file"]["uri"] == EXTERNAL_FILE_URL
        assert "extra_headers" not in data["files"]["field3"]["value"]["file"]
        assert data["files"]["field3"]["value"]["external"] is True
