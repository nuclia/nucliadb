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

from nucliadb_models.resource import NucliaDBRoles


async def test_api(reader_api, knowledgebox_ingest):
    kbid = knowledgebox_ingest
    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(f"/kb/{kbid}/export/foo")
        assert resp.status_code < 500

        resp = await client.get(f"/kb/{kbid}/export/foo/status")
        assert resp.status_code < 500

        resp = await client.get(f"/kb/{kbid}/import/foo/status")
        assert resp.status_code < 500

        # Check that for non-existing kbs, endpoints return a 404
        for endpoint in (
            "/kb/idonotexist/export/foo",
            "/kb/idonotexist/export/foo/status",
            "/kb/idonotexist/import/foo/status",
        ):
            resp = await client.get(endpoint)
            assert resp.status_code == 404
            assert resp.json()["detail"] == "Knowledge Box not found"
