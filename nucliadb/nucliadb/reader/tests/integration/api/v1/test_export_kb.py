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

from io import BytesIO

from nucliadb_models.resource import NucliaDBRoles


async def test_export_kb(reader_api, knowledgebox_ingest):
    kbid = knowledgebox_ingest
    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(f"/kb/{kbid}/export")
        assert resp.status_code == 200

        export = BytesIO()
        for chunk in resp.iter_bytes():
            export.write(chunk)
        export.seek(0)

        assert len(export.getvalue()) > 0
