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

DUMMY_EXPORT = b"ENT\x00\x00\x00\x00LAB\x00\x00\x00\x00"


async def test_import_kb(writer_api, knowledgebox_ingest):
    kbid = knowledgebox_ingest
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(f"/kb/{kbid}/import", content=DUMMY_EXPORT)
        assert resp.status_code == 200
