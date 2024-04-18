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

from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb.writer.tests.test_fields import TEST_TEXT_PAYLOAD


@pytest.mark.asyncio
async def test_delete_field(
    knowledgebox_one,
    nucliadb_reader,
    nucliadb_writer,
) -> None:
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox_one}/resources",
        json={
            "slug": "resource1",
            "title": "Resource 1",
            "texts": {"text1": TEST_TEXT_PAYLOAD, "text2": TEST_TEXT_PAYLOAD},
        },
    )
    assert resp.status_code == 201
    uuid = resp.json()["uuid"]

    resp1 = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{knowledgebox_one}/resource/{uuid}?show=values",
    )
    assert resp1.status_code == 200

    assert "text1" in resp1.json()["data"]["texts"]

    resp = await nucliadb_writer.delete(
        f"/{KB_PREFIX}/{knowledgebox_one}/resource/{uuid}/text/text1",
    )
    assert resp.status_code == 204

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{knowledgebox_one}/resource/{uuid}?show=values",
    )

    assert "text1" not in resp.json()["data"]["texts"]
