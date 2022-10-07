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

from nucliadb_ingest.serialize import serialize


@pytest.mark.asyncio()
async def test_serialize(fake_node, test_resource, knowledgebox):
    kbid = knowledgebox
    rid = test_resource.uuid
    rslug = test_resource.basic.slug

    serialize_args = dict(show=[], field_type_filter=[], extracted=[])

    # Invalid rid should return None
    assert await serialize(kbid, "idonotexist", **serialize_args) is None

    # Invalid slug should return None
    assert await serialize(kbid, None, slug="idonotexist", **serialize_args) is None

    # Getting by rid should work
    resource = await serialize(kbid, rid, **serialize_args)
    assert resource.id == rid

    # Getting by slug should work too
    resource = await serialize(kbid, None, slug=rslug, **serialize_args)
    assert resource.id == rid
