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
import pytest

from nucliadb_sdk.knowledgebox import KnowledgeBox


def test_find_resource(docs_fixture: KnowledgeBox):
    resources = docs_fixture.find(text="love")
    assert resources.total == 10
    assert len([x for x in resources]) == 10


@pytest.mark.asyncio
async def test_find_async_resource(docs_fixture: KnowledgeBox):
    resources = await docs_fixture.async_find(text="love")
    assert resources.total == 10
    assert len([x for x in resources]) == 10
