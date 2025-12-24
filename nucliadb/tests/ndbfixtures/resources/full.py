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
from collections.abc import AsyncIterator

import pytest

from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.resource import Resource
from nucliadb_utils.storages.storage import Storage


@pytest.fixture(scope="function")
async def full_resource(
    storage: Storage,
    maindb_driver: Driver,
    knowledgebox: str,
) -> AsyncIterator[Resource]:
    """Create a resource in `knowledgebox` that has every possible bit of information.

    DISCLAIMER: this comes from a really old fixture, so probably it does
    **not** contains every possible bit of information.

    """
    from tests.ndbfixtures.ingest import create_resource

    resource = await create_resource(
        storage=storage,
        driver=maindb_driver,
        knowledgebox=knowledgebox,
    )
    yield resource
    resource.clean()
