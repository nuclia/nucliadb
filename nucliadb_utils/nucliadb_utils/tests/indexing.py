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

from nucliadb_utils.indexing import IndexingUtility
from nucliadb_utils.settings import indexing_settings
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility


@pytest.fixture(scope="function")
async def indexing_utility_registered():  # pragma: no cover
    indexing_util = IndexingUtility(
        nats_creds=indexing_settings.index_jetstream_auth,
        nats_servers=indexing_settings.index_jetstream_servers,
    )
    await indexing_util.initialize()
    set_utility(Utility.INDEXING, indexing_util)
    yield
    await indexing_util.finalize()
    if get_utility(Utility.INDEXING):
        clean_utility(Utility.INDEXING)
