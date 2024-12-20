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
import uuid
from unittest.mock import patch

import pytest

from nucliadb.common.cluster import manager
from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.nidx import NIDX_ENABLED, NidxUtility
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    set_utility,
)


@pytest.fixture(scope="function")
async def dummy_index_node_cluster(
    # TODO: change this to more explicit dummy indexing utility
    indexing_utility,
):
    with (
        patch.dict(manager.INDEX_NODES, clear=True),
        patch.object(cluster_settings, "standalone_mode", False),
    ):
        manager.add_index_node(
            id=str(uuid.uuid4()),
            address="nohost",
            shard_count=0,
            available_disk=100,
            dummy=True,
        )
        manager.add_index_node(
            id=str(uuid.uuid4()),
            address="nohost",
            shard_count=0,
            available_disk=100,
            dummy=True,
        )
        yield


@pytest.fixture(scope="function")
async def dummy_nidx_utility():
    class DummyNidxUtility(NidxUtility):
        async def initialize(self):
            pass

        async def finalize(self):
            pass

        async def index(self, msg):
            pass

    if NIDX_ENABLED:
        set_utility(Utility.NIDX, DummyNidxUtility())

    yield

    clean_utility(Utility.NIDX)
