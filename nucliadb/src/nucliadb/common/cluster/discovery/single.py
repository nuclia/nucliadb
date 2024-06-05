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
import logging

from nucliadb.common.cluster.discovery.base import (
    AbstractClusterDiscovery,
    update_members,
)
from nucliadb.common.cluster.discovery.types import IndexNodeMetadata
from nucliadb.common.cluster.standalone.utils import get_self

logger = logging.getLogger(__name__)


class SingleNodeDiscovery(AbstractClusterDiscovery):
    """
    When there is no cluster and ndb is running as a single node.
    """

    async def initialize(self) -> None:
        self_node = get_self()
        update_members(
            [
                IndexNodeMetadata(
                    node_id=self_node.id,
                    name=self_node.id,
                    address=self_node.address,
                    shard_count=self_node.shard_count,
                    available_disk=self_node.available_disk,
                )
            ]
        )

    async def finalize(self) -> None: ...
