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
import asyncio
import logging

from nucliadb.common.cluster.discovery.base import (
    AbstractClusterDiscovery,
    update_members,
)

logger = logging.getLogger(__name__)


class ManualDiscovery(AbstractClusterDiscovery):
    """
    Manual provide all cluster members addresses to load information from.
    """

    async def discover(self) -> None:
        members = []
        for address in self.settings.cluster_discovery_manual_addresses:
            members.append(await self._query_node_metadata(address))
        update_members(members)

    async def watch(self) -> None:
        while True:
            try:
                await self.discover()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception(
                    "Error while watching cluster members. Will retry at started interval"
                )
            finally:
                await asyncio.sleep(15)

    async def initialize(self) -> None:
        self.task = asyncio.create_task(self.watch())

    async def finalize(self) -> None:
        self.task.cancel()
