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

import asyncio
from typing import Type, Union

from nucliadb.common.cluster.discovery.base import AbstractClusterDiscovery
from nucliadb.common.cluster.discovery.k8s import KubernetesDiscovery
from nucliadb.common.cluster.discovery.manual import ManualDiscovery
from nucliadb.common.cluster.discovery.single import SingleNodeDiscovery
from nucliadb.common.cluster.settings import ClusterDiscoveryMode, settings
from nucliadb_utils.utilities import clean_utility, get_utility, set_utility

UTIL_NAME = "cluster-discovery"


_setup_lock = asyncio.Lock()


async def setup_cluster_discovery() -> None:
    async with _setup_lock:
        util = get_utility(UTIL_NAME)
        if util is not None:
            # already loaded
            return util

        klass: Union[
            Type[ManualDiscovery], Type[KubernetesDiscovery], Type[SingleNodeDiscovery]
        ]
        if settings.cluster_discovery_mode == ClusterDiscoveryMode.MANUAL:
            klass = ManualDiscovery
        elif settings.cluster_discovery_mode == ClusterDiscoveryMode.KUBERNETES:
            klass = KubernetesDiscovery
        elif settings.cluster_discovery_mode == ClusterDiscoveryMode.SINGLE_NODE:
            klass = SingleNodeDiscovery
        else:
            raise NotImplementedError(
                f"Cluster discovery mode {settings.cluster_discovery_mode} not implemented"
            )

        disc = klass(settings)
        await disc.initialize()
        set_utility(UTIL_NAME, disc)


async def teardown_cluster_discovery() -> None:
    util: AbstractClusterDiscovery = get_utility(UTIL_NAME)
    if util is None:
        # already loaded
        return util

    await util.finalize()
    clean_utility(UTIL_NAME)
