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

import logging
import os
import uuid
from socket import gethostname

from nucliadb.common.cluster.settings import settings as cluster_settings

from .index_node import StandaloneIndexNode

logger = logging.getLogger(__name__)


def get_standalone_node_id() -> str:
    if not os.path.exists(cluster_settings.data_path):
        os.makedirs(cluster_settings.data_path, exist_ok=True)
    host_key_path = f"{cluster_settings.data_path}/node.key"
    if not os.path.exists(host_key_path):
        logger.info("Generating new node key")
        with open(host_key_path, "wb") as f:
            f.write(uuid.uuid4().bytes)

    with open(host_key_path, "rb") as f:
        return str(uuid.UUID(bytes=f.read()))


_SELF_INDEX_NODE = None


def get_self() -> StandaloneIndexNode:
    global _SELF_INDEX_NODE
    if _SELF_INDEX_NODE is None:
        if "NUCLIADB_SERVICE_HOST" in os.environ:
            host = os.environ["NUCLIADB_SERVICE_HOST"]
        else:
            host = gethostname()
        _SELF_INDEX_NODE = StandaloneIndexNode(
            id=get_standalone_node_id(),
            address=f"{host}:{cluster_settings.standalone_node_port}",
            shard_count=0,
        )
    # XXX this is weird right? How silly is this?
    _SELF_INDEX_NODE.shard_count = len(
        os.listdir(os.path.join(cluster_settings.data_path, "shards"))
    )
    return _SELF_INDEX_NODE
