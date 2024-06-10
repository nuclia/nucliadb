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
import shutil
import uuid
from socket import gethostname

from nucliadb.common.cluster.settings import StandaloneNodeRole
from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.cluster.standalone.index_node import StandaloneIndexNode

logger = logging.getLogger(__name__)


def get_standalone_node_id() -> str:
    if not is_index_node():
        return "_invalid_node_id_"

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
    """
    This returns an instance of the standalone index node
    so when API requests come into this mode, we don't
    make another grpc request since this node can service it directly.
    """
    if not is_index_node():
        raise Exception("This node is not an Index Node. You should not reach this code path.")
    global _SELF_INDEX_NODE
    node_id = get_standalone_node_id()
    if _SELF_INDEX_NODE is None or node_id != _SELF_INDEX_NODE.id:
        if "NUCLIADB_SERVICE_HOST" in os.environ:
            hn = os.environ["HOSTNAME"]
            ns = os.environ.get("NAMESPACE", "nucliadb")
            host = f"{hn}.{ns}"
        else:
            host = gethostname()
        _SELF_INDEX_NODE = StandaloneIndexNode(id=node_id, address=host, shard_count=0, available_disk=0)
    try:
        _, _, available_disk = shutil.disk_usage(cluster_settings.data_path)
        _SELF_INDEX_NODE.available_disk = available_disk
    except FileNotFoundError:  # pragma: no cover
        ...
    try:
        _shards_dir = os.path.join(cluster_settings.data_path, "shards")
        _SELF_INDEX_NODE.shard_count = len(
            [
                shard_dir
                for shard_dir in os.listdir(_shards_dir)
                if os.path.isdir(os.path.join(_shards_dir, shard_dir))
            ]
        )
    except FileNotFoundError:  # pragma: no cover
        ...
    return _SELF_INDEX_NODE


def is_index_node() -> bool:
    return cluster_settings.standalone_node_role in (
        StandaloneNodeRole.ALL,
        StandaloneNodeRole.INDEX,
    )


def is_worker_node() -> bool:
    return cluster_settings.standalone_node_role in (
        StandaloneNodeRole.ALL,
        StandaloneNodeRole.WORKER,
    )
