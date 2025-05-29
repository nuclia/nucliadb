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


from typing import Optional

from nucliadb.train.utils import get_shard_manager


async def get_kb_partitions(kbid: str, prefix: Optional[str] = None) -> list[str]:
    shard_manager = get_shard_manager()
    shards = await shard_manager.get_shards_by_kbid_inner(kbid=kbid)
    valid_shards = []
    if prefix is None:
        prefix = ""
    for shard in shards.shards:
        if shard.shard.startswith(prefix):
            valid_shards.append(shard.shard)
    return valid_shards
