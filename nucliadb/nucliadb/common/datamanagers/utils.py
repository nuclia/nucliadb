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
from typing import Optional, Type, TypeVar

from google.protobuf.message import Message

from nucliadb.common.maindb.driver import Transaction

PB_TYPE = TypeVar("PB_TYPE", bound=Message)


async def get_kv_pb(
    txn: Transaction, key: str, pb_type: Type[PB_TYPE]
) -> Optional[PB_TYPE]:
    kb_shards_bytes: Optional[bytes] = await txn.get(key)
    if kb_shards_bytes is not None:
        kb_shards = pb_type()
        kb_shards.ParseFromString(kb_shards_bytes)
        return kb_shards
    else:
        return None
