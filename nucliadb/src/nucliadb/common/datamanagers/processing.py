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
from typing import Optional

from nucliadb.common.maindb.driver import Transaction

logger = logging.getLogger(__name__)


PULL_PARTITION_OFFSET = "/processing/pull-offset/{pull_type_id}/{partition}"


async def get_pull_offset(txn: Transaction, *, pull_type_id: str, partition: str) -> Optional[int]:
    key = PULL_PARTITION_OFFSET.format(pull_type_id=pull_type_id, partition=partition)
    val: Optional[bytes] = await txn.get(key)
    if val is not None:
        return int(val)
    return None


async def set_pull_offset(txn: Transaction, *, pull_type_id: str, partition: str, offset: int) -> None:
    key = PULL_PARTITION_OFFSET.format(pull_type_id=pull_type_id, partition=partition)
    await txn.set(key, str(offset).encode())
