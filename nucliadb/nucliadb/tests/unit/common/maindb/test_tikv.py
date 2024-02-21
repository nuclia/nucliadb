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

from unittest.mock import AsyncMock, MagicMock

import pytest

from nucliadb.common.maindb.exceptions import ConflictError
from nucliadb.common.maindb.tikv import LeaderNotFoundError, TiKVDataLayer


@pytest.mark.parametrize(
    "tikv_exception,handled_exception",
    [
        (
            Exception("gRPC error: RpcFailure: 4-DEADLINE_EXCEEDED Deadline Exceeded"),
            TimeoutError,
        ),
        (Exception("Leader of region 34234 is not found"), LeaderNotFoundError),
    ],
)
async def test_get_retrials(tikv_exception, handled_exception):
    inner_txn = MagicMock(get=AsyncMock(side_effect=tikv_exception))
    tikv_txn = TiKVDataLayer(inner_txn)

    with pytest.raises(handled_exception):
        await tikv_txn.get("key")

    assert inner_txn.get.call_count == 2


async def test_commit_raises_conflict_error():
    inner_txn = MagicMock(commit=AsyncMock(side_effect=Exception("WriteConflict")))
    tikv_txn = TiKVDataLayer(inner_txn)

    with pytest.raises(ConflictError):
        await tikv_txn.commit()
