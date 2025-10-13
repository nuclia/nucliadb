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
from typing import Union
from unittest.mock import MagicMock, Mock, patch

import pytest

from nucliadb.common import locking
from nucliadb.common.cluster.rebalance import (
    NoMergeCandidatesFound,
    RebalanceShard,
    choose_merge_shards,
    run,
)


async def test_run_handles_locked_rebalance():
    context = MagicMock()
    with patch("nucliadb.common.cluster.rebalance.locking.distributed_lock") as distributed_lock:
        distributed_lock.side_effect = locking.ResourceLocked("rebalance")
        await run(context)

        distributed_lock.side_effect = locking.ResourceLocked("other-key")
        with pytest.raises(locking.ResourceLocked):
            await run(context)


def shard(id: str, active: bool = False, paragraphs: int = 0) -> RebalanceShard:
    return RebalanceShard(id=id, active=active, nidx_id=id, paragraphs=paragraphs)


@pytest.fixture(scope="function", autouse=True)
def max_shard_paragraphs():
    with patch("nucliadb.common.cluster.rebalance.settings", Mock(max_shard_paragraphs=100)):
        yield


@pytest.mark.parametrize(
    "candidates,result",
    [
        ([], NoMergeCandidatesFound("not enough candidates")),
        ([shard(id="1")], NoMergeCandidatesFound("not enough candidates")),
        (
            [shard(id="1", paragraphs=60), shard(id="2", paragraphs=60)],
            NoMergeCandidatesFound("no empty candidates found"),
        ),
        (
            [shard(id="1", paragraphs=10), shard(id="2", paragraphs=90, active=True)],
            NoMergeCandidatesFound("no candidates with room found"),
        ),
        ([shard(id="1", paragraphs=10), shard(id="2", paragraphs=30)], ("1", "2")),
    ],
)
def test_choose_merge_shards(candidates, result: Union[NoMergeCandidatesFound, tuple[str, str]]):
    if isinstance(result, NoMergeCandidatesFound):
        with pytest.raises(NoMergeCandidatesFound) as ex:
            choose_merge_shards(candidates)
        assert str(ex.value) == str(result)
    else:
        chosen = choose_merge_shards(candidates)
        assert chosen[0].id == result[0]
        assert chosen[1].id == result[1]


def test_choose_merge_shards_successive():
    # Make sure it takes the smallest and merge it to the biggest, to minimize the amount of moves.
    candidates = [
        shard(id="s", paragraphs=10),
        shard(id="m", paragraphs=20),
        shard(id="l", paragraphs=50),
    ]
    source, target = choose_merge_shards(candidates)
    assert source.id == "s"
    assert target.id == "l"

    candidates.remove(source)
    candidates[-1].paragraphs += source.paragraphs

    source, target = choose_merge_shards(candidates)

    assert source.id == "m"
    assert target.id == "l"

    candidates.remove(source)
    candidates[-1].paragraphs += source.paragraphs

    with pytest.raises(NoMergeCandidatesFound):
        choose_merge_shards(candidates)
