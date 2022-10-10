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
import os

import pytest

from nucliadb.ingest.partitions import assign_partitions


@pytest.mark.asyncio
async def test_assign_partitions(partition_settings):
    expected_partition_list = []
    part = partition_settings.replica_number

    while part < partition_settings.nuclia_partitions:
        expected_partition_list.append(str(part + 1))
        part += partition_settings.total_replicas

    assign_partitions(partition_settings)

    assert partition_settings.partitions == expected_partition_list
    assert os.environ["PARTITIONS"] == str(expected_partition_list)
