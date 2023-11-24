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
import pytest

from nucliadb.common.datamanagers.entities import EntitiesDataManager

pytestmark = pytest.mark.asyncio


async def test_set_duplicate_entities_index(maindb_driver):
    duplicates = {
        "ORG": {
            "foo": ["1", "2"],
        }
    }
    async with maindb_driver.transaction() as txn:
        meta_cache = await EntitiesDataManager.get_entities_meta_cache("kbid", txn)
        meta_cache.set_duplicates("ORG", duplicates["ORG"])
        await EntitiesDataManager.set_entities_meta_cache("kbid", meta_cache, txn)
        await txn.commit()

    async with maindb_driver.transaction() as txn:
        assert (
            await EntitiesDataManager.get_entities_meta_cache("kbid", txn)
        ).duplicate_entities == duplicates
