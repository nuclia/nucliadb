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

from nucliadb.common import datamanagers


async def test_set_duplicate_entities_index(maindb_driver):
    duplicates = {
        "ORG": {
            "foo": ["1", "2"],
        }
    }
    async with maindb_driver.transaction(read_only=False) as txn:
        meta_cache = await datamanagers.entities.get_entities_meta_cache(txn, kbid="kbid")
        meta_cache.set_duplicates("ORG", duplicates["ORG"])
        await datamanagers.entities.set_entities_meta_cache(txn, kbid="kbid", cache=meta_cache)
        await txn.commit()

    async with maindb_driver.transaction(read_only=True) as txn:
        assert (
            await datamanagers.entities.get_entities_meta_cache(txn, kbid="kbid")
        ).duplicate_entities == duplicates
