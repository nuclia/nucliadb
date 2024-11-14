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

from nucliadb.common.nidx import get_nidx

# Set of httpx hooks that wait for nidx to be synced before each reader request, but only if we made
# a write first (there is no need to wait if we get a sequence of consecutive read requests)
_nidx_is_dirty = False


async def wait_for_sync(*args):
    global _nidx_is_dirty
    if _nidx_is_dirty:
        nidx = get_nidx()
        if nidx:
            nidx.wait_for_sync()
            _nidx_is_dirty = False


async def mark_dirty(*args):
    global _nidx_is_dirty
    _nidx_is_dirty = True
