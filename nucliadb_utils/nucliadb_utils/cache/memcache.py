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

from nucliadb_utils.cache.settings import settings

try:
    from nucliadb_utils.cache.lru import LRU
except ModuleNotFoundError:
    from lru import LRU  # type: ignore

_lru: Optional[LRU] = None


def get_memory_cache() -> LRU:
    global _lru
    if _lru is None:
        _lru = LRU(settings.cache_memory_size)
    return _lru
