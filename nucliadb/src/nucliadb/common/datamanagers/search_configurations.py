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

from pydantic import TypeAdapter

from nucliadb.common.maindb.driver import Transaction
from nucliadb_models.configuration import SearchConfiguration

logger = logging.getLogger(__name__)

KB_SEARCH_CONFIGURATION_PREFIX = "/kbs/{kbid}/search_configuration"
KB_SEARCH_CONFIGURATION = "/kbs/{kbid}/search_configuration/{name}"


async def get(txn: Transaction, *, kbid: str, name: str) -> Optional[SearchConfiguration]:
    key = KB_SEARCH_CONFIGURATION.format(kbid=kbid, name=name)
    data = await txn.get(key, for_update=True)
    if not data:
        return None
    return TypeAdapter(SearchConfiguration).validate_json(data)


async def list(txn: Transaction, *, kbid: str) -> dict[str, SearchConfiguration]:
    keys = []
    async for key in txn.keys(KB_SEARCH_CONFIGURATION_PREFIX.format(kbid=kbid)):
        keys.append(key)

    configs_data = await txn.batch_get(keys)

    configs = {}
    for key, data in zip(keys, configs_data):
        if data is None:
            continue
        name = key.split("/")[-1]
        config: SearchConfiguration = TypeAdapter(SearchConfiguration).validate_json(data)
        configs[name] = config

    return configs


async def set(txn: Transaction, *, kbid: str, name: str, config: SearchConfiguration) -> None:
    key = KB_SEARCH_CONFIGURATION.format(kbid=kbid, name=name)
    data = config.model_dump_json(exclude_unset=True)
    await txn.set(key, data.encode("utf-8"))


async def delete(txn: Transaction, *, kbid: str, name: str) -> None:
    key = KB_SEARCH_CONFIGURATION.format(kbid=kbid, name=name)
    await txn.delete(key)
