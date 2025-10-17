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

"""Migration #40

Replaces deprecated and removed generative models from search configurations

"""

import logging
from typing import cast

from nucliadb.common import datamanagers
from nucliadb.migrator.context import ExecutionContext
from nucliadb_models.configuration import SearchConfiguration
from nucliadb_models.search import AskRequest, FindRequest

logger = logging.getLogger(__name__)

REPLACEMENTS = {
    "claude-3-5-small": "claude-4-5-sonnet",
    "gcp-claude-3-5-sonnet-v2": "gcp-claude-4-5-sonnet",
}


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    affected = await get_affected_search_configurations(kbid)
    if not affected:
        return

    async with datamanagers.with_rw_transaction() as txn:
        for name, config in affected.items():
            logger.info(
                "Migrating search config for kb",
                extra={
                    "kbid": kbid,
                    "search_config": name,
                    "generative_model": config.config.generative_model,  # type: ignore
                },
            )
            config.config.generative_model = REPLACEMENTS[config.config.generative_model]  # type: ignore
            await datamanagers.search_configurations.set(txn, kbid=kbid, name=name, config=config)
        await txn.commit()


async def get_affected_search_configurations(kbid: str) -> dict[str, SearchConfiguration]:
    result: dict[str, SearchConfiguration] = {}
    async with datamanagers.with_ro_transaction() as txn:
        search_configs = await datamanagers.search_configurations.list(txn, kbid=kbid)
        for name, config in search_configs.items():
            if config.kind == "find":
                find_config = cast(FindRequest, config.config)
                if find_config.generative_model in REPLACEMENTS:
                    result[name] = config
            elif config.kind == "ask":
                ask_config = cast(AskRequest, config.config)
                if ask_config.generative_model in REPLACEMENTS:
                    result[name] = config
    return result
