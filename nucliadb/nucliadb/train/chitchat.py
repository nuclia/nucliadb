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
from __future__ import annotations

from nucliadb.ingest.chitchat import start_chitchat as start_chitchat_ingest
from nucliadb.ingest.orm.node import Node
from nucliadb.ingest.utils import get_chitchat
from nucliadb.train import logger
from nucliadb.train.settings import settings
from nucliadb_utils.utilities import Utility, clean_utility, get_utility


async def start_chitchat(service_name: str):
    existing_chitchat_utility = get_utility(Utility.CHITCHAT)
    if existing_chitchat_utility is None:
        if settings.nodes_load_ingest:
            # used for testing proposes get nodes from a real ingest
            await Node.load_active_nodes()
        else:
            await start_chitchat_ingest(service_name)
    else:
        logger.info(
            "Not registering train chitchat as a chitchat utility already exists"
        )


async def stop_chitchat():
    if get_utility(Utility.CHITCHAT):
        util = get_chitchat()
        await util.close()
        clean_utility(Utility.CHITCHAT)
