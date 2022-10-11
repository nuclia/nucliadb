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
import sys
from collections import Counter

from nucliadb.ingest.utils import get_driver  # type: ignore
from nucliadb.ingest.utils import get_chitchat, start_ingest, stop_ingest
from nucliadb.search import SERVICE_NAME, logger
from nucliadb.search.chitchat import start_chitchat
from nucliadb.search.nodes import NodesManager
from nucliadb.search.predict import PredictEngine
from nucliadb_telemetry.utils import clean_telemetry, get_telemetry, init_telemetry
from nucliadb_utils.settings import nuclia_settings, running_settings
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    finalize_utilities,
    get_cache,
    get_utility,
    set_utility,
    start_audit_utility,
    stop_audit_utility,
)


async def initialize() -> None:
    tracer_provider = get_telemetry(SERVICE_NAME)
    if tracer_provider:
        await init_telemetry(tracer_provider)

    set_utility(Utility.COUNTER, Counter())
    await start_ingest(SERVICE_NAME)
    predict_util = PredictEngine(
        nuclia_settings.nuclia_inner_predict_url,
        nuclia_settings.nuclia_public_url,
        nuclia_settings.nuclia_service_account,
        nuclia_settings.nuclia_zone,
        nuclia_settings.onprem,
        nuclia_settings.dummy_processing,
    )
    await predict_util.initialize()
    set_utility(Utility.PREDICT, predict_util)
    driver = await get_driver()
    cache = await get_cache()
    set_utility(Utility.NODES, NodesManager(driver=driver, cache=cache))

    existing_chitchat_utility = get_utility(Utility.CHITCHAT)
    if existing_chitchat_utility is None:
        start_chitchat(SERVICE_NAME)
    else:
        logger.info(
            "Not registering search chitchat as already exist a chitchat utility"
        )

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s.%(msecs)02d] [%(levelname)s] - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    logger.setLevel(logging.getLevelName(running_settings.log_level.upper()))
    await start_audit_utility()


async def finalize() -> None:
    await stop_ingest()
    if get_utility(Utility.PARTITION):
        clean_utility(Utility.PARTITION)
    if get_utility(Utility.PREDICT):
        clean_utility(Utility.PREDICT)
    if get_utility(Utility.NODES):
        clean_utility(Utility.NODES)
    if get_utility(Utility.COUNTER):
        clean_utility(Utility.COUNTER)
    if get_utility(Utility.CHITCHAT):
        util = get_chitchat()
        await util.close()
        clean_utility(Utility.CHITCHAT)
    await finalize_utilities()
    await stop_audit_utility()
    await clean_telemetry(SERVICE_NAME)
