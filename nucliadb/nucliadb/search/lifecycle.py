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
from nucliadb.ingest.chitchat import start_chitchat, stop_chitchat
from nucliadb.ingest.orm.nodes_manager import NodesManager
from nucliadb.ingest.utils import get_driver  # type: ignore
from nucliadb.ingest.utils import start_ingest, stop_ingest
from nucliadb.search import SERVICE_NAME
from nucliadb.search.predict import PredictEngine
from nucliadb.search.search import paragraphs
from nucliadb_telemetry.utils import clean_telemetry, setup_telemetry
from nucliadb_utils.settings import nuclia_settings
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
    await setup_telemetry(SERVICE_NAME)

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
    await start_chitchat(SERVICE_NAME)

    await paragraphs.initialize_cache()

    await start_audit_utility(SERVICE_NAME)


async def finalize() -> None:
    await stop_ingest()
    if get_utility(Utility.PARTITION):
        clean_utility(Utility.PARTITION)
    if get_utility(Utility.PREDICT):
        clean_utility(Utility.PREDICT)
    if get_utility(Utility.NODES):
        clean_utility(Utility.NODES)

    await finalize_utilities()
    await stop_audit_utility()
    await stop_chitchat()
    await clean_telemetry(SERVICE_NAME)
