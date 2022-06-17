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
from collections import Counter

from nucliadb_ingest.utils import get_driver  # noqa
from nucliadb_search.chitchat import ChitchatNucliaDBSearch
from nucliadb_search.nodes import NodesManager
from nucliadb_search.predict import PredictEngine
from nucliadb_utils.utilities import Utility, get_utility


def get_predict() -> PredictEngine:
    return get_utility(Utility.PREDICT)  # type: ignore


def get_nodes() -> NodesManager:
    return get_utility(Utility.NODES)  # type: ignore


def get_counter() -> Counter:
    return get_utility(Utility.COUNTER)  # type: ignore


def get_chitchat() -> ChitchatNucliaDBSearch:
    return get_utility(Utility.CHITCHAT)
