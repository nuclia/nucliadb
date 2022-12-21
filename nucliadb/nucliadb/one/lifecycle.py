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
import asyncio

from nucliadb.ingest.app import main as initialize_ingest
from nucliadb.reader.lifecycle import finalize as finalize_reader
from nucliadb.reader.lifecycle import initialize as initialize_reader
from nucliadb.search.lifecycle import finalize as finalize_search
from nucliadb.search.lifecycle import initialize as initialize_search
from nucliadb.train.lifecycle import finalize as finalize_train
from nucliadb.train.lifecycle import initialize as initialize_train
from nucliadb.writer.lifecycle import finalize as finalize_writer
from nucliadb.writer.lifecycle import initialize as initialize_writer
from nucliadb_utils.utilities import finalize_utilities

SYNC_FINALIZERS = []


async def initialize():
    finalizers = await initialize_ingest()
    SYNC_FINALIZERS.extend(finalizers)
    await initialize_writer()
    await initialize_reader()
    await initialize_search()
    await initialize_train()


async def finalize():
    for finalizer in SYNC_FINALIZERS:
        if asyncio.iscoroutinefunction(finalizer):
            await finalizer()
        else:
            finalizer()
    SYNC_FINALIZERS.clear()

    await finalize_writer()
    await finalize_reader()
    await finalize_search()
    await finalize_train()
    await finalize_utilities()
