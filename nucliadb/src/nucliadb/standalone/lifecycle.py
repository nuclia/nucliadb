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
from contextlib import asynccontextmanager

from fastapi import FastAPI

from nucliadb.common.cluster.utils import setup_cluster, teardown_cluster
from nucliadb.common.context.fastapi import inject_app_context
from nucliadb.ingest.app import initialize_grpc as initialize_ingest_grpc
from nucliadb.ingest.app import initialize_pull_workers
from nucliadb.ingest.settings import settings as ingest_settings
from nucliadb.reader.lifecycle import lifespan as reader_lifespan
from nucliadb.search.lifecycle import lifespan as search_lifespan
from nucliadb.train.lifecycle import lifespan as train_lifespan
from nucliadb.writer.lifecycle import lifespan as writer_lifespan
from nucliadb_utils.utilities import finalize_utilities

SYNC_FINALIZERS = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    if ingest_settings.disable_pull_worker:
        finalizers = await initialize_ingest_grpc()
    else:
        finalizers = await initialize_pull_workers()
    SYNC_FINALIZERS.extend(finalizers)

    async with (
        writer_lifespan(app),
        reader_lifespan(app),
        search_lifespan(app),
        train_lifespan(app),
        inject_app_context(app),
    ):
        await setup_cluster()

        yield

        for finalizer in SYNC_FINALIZERS:
            if asyncio.iscoroutinefunction(finalizer):
                await finalizer()
            else:
                finalizer()
        SYNC_FINALIZERS.clear()

    await finalize_utilities()
    await teardown_cluster()
