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
import asyncio
import os
from typing import Awaitable, Callable

import prometheus_client
from watchdog.events import FileCreatedEvent, FileModifiedEvent, FileSystemEventHandler
from watchdog.observers import Observer

from .settings import settings

KB = 1024
MB = 1024 * KB
GB = 1024 * MB


histogram = prometheus_client.Histogram(  # needs to be moved to nucliadb_telemetry
    "nucliadb_node_fs_event_sizes",
    "Distribution of file sizes for filesystem events",
    buckets=(KB * 512, MB, MB * 10, MB * 100, MB * 500, GB, GB * 10),
)


class RecorderHandler(FileSystemEventHandler):
    def on_created(self, event: FileCreatedEvent):
        if os.path.exists(event.src_path):
            size = os.stat(event.src_path).st_size
            histogram.observe(size)

    def on_modified(self, event: FileModifiedEvent):
        if os.path.exists(event.src_path):
            size = os.stat(event.src_path).st_size
            histogram.observe(size)


async def sync_fs_index():
    observer = Observer()
    observer.schedule(RecorderHandler(), settings.data_path, recursive=True)
    observer.start()
    try:
        while True:
            await asyncio.sleep(1)
    except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
        ...
    observer.stop()
    observer.join()


def start() -> Callable[[], Awaitable[None]]:
    task = asyncio.create_task(sync_fs_index())

    async def finalizer():
        task.cancel()
        return

    return finalizer
