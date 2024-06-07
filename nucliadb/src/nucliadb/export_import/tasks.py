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
from nucliadb.common.context import ApplicationContext
from nucliadb.export_import.exporter import export_kb_to_blob_storage
from nucliadb.export_import.importer import import_kb_from_blob_storage
from nucliadb.export_import.models import NatsTaskMessage
from nucliadb.tasks import create_consumer, create_producer
from nucliadb.tasks.consumer import NatsTaskConsumer
from nucliadb.tasks.producer import NatsTaskProducer
from nucliadb_utils import const


def get_exports_consumer() -> NatsTaskConsumer:
    return create_consumer(
        name="exports_consumer",
        stream=const.Streams.KB_EXPORTS,  # type: ignore
        callback=export_kb_to_blob_storage,  # type: ignore
        msg_type=NatsTaskMessage,
        max_concurrent_messages=10,
    )


async def get_exports_producer(context: ApplicationContext) -> NatsTaskProducer:
    producer = create_producer(
        name="exports_producer",
        stream=const.Streams.KB_EXPORTS,  # type: ignore
        msg_type=NatsTaskMessage,
    )
    await producer.initialize(context)
    return producer


def get_imports_consumer() -> NatsTaskConsumer:
    return create_consumer(
        name="imports_consumer",
        stream=const.Streams.KB_IMPORTS,  # type: ignore
        callback=import_kb_from_blob_storage,  # type: ignore
        msg_type=NatsTaskMessage,
        max_concurrent_messages=10,
    )


async def get_imports_producer(context: ApplicationContext) -> NatsTaskProducer:
    producer = create_producer(
        name="imports_producer",
        stream=const.Streams.KB_IMPORTS,  # type: ignore
        msg_type=NatsTaskMessage,
    )
    await producer.initialize(context)
    return producer
