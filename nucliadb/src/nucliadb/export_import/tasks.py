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
from nucliadb.export_import.exporter import export_kb_to_blob_storage
from nucliadb.export_import.importer import import_kb_from_blob_storage
from nucliadb.export_import.models import NatsTaskMessage
from nucliadb.tasks import create_consumer, create_producer
from nucliadb.tasks.consumer import NatsTaskConsumer
from nucliadb.tasks.producer import NatsTaskProducer
from nucliadb.tasks.utils import NatsConsumer, NatsStream


class ExportsNatsConfig:
    stream = NatsStream(
        name="ndb-exports",
        subjects=["ndb-exports"],
    )
    consumer = NatsConsumer(
        subject="ndb-exports",
        group="ndb-exports",
    )


class ImportsNatsConfig:
    stream = NatsStream(
        name="ndb-imports",
        subjects=["ndb-imports"],
    )
    consumer = NatsConsumer(
        subject="ndb-imports",
        group="ndb-imports",
    )


def get_exports_consumer() -> NatsTaskConsumer[NatsTaskMessage]:
    return create_consumer(
        name="exports_consumer",
        stream=ExportsNatsConfig.stream,
        consumer=ExportsNatsConfig.consumer,
        callback=export_kb_to_blob_storage,
        msg_type=NatsTaskMessage,
        max_concurrent_messages=10,
    )


def get_exports_producer() -> NatsTaskProducer[NatsTaskMessage]:
    producer = create_producer(
        name="exports_producer",
        stream=ExportsNatsConfig.stream,
        producer_subject=ExportsNatsConfig.consumer.subject,
        msg_type=NatsTaskMessage,
    )
    return producer


def get_imports_consumer() -> NatsTaskConsumer[NatsTaskMessage]:
    return create_consumer(
        name="imports_consumer",
        stream=ImportsNatsConfig.stream,
        consumer=ImportsNatsConfig.consumer,
        callback=import_kb_from_blob_storage,
        msg_type=NatsTaskMessage,
        max_concurrent_messages=10,
    )


def get_imports_producer() -> NatsTaskProducer[NatsTaskMessage]:
    producer = create_producer(
        name="imports_producer",
        stream=ImportsNatsConfig.stream,
        producer_subject=ImportsNatsConfig.consumer.subject,
        msg_type=NatsTaskMessage,
    )
    return producer
