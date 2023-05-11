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

import pytest

from nucliadb.ingest.consumer import auditing
from nucliadb_protos import audit_pb2, writer_pb2
from nucliadb_utils import const
from nucliadb_utils.audit.stream import StreamAuditStorage

pytestmark = pytest.mark.asyncio


async def test_audit_counters(
    maindb_driver,
    stream_audit: StreamAuditStorage,
    pubsub,
    nats_manager,
    fake_node,
    knowledgebox_ingest,
):
    from nucliadb_utils.settings import audit_settings

    partition = stream_audit.get_partition(knowledgebox_ingest)
    subject = audit_settings.audit_jetstream_target.format(  # type: ignore
        partition=partition, type="*"
    )
    await nats_manager.js.add_stream(
        name=audit_settings.audit_stream, subjects=[subject]
    )
    psub = await nats_manager.js.pull_subscribe(subject, "psub")

    iah = auditing.IndexAuditHandler(
        driver=maindb_driver,
        audit=stream_audit,
        pubsub=pubsub,
        check_delay=0.05,
    )
    await iah.initialize()

    await pubsub.publish(
        const.PubSubChannels.RESOURCE_NOTIFY.format(kbid=knowledgebox_ingest),
        writer_pb2.Notification(
            kbid=knowledgebox_ingest,
            action=writer_pb2.Notification.Action.INDEXED,
        ).SerializeToString(),
    )

    await asyncio.sleep(0.1)

    await iah.finalize()

    # should have produced audit message, get the message
    msg = await psub.fetch(1)
    auditreq = audit_pb2.AuditRequest()
    auditreq.ParseFromString(msg[0].data)

    assert auditreq.kbid == knowledgebox_ingest
    assert auditreq.type == audit_pb2.AuditRequest.AuditType.INDEXED
    assert auditreq.kb_counter.fields == 2
    assert auditreq.kb_counter.paragraphs == 2
