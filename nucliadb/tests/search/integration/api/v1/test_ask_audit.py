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

import nats
import pytest
from httpx import AsyncClient
from nats.aio.client import Client
from nats.js import JetStreamContext

from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb_protos.audit_pb2 import AuditRequest
from nucliadb_utils.settings import audit_settings


async def get_audit_messages(sub):
    msg = await sub.fetch(1)
    auditreq = AuditRequest()
    auditreq.ParseFromString(msg[0].data)
    return auditreq


@pytest.mark.deploy_modes("cluster")
async def test_ask_sends_only_one_audit(
    nucliadb_search: AsyncClient, test_search_resource: str, stream_audit
) -> None:
    kbid = test_search_resource

    # Prepare a test audit stream to receive our messages
    partition = stream_audit.get_partition(kbid)
    nats_client: Client = await nats.connect(stream_audit.nats_servers)
    jetstream: JetStreamContext = nats_client.jetstream()
    if audit_settings.audit_jetstream_target is None:
        assert False, "Missing jetstream target in audit settings"
    subject = audit_settings.audit_jetstream_target.format(partition=partition, type="*")

    try:
        await jetstream.delete_stream(name=audit_settings.audit_stream)
        await jetstream.delete_stream(name="test_usage")
    except nats.js.errors.NotFoundError:
        pass

    await jetstream.add_stream(name=audit_settings.audit_stream, subjects=[subject])

    psub = await jetstream.pull_subscribe(subject, "psub")

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/ask",
        json={"query": "title"},
    )
    assert resp.status_code == 200

    # Testing the middleware integration where it collects audit calls and sends a single message
    # at requests ends. In this case we expect one seach and one chat sent once
    stream_audit.search.assert_called_once()
    stream_audit.chat.assert_called_once()
    assert stream_audit.js.publish.call_count == 2
    stream_audit.send.assert_called_once()

    auditreq = await get_audit_messages(psub)
    assert auditreq.type == AuditRequest.AuditType.CHAT
    assert auditreq.kbid == kbid
    assert auditreq.HasField("chat")
    assert auditreq.HasField("search")
    assert auditreq.request_time > 0
    assert auditreq.generative_answer_time > 0
    assert auditreq.retrieval_time > 0
    assert (auditreq.generative_answer_time + auditreq.retrieval_time) < auditreq.request_time
    try:
        auditreq = await get_audit_messages(psub)
    except nats.errors.TimeoutError:
        pass
    else:
        assert "There was an unexpected extra audit message in nats"
    await psub.unsubscribe()
    await nats_client.flush()
    await nats_client.close()
