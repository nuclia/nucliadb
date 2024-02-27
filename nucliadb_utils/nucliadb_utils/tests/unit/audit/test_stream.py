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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nucliadb_protos.audit_pb2 import AuditKBCounter, AuditRequest, ChatContext
from nucliadb_protos.nodereader_pb2 import SearchRequest

from nucliadb_utils.audit.stream import StreamAuditStorage


@pytest.fixture()
def nats():
    mock = AsyncMock()
    mock.jetstream = MagicMock(return_value=AsyncMock())
    yield mock


@pytest.fixture()
async def audit_storage(nats):
    with patch("nucliadb_utils.audit.stream.nats.connect", return_value=nats):
        aud = StreamAuditStorage(
            nats_servers=["nats://localhost:4222"],
            nats_target="test",
            partitions=1,
            seed=1,
            nats_creds="nats_creds",
        )
        await aud.initialize()
        yield aud
        await aud.finalize()


async def wait_for_queue(audit_storage: StreamAuditStorage):
    while audit_storage.queue.qsize() > 0:
        await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_lifecycle(audit_storage: StreamAuditStorage, nats):
    nats.jetstream.assert_called_once()

    await audit_storage.finalize()
    nats.close.assert_called_once()


@pytest.mark.asyncio
async def test_publish(audit_storage: StreamAuditStorage, nats):
    await audit_storage.initialize()
    await audit_storage.send(AuditRequest())

    await wait_for_queue(audit_storage)

    nats.jetstream().publish.assert_called_once()


@pytest.mark.asyncio
async def test_report(audit_storage: StreamAuditStorage, nats):
    await audit_storage.report(
        kbid="kbid",
        audit_type=AuditRequest.AuditType.DELETED,
        audit_fields=[],
        kb_counter=AuditKBCounter(),
    )

    await wait_for_queue(audit_storage)
    nats.jetstream().publish.assert_called_once()


@pytest.mark.asyncio
async def test_visited(audit_storage: StreamAuditStorage, nats):
    await audit_storage.visited("kbid", "uuid", "user", "origin")

    await wait_for_queue(audit_storage)
    nats.jetstream().publish.assert_called_once()


@pytest.mark.asyncio
async def test_delete_kb(audit_storage: StreamAuditStorage, nats):
    await audit_storage.delete_kb("kbid")

    await wait_for_queue(audit_storage)
    nats.jetstream().publish.assert_called_once()


@pytest.mark.asyncio
async def test_search(audit_storage: StreamAuditStorage, nats):
    await audit_storage.search("kbid", "user", 0, "origin", SearchRequest(), -1, 1)

    await wait_for_queue(audit_storage)
    nats.jetstream().publish.assert_called_once()


@pytest.mark.asyncio
async def test_suggest(audit_storage: StreamAuditStorage, nats):
    await audit_storage.suggest("kbid", "user", 0, "origin", -1)

    await wait_for_queue(audit_storage)
    nats.jetstream().publish.assert_called_once()


@pytest.mark.asyncio
async def test_chat(audit_storage: StreamAuditStorage, nats):
    await audit_storage.chat(
        kbid="kbid",
        user="user",
        client_type=0,
        origin="origin",
        timeit=-1,
        question="foo",
        rephrased_question="rephrased",
        context=[ChatContext(author="USER", text="epa")],
        answer="bar",
        learning_id="learning_id"
    )

    await wait_for_queue(audit_storage)
    nats.jetstream().publish.assert_called_once()

    arg = nats.jetstream().publish.call_args[0][1]
    pb = AuditRequest()
    pb.ParseFromString(arg)

    assert pb.chat.question == "foo"
    assert pb.chat.rephrased_question == "rephrased"
    assert pb.chat.answer == "bar"
    assert pb.chat.learning_id == "learning_id"
    assert pb.chat.context[0].author == "USER"
    assert pb.chat.context[0].text == "epa"
