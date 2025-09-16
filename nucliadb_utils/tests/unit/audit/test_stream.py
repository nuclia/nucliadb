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
import inspect
import time
from functools import partial
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nidx_protos.nodereader_pb2 import SearchRequest

from nucliadb_protos.audit_pb2 import AuditRequest, ChatContext, RetrievedContext
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


def stream_audit_finish_condition(audit_storage: StreamAuditStorage, count_publish: int):
    return (
        audit_storage.queue.qsize() == 0
        and audit_storage.kb_usage_utility.queue.qsize() == 0
        and audit_storage.js.publish.call_count == count_publish
    )


async def wait_until(condition, timeout=1):
    start = time.monotonic()
    while True:
        result_or_coro = condition()
        if inspect.iscoroutine(result_or_coro):
            result = await result_or_coro
        else:
            result = result_or_coro

        if result:
            break

        await asyncio.sleep(0.05)
        if time.monotonic() - start > timeout:
            raise Exception("TESTING ERROR: Condition was never reached")


async def test_lifecycle(audit_storage: StreamAuditStorage, nats):
    nats.jetstream.assert_called_once()

    await audit_storage.finalize()
    nats.close.assert_called_once()


async def test_publish(audit_storage: StreamAuditStorage, nats):
    await audit_storage.initialize()
    audit_storage.send(AuditRequest())

    await wait_until(partial(stream_audit_finish_condition, audit_storage, 1))


async def test_report(audit_storage: StreamAuditStorage, nats):
    audit_storage.report_and_send(kbid="kbid", audit_type=AuditRequest.AuditType.DELETED)

    await wait_until(partial(stream_audit_finish_condition, audit_storage, 1))


async def test_visited(audit_storage: StreamAuditStorage, nats):
    from nucliadb_utils.audit.stream import RequestContext, request_context_var

    context = RequestContext()
    request_context_var.set(context)
    audit_storage.visited("kbid", "uuid", "user", "origin")
    audit_storage.send(context.audit_request)
    await wait_until(partial(stream_audit_finish_condition, audit_storage, 1))


async def test_delete_kb(audit_storage: StreamAuditStorage, nats):
    audit_storage.delete_kb("kbid")

    await wait_until(partial(stream_audit_finish_condition, audit_storage, 1))


async def test_search(audit_storage: StreamAuditStorage, nats):
    from nucliadb_utils.audit.stream import RequestContext, request_context_var

    context = RequestContext()
    request_context_var.set(context)
    audit_storage.search("kbid", "user", 0, "origin", SearchRequest(), -1, 1, "foobar")
    audit_storage.send(context.audit_request)
    await wait_until(partial(stream_audit_finish_condition, audit_storage, 2))


async def test_chat(audit_storage: StreamAuditStorage, nats):
    from nucliadb_utils.audit.stream import RequestContext, request_context_var

    context = RequestContext()
    request_context_var.set(context)

    audit_storage.chat(
        kbid="kbid",
        user="user",
        client_type=0,
        origin="origin",
        generative_answer_time=1,
        generative_answer_first_chunk_time=1,
        generative_reasoning_first_chunk_time=1,
        rephrase_time=1,
        question="foo",
        rephrased_question="rephrased",
        retrieval_rephrased_question="rephrased_user_query_for_better_retrieval",
        chat_context=[ChatContext(author="USER", text="epa")],
        retrieved_context=[RetrievedContext(text="epa", text_block_id="some/id/path")],
        answer="bar",
        reasoning="reasoning",
        learning_id="learning_id",
        status_code=0,
        model="xx",
    )

    audit_storage.send(context.audit_request)
    await wait_until(partial(stream_audit_finish_condition, audit_storage, 1))

    arg = nats.jetstream().publish.call_args[0][1]
    pb = AuditRequest()
    pb.ParseFromString(arg)

    assert pb.retrieval_rephrased_question == "rephrased_user_query_for_better_retrieval"
    assert pb.chat.question == "foo"
    assert pb.chat.rephrased_question == "rephrased"
    assert pb.chat.answer == "bar"
    assert pb.chat.learning_id == "learning_id"
    assert pb.chat.status_code == 0
    assert pb.chat.model == "xx"
    assert pb.chat.chat_context[0].author == "USER"
    assert pb.chat.chat_context[0].text == "epa"
    assert pb.chat.retrieved_context[0].text_block_id == "some/id/path"
    assert pb.chat.retrieved_context[0].text == "epa"
    assert pb.generative_answer_first_chunk_time == 1
    assert pb.generative_reasoning_first_chunk_time == 1
