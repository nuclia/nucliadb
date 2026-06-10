# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import inspect
import time
from functools import partial
from unittest.mock import AsyncMock, Mock

from nucliadb_protos.kb_usage_pb2 import (
    ClientType,
    KBSource,
    Predict,
    PredictType,
    Process,
    Search,
    SearchType,
    Service,
    Storage,
)
from nucliadb_utils.nuclia_usage.utils.kb_usage_report import KbUsageReportUtility


def kb_usage_report_finish_condition(kb_usage_report: KbUsageReportUtility, count_publish: int):
    return (
        kb_usage_report.queue.qsize() == 0
        and kb_usage_report.nats_stream.publish.call_count == count_publish  # type: ignore[ty:unresolved-attribute]
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


async def test_kb_usage_report():
    nats_stream = Mock(publish=AsyncMock())
    report_util = KbUsageReportUtility(nats_stream=nats_stream, nats_subject="test-stream")

    await report_util.initialize()

    report_util.send_kb_usage(
        service=Service.NUCLIA_DB,
        account_id="test-account",
        kb_id="test-kbid",
        kb_source=KBSource.HOSTED,
        processes=(
            Process(
                client=ClientType.INTERNAL,
                slow_processing_time=10,
                pre_processing_time=10,
                bytes=10,
                chars=10,
                media_seconds=10,
                pages=10,
                paragraphs=10,
                num_processed=1,
            ),
        ),
        predicts=(
            Predict(
                client=ClientType.API,
                type=PredictType.EXTRACT_TABLES,
                model="chatgpt",
                input=10,
                output=10,
                image=10,
                num_predicts=1,
            ),
        ),
        searches=(
            Search(
                client=ClientType.DASHBOARD,
                type=SearchType.SEARCH,
                tokens=10,
                num_searches=1,
            ),
        ),
        storage=Storage(
            paragraphs=10,
            fields=10,
            resources=10,
        ),
    )

    await wait_until(partial(kb_usage_report_finish_condition, report_util, 1))
    await report_util.finalize()

    nats_stream.publish.assert_called_once()
