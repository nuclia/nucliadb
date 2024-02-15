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

from uuid import uuid4

import pytest

from nucliadb.tests.utils.aiohttp_session import get_mocked_session


@pytest.mark.parametrize("onprem", [True, False])
@pytest.mark.parametrize(
    "mock_payload",
    [
        {"seqid": 1, "account_seq": 1, "queue": "private"},
        {"seqid": 1, "account_seq": 1, "queue": "shared"},
        {"seqid": 1, "account_seq": None, "queue": "private"},
        {"seqid": 1, "account_seq": None, "queue": "shared"},
        {"seqid": 1, "queue": "private"},
        {"seqid": 1, "queue": "shared"},
    ],
)
@pytest.mark.asyncio
async def test_send_to_process(onprem, mock_payload):
    """
    Test that send_to_process does not fail
    """

    from nucliadb.ingest.processing import ProcessingEngine, PushPayload

    fake_nuclia_proxy_url = "http://fake_proxy"
    processing_engine = ProcessingEngine(
        onprem=onprem,
        nuclia_processing_cluster_url=fake_nuclia_proxy_url,
        nuclia_public_url=fake_nuclia_proxy_url,
    )
    await processing_engine.initialize()

    payload = PushPayload(
        uuid=str(uuid4()), kbid=str(uuid4()), userid=str(uuid4()), partition=0
    )

    processing_engine.session = get_mocked_session(
        "POST", 200, json=mock_payload, context_manager=False
    )
    await processing_engine.send_to_process(payload, partition=0)

    await processing_engine.finalize()


@pytest.mark.parametrize("onprem", [True, False])
@pytest.mark.asyncio
async def test_delete_from_processing(onprem):
    """
    Test that send_to_process does not fail
    """

    from nucliadb.ingest.processing import ProcessingEngine

    fake_nuclia_proxy_url = "http://fake_proxy"
    processing_engine = ProcessingEngine(
        onprem=onprem,
        nuclia_processing_cluster_url=fake_nuclia_proxy_url,
        nuclia_public_url=fake_nuclia_proxy_url,
    )
    await processing_engine.initialize()

    processing_engine.session = get_mocked_session(
        "POST",
        200,
        json={"kbid": "kbid", "resource_id": "resource_id"},
        context_manager=False,
    )
    await processing_engine.delete_from_processing(
        kbid="kbid", resource_id="resource_id"
    )

    await processing_engine.finalize()
