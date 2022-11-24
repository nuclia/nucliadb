from uuid import uuid4

import pytest
from aioresponses import aioresponses


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
        nuclia_cluster_url=fake_nuclia_proxy_url,
        nuclia_public_url=fake_nuclia_proxy_url,
    )
    await processing_engine.initialize()

    payload = PushPayload(
        uuid=str(uuid4()), kbid=str(uuid4()), userid=str(uuid4()), partition=0
    )

    with aioresponses() as m:
        m.post(
            f"{fake_nuclia_proxy_url}/api/internal/processing/push",
            payload=mock_payload,
        )
        m.post(
            f"{fake_nuclia_proxy_url}/api/v1/processing/push?partition=0",
            payload=mock_payload,
        )

        await processing_engine.send_to_process(payload, partition=0)
    await processing_engine.finalize()
