from aioresponses import aioresponses
from nucliadb_protos.train_pb2_grpc import TrainStub
from nucliadb_protos.train_pb2 import GetInfoRequest, TrainInfo
import pytest


@pytest.mark.asyncio
async def test_get_info(
    train_client: TrainStub, knowledgebox: str, test_pagination_resources
) -> None:
    req = GetInfoRequest()
    req.kb.uuid = knowledgebox

    with aioresponses() as m:
        m.get(
            f"http://search.nuclia.svc.cluster.local:8030/api/v1/kb/{knowledgebox}/counters",
            payload={"resources": 4, "paragraphs": 89, "fields": 4, "sentences": 90},
        )

        labels: TrainInfo = await train_client.GetInfo(req)  # type: ignore
    assert labels.fields == 4
    assert labels.resources == 4
    assert labels.paragraphs == 89
    assert labels.sentences == 90
