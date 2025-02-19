from nucliadb.search.predict import PredictEngine
import asyncio

from nucliadb.search.predict_models import FieldInfo, RunAgentsRequest
from nucliadb_protos.resources_pb2 import FieldMetadata

from base64 import b64encode


def proto_to_base64(proto):
    return b64encode(proto.SerializeToString()).decode()


async def main():
    predict = PredictEngine(
        cluster_url="http://127.1.27.1:8080",
        onprem=False,
    )
    await predict.initialize()
    try:
        kbid = "2132eb44-4c12-4ca2-9dfe-719d33d33a1a"
        metadata = FieldMetadata()
        metadata.classifications.add()
        metadata.classifications[0].labelset = "foo"
        metadata.classifications[0].label = "bar"
        item = RunAgentsRequest(
            user_id="user_id",
            fields=[
                FieldInfo(
                    text="The quick brown fox jumps over the lazy dog.",
                    field_id="1",
                    metadata=proto_to_base64(metadata)
                )
            ]
        )
        response = await predict.run_agents(kbid, item)
        print(response)
    finally:
        await predict.finalize()

if __name__ == "__main__":
    asyncio.run(main())



