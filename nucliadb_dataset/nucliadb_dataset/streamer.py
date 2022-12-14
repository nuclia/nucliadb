import aiohttp
from nucliadb_sdk.client import NucliaDBClient
import requests
from nucliadb_protos.train_pb2 import TrainSet

SIZE_BYTES = 4


class AsyncStreamer:
    def __init__(self, base_url: str, trainset: TrainSet):
        self.base_url = base_url
        self.headers = {"X-NUCLIADB-ROLES": "READER"}
        self.trainset = trainset

    async def get_partitions(self):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(
                f"{self.base_url}/{self.trainset.kbid}/trainset"
            ) as client:
                data = await client.json()
        return data

    async def get_data(self):
        finished = False
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(
                f"{self.base_url}/{self.trainset.kbid}/trainset"
            ) as client:
                while not finished:
                    data = await client.content.read(SIZE_BYTES)
                    if data is None:
                        finished = True
                        break
                    payload_size = int.from_bytes(data, byteorder="big", signed=False)

                    payload = await client.read(payload_size)
                    pb = TrainPayload()
                    pb.ParseFromString(payload)


class Streamer:
    session: requests.Session
    resp: requests.Response
    client: NucliaDBClient

    def __init__(self, trainset: TrainSet, client: NucliaDBClient):
        self.client = client
        self.trainset = trainset
        self.session = requests.Session()

    def get_partitions(self):
        return self.client.reader_session.get(
            f"{self.base_url}/{self.trainset.kbid}/trainset"
        ).json()

    def initialize(self):
        self.resp = self.client.reader_session.stream(
            "GET",
            f"{self.base_url}/{self.trainset.kbid}/trainset",
            headers=self.headers,
            stream=True,
        )

    def finalize(self):
        self.resp.close()

    def get_data(self):
        self.initialize()
        for data in self:
            yield data

        self.finalize()

    def __iter__(self):
        return self.next()

    def next(self):
        data = self.resp.raw.stream(4, decode_content=True)
        if data is None:
            return
        payload_size = int.from_bytes(data, byteorder="big", signed=False)
        payload = self.resp.raw.stream(payload_size, decode_content=True)
        pb = TrainPayload()
        pb.ParseFromString(payload)
        return pb
