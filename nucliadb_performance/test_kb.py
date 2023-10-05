from email.mime import base
from molotov import scenario
import nucliadb_sdk
from faker import Faker

fake = Faker()
REGION = "europe-1"
ENV = "stashify.cloud"
LOCAL_NDB = True
URL = "http://localhost:8080/api/v1"


def get_base_url():
    if LOCAL_NDB and URL:
        return URL
    return f"https://{REGION}.{ENV}/api/v1"


def get_url(path):
    return get_base_url() + path


kbid = "b27f6902-c2b8-4399-8310-528eacd9642f"


class Client:
    def __init__(self, session, base_url):
        self.session = session
        self.base_url = base_url

    async def make_request(self, method: str, path: str, *args, **kwargs):
        url = self.base_url + path
        func = getattr(self.session, method.lower())
        async with func(url, *args, **kwargs) as resp:
            assert resp.status == 200, resp.status


@scenario(weight=50)
async def test_find_default(session):
    client = Client(session, get_base_url())
    await client.make_request(
        "GET",
        f"/kb/{kbid}/find",
        params={"query": fake.sentence()},
        headers={"x-nucliadb-roles": "READER"},
    )
