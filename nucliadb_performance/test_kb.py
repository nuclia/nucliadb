from email.mime import base
from molotov import scenario
import nucliadb_sdk
from faker import Faker
import random

fake = Faker()
REGION = "europe-1"
ENV = "stashify.cloud"

URL = "http://localhost:8080/api/v1"


def get_base_url():
    return "http://search.nucliadb.svc.cluster.local:8080/api/v1"
    if LOCAL_NDB and URL:
        return URL
    return f"https://{REGION}.{ENV}/api/v1"


def get_url(path):
    return get_base_url() + path


kbid_local = "b27f6902-c2b8-4399-8310-528eacd9642f"
kbid_stashi = "13c0f7ee-cd27-4739-a41b-898653b5885b"

LOCAL_NDB = False

if LOCAL_NDB:
    KBID = kbid_local
else:
    KBID = kbid_stashi

_KBS = None


def get_kbs():
    import requests    
    resp = requests.get("http://reader.nucliadb.svc.cluster.local:8080/api/v1/kbs", headers={"X-nucliadb-roles": "MANAGER"})
    assert resp.status_code == 200
    return [kb["uuid"] for kb in resp.json()["kbs"]]


def get_random_kb() -> str:
    global _KBS

    if _KBS is None:
        _KBS = get_kbs()
    return random.choice(_KBS) 


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
    kbid = get_random_kb()
    await client.make_request(
        "GET",
        f"/kb/{kbid}/find",
        params={"query": fake.sentence(), "filters": ["l/foo/bar/"]},
        headers={"x-nucliadb-roles": "READER"},
    )
