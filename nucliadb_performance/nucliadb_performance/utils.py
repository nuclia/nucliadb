import random
from typing import Optional

from faker import Faker

from nucliadb_performance.settings import get_reader_api_url, get_search_api_url
from nucliadb_sdk import NucliaDB

fake = Faker()


_DATA = {}


class Client:
    def __init__(self, session, base_url, headers: Optional[dict[str, str]] = None):
        self.session = session
        self.base_url = base_url
        self.headers = headers or {}

    async def make_request(self, method: str, path: str, *args, **kwargs):
        url = self.base_url + path
        func = getattr(self.session, method.lower())
        base_headers = self.headers.copy()
        kwargs_headers = kwargs.get("headers") or {}
        kwargs_headers.update(base_headers)
        kwargs["headers"] = kwargs_headers
        async with func(url, *args, **kwargs) as resp:
            assert resp.status == 200, resp.status


def get_kbs():
    ndb = NucliaDB(
        url=get_reader_api_url(),
        headers={"X-NUCLIADB-ROLES": "MANAGER"},
    )
    resp = ndb.list_knowledge_boxes()
    return [kb.uuid for kb in resp.kbs]


def get_kb(kbid):
    ndb = NucliaDB(
        url=get_reader_api_url(),
        headers={"X-NUCLIADB-ROLES": "READER"},
    )
    return ndb.get_knowledge_box(kbid=kbid)


def load_kbs():
    kbs = get_kbs()
    _DATA["kbs"] = kbs


def load_kb(kbid: str):
    kb = get_kb(kbid)
    _DATA["kb"] = kb


def get_random_kb() -> str:
    return random.choice(_DATA["kbs"])


def get_loaded_kb():
    return _DATA["kb"].uuid


def get_fake_word():
    word = fake.word()
    while len(word) < 3:
        word = fake.word()
    return word


def get_search_client(session):
    return Client(session, get_search_api_url(), headers={"X-NUCLIADB-ROLES": "READER"})
