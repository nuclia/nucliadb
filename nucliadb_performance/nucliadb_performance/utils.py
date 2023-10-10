import random
from typing import Optional

from faker import Faker

from nucliadb_sdk import NucliaDB

fake = Faker()


def get_base_url():
    return "http://search.nucliadb.svc.cluster.local:8080/api/v1"


def get_url(path):
    return get_base_url() + path


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
        url="http://reader.nucliadb.svc.cluster.local:8080/api",
        headers={"X-NUCLIADB-ROLES": "MANAGER"},
    )
    resp = ndb.list_knowledge_boxes()
    return [kb.uuid for kb in resp.kbs]


def load_kbs():
    kbs = get_kbs()
    _DATA["kbs"] = kbs


def get_entities(kbs):
    ent = {}
    for kb in kbs:
        entities = get_kb_entities(kb)
        ent[kb] = entities
    return ent


def get_kb_entities(kbid):
    ndb = NucliaDB(
        url="http://reader.nucliadb.svc.cluster.local:8080/api",
        headers={"X-NUCLIADB-ROLES": "READER"},
    )
    entities = ndb.get_entitygroups(kbid=kbid, query_params={"show_entities": True})
    return [
        f"{group_id}/{entity_id}"
        for group_id, group in entities.groups.items()
        for entity_id in group.entities.keys()
    ]


def get_random_kb() -> str:
    return random.choice(_DATA["kbs"])


def get_fake_word():
    word = fake.word()
    while len(word) < 3:
        word = fake.word()
    return word


def get_search_client(session):
    return Client(session, get_base_url(), headers={"X-NUCLIADB-ROLES": "READER"})
