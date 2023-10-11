import json
import random
from typing import Optional

from faker import Faker
from pydantic import BaseSettings

from nucliadb_sdk import NucliaDB

fake = Faker()


def get_search_api_url():
    return settings.search_api or settings.main_api


def get_reader_api_url():
    return settings.reader_api or settings.main_api


def get_search_url(path):
    return get_search_api_url() + path


_DATA = {}


class Settings(BaseSettings):
    main_api: str = "http://localhost:8080/api"
    search_api: Optional[str] = None
    reader_api: Optional[str] = None


settings = Settings()


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


def load_kb(kbid):
    kb = get_kb(kbid)
    entities = get_kb_entities(kbid)
    print(f"A total of {len(entities)} found")
    _DATA["kb"] = {
        "kb": kb,
        "entities": entities,
    }


def get_entities(kbs):
    ent = {}
    for kb in kbs:
        entities = get_kb_entities(kb)
        ent[kb] = entities
    return ent


def get_kb_entities(kbid):
    entities = _get_cached_kb_entities(kbid)
    if entities is None:
        entities = _get_kb_entities(kbid)
        _cache_kb_entities(kbid, entities)
    return entities


def _cache_kb_entities(kbid, entities):
    entities_cache = ".entitiescache"
    try:
        with open(entities_cache, "r") as f:
            cached = json.loads(f.read())
    except FileNotFoundError:
        cached = {}
    cached[kbid] = entities
    with open(entities_cache, "w") as f:
        f.write(json.dumps(cached))


def _get_cached_kb_entities(kbid):
    entities_cache = ".entitiescache"
    try:
        with open(entities_cache, "r") as f:
            entities = json.loads(f.read())
    except FileNotFoundError:
        return None
    return entities.get(kbid)


def _get_kb_entities(kbid):
    entities = []
    ndb = NucliaDB(
        url=get_reader_api_url(),
        headers={"X-NUCLIADB-ROLES": "READER"},
    )
    egroups = ndb.get_entitygroups(kbid=kbid, query_params={"show_entities": False})
    for group in egroups.groups.keys():
        gresp = ndb.get_entitygroup(kbid=kbid, group=group)
        entities.extend([f"{group}/{entity}" for entity in gresp.entities.keys()])
    return entities


def get_random_kb() -> str:
    return random.choice(_DATA["kbs"])


def get_loaded_kb():
    return _DATA["kb"]["kb"].uuid


def get_random_kb_entity_filters(n=None):
    entities = _DATA["kb"]["entities"]
    filters = []
    if n is None:
        n = random.randint(1, 10)
    while len(filters) < n:
        choice = random.choice(entities)
        if choice not in filters:
            filters.append(f"/e/{choice}")
    return filters


def get_fake_word():
    word = fake.word()
    while len(word) < 3:
        word = fake.word()
    return word


def get_search_client(session):
    return Client(session, get_search_api_url(), headers={"X-NUCLIADB-ROLES": "READER"})
