import random
from dataclasses import dataclass
from typing import Optional

from faker import Faker

from nucliadb_performance.settings import get_reader_api_url, get_search_api_url
from nucliadb_sdk import NucliaDB

fake = Faker()

EXCLUDE_KBIDS = [
    # These are old kbs that are excluded because they
    # are in an inconsistent state and all requests to them fail.
    "058efdc1-59dd-4d22-81a1-4a5c733dad71",
    "1ae07102-3abc-4ded-a861-eb56a089dfea",
    "3c60921c-b6e6-41f5-a1d6-68b406904635",
]
_DATA = {}


@dataclass
class Error:
    kbid: str
    endpoint: str
    status_code: int
    error: str


class RequestError(Exception):
    def __init__(self, status, content=None, text=None):
        self.status = status
        self.content = content
        self.text = text


ERRORS: list[Error] = []


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
            if resp.status == 200:
                return
            await self.handle_search_error(resp)

    async def handle_search_error(self, resp):
        content = None
        text = None
        try:
            content = await resp.json()
        except Exception:
            text = await resp.text()
        raise RequestError(resp.status, content=content, text=text)


def get_kbs():
    ndb = NucliaDB(
        url=get_reader_api_url(),
        headers={"X-NUCLIADB-ROLES": "MANAGER"},
    )
    resp = ndb.list_knowledge_boxes()
    return [kb.uuid for kb in resp.kbs if kb.uuid not in EXCLUDE_KBIDS]


def get_kb(kbid):
    ndb = NucliaDB(
        url=get_reader_api_url(),
        headers={"X-NUCLIADB-ROLES": "READER"},
    )
    return ndb.get_knowledge_box(kbid=kbid)


def load_kbs():
    kbs = get_kbs()
    random.shuffle(kbs)
    _DATA["kbs"] = kbs
    return kbs


def pick_kb(worker_id) -> str:
    kbs = _DATA["kbs"]
    index = worker_id % len(kbs)
    return kbs[index]


def get_fake_word():
    word = fake.word()
    while len(word) < 3:
        word = fake.word()
    return word


def get_search_client(session):
    return Client(session, get_search_api_url(), headers={"X-NUCLIADB-ROLES": "READER"})


async def make_kbid_request(session, kbid, method, path, params=None, json=None):
    global ERRORS
    try:
        client = get_search_client(session)
        await client.make_request(method, path, params=params, json=json)
    except RequestError as err:
        # Store error info so we can inspect after the script runs
        detail = (
            err.content and err.content.get("detail", None) if err.content else err.text
        )
        error = Error(
            kbid=kbid,
            endpoint=path.split("/")[-1],
            status_code=err.status,
            error=detail,
        )
        ERRORS.append(error)
        raise
