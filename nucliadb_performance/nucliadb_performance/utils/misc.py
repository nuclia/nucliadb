import inspect
import os
import random
import shelve
import statistics
from dataclasses import dataclass
from functools import cache
from typing import Optional

from faker import Faker

from nucliadb_performance.settings import (
    get_reader_api_url,
    get_search_api_url,
    settings,
)
from nucliadb_performance.utils.kbs import parse_input_kb_slug
from nucliadb_sdk import NucliaDB

from .metrics import record_request_process_time
from .saved_requests import Request, load_saved_request

CURRENT_DIR = os.getcwd()

fake = Faker()


EXCLUDE_KBIDS = [
    # These are old kbs that are excluded because they
    # are in an inconsistent state and all requests to them fail.
    "058efdc1-59dd-4d22-81a1-4a5c733dad71",
    "1ae07102-3abc-4ded-a861-eb56a089dfea",
    "3c60921c-b6e6-41f5-a1d6-68b406904635",
]
_DATA = {}

MIN_KB_PARAGRAPHS = 5_000


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


def cache_to_disk(func):
    def new_func(*args, **kwargs):
        d = shelve.open("cache.data")
        try:
            cache_key = f"{func.__name__}::{args}::{tuple(sorted(kwargs.items()))}"
            if cache_key not in d:
                d[cache_key] = func(*args, **kwargs)
            return d[cache_key]
        finally:
            d.close()

    async def new_coro(*args, **kwargs):
        d = shelve.open("cache.data")
        try:
            cache_key = f"{func.__name__}::{args}::{tuple(sorted(kwargs.items()))}"
            if cache_key not in d:
                d[cache_key] = await func(*args, **kwargs)
            return d[cache_key]
        finally:
            d.close()

    if inspect.iscoroutinefunction(func):
        return new_coro
    else:
        return new_func


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
                record_request_process_time(resp)
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
    print(f"Loading data from cluster...")

    ndb = NucliaDB(
        url=get_reader_api_url(),
        headers={"X-NUCLIADB-ROLES": "MANAGER"},
    )
    resp = ndb.list_knowledge_boxes()

    kbids = [kb.uuid for kb in resp.kbs if kb.uuid not in EXCLUDE_KBIDS]
    paragraphs = []
    result = []
    for kbid in kbids:
        try:
            pars = get_kb_paragraphs(kbid)
        except CountersError:
            print(f"Error getting counters for {kbid}")
            continue
        if pars > MIN_KB_PARAGRAPHS:
            # Ignore KBs with not a considerable amount of data
            result.append(kbid)
            paragraphs.append(pars)

    print_cluster_stats(result, paragraphs)
    return result


def print_cluster_stats(kbs, paragraphs):
    print(f"Found KBs: {len(kbs)}")
    print(f"Paragraph stats in cluster:")
    print(f" - Total: {sum(paragraphs)}")
    print(
        f" - Avg: {int(statistics.mean(paragraphs))} +/- {int(statistics.stdev(paragraphs))}"
    )
    print(f" - Median: {int(statistics.median(paragraphs))}")
    print(f" - Quantiles (n=10): {statistics.quantiles(paragraphs, n=10)}")
    print(f" - Max: {max(paragraphs)}")


def get_kb(kbid=None, slug=None) -> str:
    if not any([kbid, slug]):
        raise ValueError("Either slug or kbid must be set")

    print(f"Loading kb data from... {get_reader_api_url()}")
    ndb = NucliaDB(
        url=get_reader_api_url(),
        headers={"X-NUCLIADB-ROLES": "READER"},
    )
    if kbid:
        slug = ndb.get_knowledge_box(kbid=kbid).slug
    else:
        kbid = ndb.get_knowledge_box_by_slug(slug=slug).uuid
    paragraphs = get_kb_paragraphs(kbid)
    print(f"Starting search kb test on kb={slug} with {paragraphs} paragraphs")
    return kbid


class CountersError(Exception):
    ...


@cache_to_disk
def get_kb_paragraphs(kbid):
    ndb = NucliaDB(
        url=get_search_api_url(),
        headers={"X-NUCLIADB-ROLES": "READER"},
    )
    resp = ndb.session.get(url=f"/v1/kb/{kbid}/counters")
    if resp.status_code != 200:
        raise CountersError()
    return resp.json()["paragraphs"]


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


def print_errors():
    print("Errors summary:")
    for error in ERRORS:
        print(error)
    print("=" * 50)


def get_request(kbid_or_slug: str, with_tags=None) -> Request:
    if settings.saved_requests_file is None:
        raise AttributeError("SAVED_REQUESTS_FILE env var is not set!")
    saved_requests_file = CURRENT_DIR + "/" + settings.saved_requests_file
    requests = load_saved_request(
        saved_requests_file, kbid_or_slug, with_tags=tuple(with_tags)
    )
    if len(requests) == 0:
        raise ValueError("Could not find any request saved")
    return random.choice(requests)


@cache
def get_kb_to_test():
    slug = parse_input_kb_slug()
    kbid = get_kb(slug=slug)
    return kbid, slug
