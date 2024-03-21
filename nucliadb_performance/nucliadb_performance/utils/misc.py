# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import inspect
import os
import random
import shelve
import statistics
import traceback
from functools import cache
from json import dumps

from faker import Faker

from nucliadb_performance.settings import (
    get_reader_api_url,
    get_search_api_url,
    settings,
)
from nucliadb_performance.utils.kbs import parse_input_kb_slug
from nucliadb_sdk import NucliaDB

from .errors import append_error
from .exceptions import CountersError, RequestError
from .nucliadb import SearchClient
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

    print("Loading kb data...")
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


async def make_kbid_request(session, kbid, method, path, params=None, json=None):
    client = SearchClient(session)
    return await _make_kbid_request(client, kbid, method, path, params, json)


async def _make_kbid_request(client, kbid, method, path, params=None, json=None):
    try:
        return await client.make_request(method, path, params=params, json=json)
    except RequestError as err:
        # Store error info so we can inspect after the script runs
        detail = (
            err.content and err.content.get("detail", None) if err.content else err.text
        )
        if isinstance(detail, dict):
            detail = dumps(detail)
        endpoint = path.split("/")[-1]
        append_error(kbid, endpoint, err.status, detail)
        raise
    except Exception:
        endpoint = path.split("/")[-1]
        append_error(kbid, endpoint, -1, traceback.format_exc())
        raise


def get_request(kbid_or_slug: str, endpoint: str) -> Request:
    if settings.saved_requests_file is None:
        raise AttributeError("SAVED_REQUESTS_FILE env var is not set!")
    saved_requests_file = CURRENT_DIR + "/" + settings.saved_requests_file
    requests = load_saved_request(
        saved_requests_file,
        kbid_or_slug,
        endpoint=endpoint,
    )
    if len(requests) == 0:
        raise ValueError("Could not find any request saved")
    return random.choice(requests)


@cache
def get_kb_to_test():
    slug = parse_input_kb_slug()
    kbid = get_kb(slug=slug)
    return kbid, slug
