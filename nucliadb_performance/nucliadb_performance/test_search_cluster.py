import asyncio
import random
from dataclasses import dataclass

from faker import Faker
from molotov import get_context, global_setup, global_teardown, scenario

from nucliadb_performance.utils import (
    RequestError,
    get_fake_word,
    get_search_client,
    load_kbs,
    pick_kb,
)

fake = Faker()


@dataclass
class Error:
    kbid: str
    endpoint: str
    status_code: int
    error: str


ERRORS: list[Error] = []


# These weights have been obtained from grafana in production by looking at
# some time period where we had some traffic and getting the average
# request rate for each endpoint during that time period.
SUGGEST_WEIGHT = 40.4
CATALOG_WEIGHT = 38.4
CHAT_WEIGHT = 10
FIND_WEIGHT = 5.3
SEARCH_WEIGHT = 5.6


def get_kb(session):
    worker_id = get_context(session).worker_id
    kbid = pick_kb(worker_id)
    return kbid


@global_setup()
def init_test(args):
    kbs = load_kbs()
    print(f"Running cluster test. {len(kbs)} found")


@scenario(weight=SUGGEST_WEIGHT)
async def test_suggest(session):
    client = get_search_client(session)
    kbid = get_kb(session)
    await make_kbid_request(
        client,
        kbid,
        "GET",
        f"/v1/kb/{kbid}/suggest",
        params={"query": get_fake_word()},
    )


@scenario(weight=CATALOG_WEIGHT)
async def test_catalog(session):
    client = get_search_client(session)
    kbid = get_kb(session)
    await make_kbid_request(
        client,
        kbid,
        "GET",
        f"/v1/kb/{kbid}/catalog",
    )


@scenario(weight=CHAT_WEIGHT)
async def test_chat(session):
    client = get_search_client(session)
    kbid = get_kb(session)
    # To avoid calling the LLM in the performance test, we simulate the
    # chat intraction as a find (the retrieval phase) plus some synthetic
    # sleep time (the LLM answer generation streaming time)
    await make_kbid_request(
        client,
        kbid,
        "POST",
        f"/v1/kb/{kbid}/find",
        json={"query": fake.sentence()},
    )
    sleep_time = abs(random.gauss(2, 1))
    await asyncio.sleep(sleep_time)


@scenario(weight=FIND_WEIGHT)
async def test_find(session):
    client = get_search_client(session)
    get_context(session).worker_id
    kbid = get_kb(session)
    await make_kbid_request(
        client,
        kbid,
        "GET",
        f"/v1/kb/{kbid}/find",
        params={"query": fake.sentence()},
    )


@scenario(weight=SEARCH_WEIGHT)
async def test_search(session):
    client = get_search_client(session)
    kbid = get_kb(session)
    await make_kbid_request(
        client,
        kbid,
        "GET",
        f"/v1/kb/{kbid}/search",
        params={"query": fake.sentence()},
    )


@global_teardown()
def end_test():
    print("This is the end of the test.")
    for error in ERRORS:
        print(error)


async def make_kbid_request(client, kbid, method, path, params=None, json=None):
    global ERRORS
    try:
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
