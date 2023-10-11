from faker import Faker
from molotov import global_setup, scenario

from nucliadb_performance.utils import (
    get_loaded_kb,
    get_random_kb_entity_filters,
    get_search_client,
    load_kb,
)

fake = Faker()

TINY_KB = "1e11b18a-e829-46ad-91d7-155c4777792b"
SMALL_KB = "c02b6960-4c0e-4ee3-b006-53836da5a5a9"
MEDIUM_KB = "3336b978-6af5-460d-b459-a2fdebcc06df"


KBID_TO_TEST = SMALL_KB


@global_setup()
def init_test(args):
    global KBID_TO_TEST

    if KBID_TO_TEST is None:
        KBID_TO_TEST = input("Enter the kbid to test with: ")

    load_kb(KBID_TO_TEST)


@scenario(weight=1)
async def test_find_entity_filter(session):
    client = get_search_client(session)
    kbid = get_loaded_kb()
    filters = get_random_kb_entity_filters(n=1)
    await client.make_request(
        "GET",
        f"/v1/kb/{kbid}/find",
        params={"query": fake.sentence(), "filters": filters},
    )
