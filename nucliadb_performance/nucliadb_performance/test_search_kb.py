import os

from faker import Faker
from molotov import global_setup, global_teardown, scenario

from nucliadb_performance.utils import (
    convert_sentence_to_vector,
    get_kb,
    get_kb_request,
    make_kbid_request,
    print_errors,
)

fake = Faker()

TINY_KB = "1e11b18a-e829-46ad-91d7-155c4777792b"
SMALL_KB = "c02b6960-4c0e-4ee3-b006-53836da5a5a9"
MEDIUM_KB = "3336b978-6af5-460d-b459-a2fdebcc06df"
BIG_KB = "TO_BE_ADDED_YET"

KBID_TO_TEST = None


def get_kb_to_test() -> str:
    global KBID_TO_TEST

    if KBID_TO_TEST is not None:
        return KBID_TO_TEST

    kbid = os.environ.get("KBID", "")
    if kbid == "":
        kbid = input(
            "Enter the kbid to test with or choose a default one [tiny, small, medium, big]: "
        )
    try:
        # Check if it´s one of the pre-defined ones
        kbid = {"tiny": TINY_KB, "small": SMALL_KB, "medium": MEDIUM_KB, "big": BIG_KB}[
            kbid.lower()
        ]
    except KeyError:
        pass

    KBID_TO_TEST = kbid
    return kbid


@global_setup()
def init_test(args):
    kbid = get_kb_to_test()
    get_kb(kbid)


@scenario(weight=1)
async def test_find_with_filter(session):
    kbid = get_kb_to_test()

    request = get_kb_request(kbid)
    query = request.query
    vector = await convert_sentence_to_vector(kbid, query)
    data = {"query": query, "filters": request.filters, "vector": vector}
    if request.features is None:
        data["features"] = ["paragraph", "vector"]
    else:
        data["features"] = request.features
    if request.min_score is not None:
        data["min_score"] = request.min_score

    await make_kbid_request(
        session,
        kbid,
        "POST",
        f"/v1/kb/{kbid}/find",
        json=data,
    )


@global_teardown()
def end_test():
    print("This is the end of the test.")
    print_errors()
