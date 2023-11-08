from faker import Faker
from molotov import global_setup, global_teardown, scenario

from nucliadb_performance.settings import get_predict_api_url
from nucliadb_performance.utils.misc import (
    get_kb_request,
    get_kb_to_test,
    make_kbid_request,
    predict_sentence_to_vector,
    print_errors,
)

fake = Faker()


@global_setup()
def init_test(args):
    get_kb_to_test()


@scenario(weight=1)
async def test_find_with_filter(session):
    kbid, slug = get_kb_to_test()
    payload = get_kb_request(slug)
    query = payload["query"]
    vector = await predict_sentence_to_vector(get_predict_api_url(), kbid, query)
    payload["vector"] = vector
    await make_kbid_request(
        session,
        kbid,
        "POST",
        f"/v1/kb/{kbid}/find",
        json=payload,
    )


@global_teardown()
def end_test():
    print("This is the end of the test.")
    print_errors()
