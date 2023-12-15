from molotov import global_setup, global_teardown, scenario

from nucliadb_performance.settings import get_predict_api_url
from nucliadb_performance.utils.errors import print_errors
from nucliadb_performance.utils.misc import (
    get_kb_to_test,
    get_request,
    make_kbid_request,
)
from nucliadb_performance.utils.vectors import predict_sentence_to_vector


@global_setup()
def init_test(args):
    get_kb_to_test()


@scenario(weight=1)
async def test_search(session):
    kbid, slug = get_kb_to_test()
    request = get_request(slug)
    predict_url = get_predict_api_url()
    vector = await predict_sentence_to_vector(
        predict_url, kbid, request.payload["query"]
    )
    request.payload["vector"] = vector
    await make_kbid_request(
        session,
        kbid,
        request.method.upper(),
        request.url.format(kbid=kbid),
        json=request.payload,
    )


@global_teardown()
def end_test():
    print("This is the end of the test.")
    print_errors()
